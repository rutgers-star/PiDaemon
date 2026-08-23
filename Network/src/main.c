#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <endian.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <poll.h>
#include <fcntl.h>

#include <spicenet/config.h>
#include <spicenet/snp.h>

#include <daemon/client.h>
#include <daemon/proto.h>
#include <net_ipc.h>

// Entries for bulk transfer
struct bulk_calc_entry {
    uint64_t mission_time;
    float    angular_accel[3];
    float    commanded_torque[4];
};

struct bulk_ft_entry {
    uint64_t mission_time;
    float    force_vector[3];
    float    torque_vector[3];
};

// APID Definitions 
// OBC -> Pi
#define APID_CMD_RX            0x010
#define APID_ADCS_TELEM_RX     0x020
#define APID_ADCS_RESP_RX      0x022
#define APID_BULK_EXP_RX       0x033
#define APID_BULK_CTRL_RX      0x034

// Pi -> OBC
#define APID_TELEM_TX          0x011
#define APID_CMD_RESP_TX       0x012
#define APID_ADCS_REQ_TX       0x021
#define APID_BULK_CALC_TX      0x030
#define APID_BULK_FT_TX        0x031
#define APID_BULK_VID_TX       0x032

// Pi <-> OBC
#define APID_BULK_ACK_BIDI     0x035

// Bulk Data Transfer Wrapper layout constants
#define BULK_WRAPPER_HEADER_SIZE 14
#define MAX_CHUNK_DATA (MAX_PACKET_LENGTH - BULK_WRAPPER_HEADER_SIZE)

#define BULK_CALC_ENTRY_SIZE 36 
#define MAX_CALC_ENTRIES_PER_CHUNK (MAX_CHUNK_DATA / BULK_CALC_ENTRY_SIZE)

#define BULK_FT_ENTRY_SIZE 32 
#define MAX_FT_ENTRIES_PER_CHUNK (MAX_CHUNK_DATA / BULK_FT_ENTRY_SIZE)

#define BULK_ACK_HEADER_SIZE 5
#define MAX_MISSING_CHUNKS ((MAX_PACKET_LENGTH - BULK_ACK_HEADER_SIZE) / 4)

// APIDs to poll/listen on
#define NUM_LISTEN_APIDS 6
int listen_apids[NUM_LISTEN_APIDS] = {
    APID_CMD_RX, 
    APID_ADCS_TELEM_RX, 
    APID_ADCS_RESP_RX, 
    APID_BULK_EXP_RX, 
    APID_BULK_CTRL_RX, 
    APID_BULK_ACK_BIDI
};

// Apps to transmit data to OBC
snp_app_t *app_telem_tx;
snp_app_t *app_cmd_resp_tx;
snp_app_t *app_adcs_req_tx;
snp_app_t *app_bulk_calc_tx;
snp_app_t *app_bulk_ft_tx;
snp_app_t *app_bulk_vid_tx;
snp_app_t *app_bulk_ack; // APID 0x035 is bidirectional

// Connection to daemond. Payload application processes (e.g. payload_app)
// exchange net_ipc_hdr_t-framed messages with us through this, keyed by the
// app's registered name NET_IPC_PAYLOAD_APP_NAME.
static daemon_client_t g_daemon;

// Forwards `len` bytes received on `apid` from the OBC to the payload
// application process via the daemon. Fire-and-forget: if no app is
// registered to receive it, the daemon reports a DMSG_ERROR the next time
// we read from it, which the event loop just logs.
static void forward_to_app(uint16_t apid, const uint8_t *buf, int len) {
    uint8_t frame[sizeof(net_ipc_hdr_t) + NET_IPC_MAX_DATA];
    net_ipc_hdr_t hdr = { .apid = apid, .len = (uint16_t)len };
    memcpy(frame, &hdr, sizeof(hdr));
    memcpy(frame + sizeof(hdr), buf, (size_t)len);
    daemon_client_send(&g_daemon, NET_IPC_PAYLOAD_APP_NAME, frame, (uint32_t)(sizeof(hdr) + len));
}

// Input handler functions
// All multi-byte fields arrive in Big-Endian, handle this here also
// APID 0x010
void handle_payload_command(uint8_t *buf, int len) {
    forward_to_app(APID_CMD_RX, buf, len);
}

// APID 0x020
void handle_adcs_telemetry(uint8_t *buf, int len) {
    forward_to_app(APID_ADCS_TELEM_RX, buf, len);
}

// APID 0x022
void handle_adcs_response(uint8_t *buf, int len) {
    forward_to_app(APID_ADCS_RESP_RX, buf, len);
}

// APID 0x033
void handle_bulk_experiment_table(uint8_t *buf, int len) {
    forward_to_app(APID_BULK_EXP_RX, buf, len);
}

// APID 0x034
void handle_bulk_control_algo(uint8_t *buf, int len) {
    forward_to_app(APID_BULK_CTRL_RX, buf, len);
}

// APID 0x035
void handle_bulk_ack(uint8_t *buf, int len) {
    forward_to_app(APID_BULK_ACK_BIDI, buf, len);
}

// Looks up which outbound SPICEnet app to write on for a given APID, for
// routing net_ipc frames received from payload application processes back
// out over the serial link.
static snp_app_t *tx_app_for_apid(uint16_t apid) {
    switch (apid) {
        case APID_TELEM_TX:     return app_telem_tx;
        case APID_CMD_RESP_TX:  return app_cmd_resp_tx;
        case APID_ADCS_REQ_TX:  return app_adcs_req_tx;
        case APID_BULK_CALC_TX: return app_bulk_calc_tx;
        case APID_BULK_FT_TX:   return app_bulk_ft_tx;
        case APID_BULK_VID_TX:  return app_bulk_vid_tx;
        case APID_BULK_ACK_BIDI: return app_bulk_ack;
        default: return NULL;
    }
}

// Handles one frame delivered by the daemon from a payload application
// process: unwraps the net_ipc_hdr_t and writes the data out over
// SPICEnet on the APID it names.
static void handle_app_delivery(const uint8_t *buf, uint32_t len) {
    if (len < sizeof(net_ipc_hdr_t)) {
        fprintf(stderr, "[network] short net_ipc frame from app (%u bytes)\n", len);
        return;
    }
    net_ipc_hdr_t hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    const uint8_t *data = buf + sizeof(hdr);
    if (sizeof(hdr) + hdr.len > len) {
        fprintf(stderr, "[network] truncated net_ipc frame from app (apid=0x%03X len=%u)\n", hdr.apid, hdr.len);
        return;
    }

    snp_app_t *app = tx_app_for_apid(hdr.apid);
    if (app == NULL) {
        fprintf(stderr, "[network] app requested send on unroutable apid 0x%03X\n", hdr.apid);
        return;
    }

    int ret = snp_write(app, (void *)data, hdr.len);
    if (ret < 0) {
        fprintf(stderr, "[network] snp_write failed for apid 0x%03X: %d\n", hdr.apid, ret);
    }
}

// Packing helpers
static inline void pack_u8(uint8_t *buf, size_t *off, uint8_t val) {
    buf[*off] = val;
    *off += 1;
}

static inline void pack_be16(uint8_t *buf, size_t *off, uint16_t val) {
    uint16_t be = htobe16(val);
    memcpy(buf + *off, &be, sizeof(be));
    *off += sizeof(be);
}

static inline void pack_be32(uint8_t *buf, size_t *off, uint32_t val) {
    uint32_t be = htobe32(val);
    memcpy(buf + *off, &be, sizeof(be));
    *off += sizeof(be);
}

static inline void pack_be64(uint8_t *buf, size_t *off, uint64_t val) {
    uint64_t be = htobe64(val);
    memcpy(buf + *off, &be, sizeof(be));
    *off += sizeof(be);
}

static inline void pack_be_float(uint8_t *buf, size_t *off, float val) {
    uint32_t bits;
    memcpy(&bits, &val, sizeof(bits));
    pack_be32(buf, off, bits);
}

static inline void pack_be_float_array(uint8_t *buf, size_t *off, const float *vals, int n) {
    for (int i = 0; i < n; i++) {
        pack_be_float(buf, off, vals[i]);
    }
}

// Packs the common Bulk Data Transfer Wrapper header + data returns total frame length
static int pack_bulk_wrapper(uint8_t *frame, uint32_t transfer_id, uint32_t chunk_seq, uint32_t total_chunks,
                              const uint8_t *data, uint16_t n) {
    size_t off = 0;
    pack_be32(frame, &off, transfer_id);
    pack_be32(frame, &off, chunk_seq);
    pack_be32(frame, &off, total_chunks);
    pack_be16(frame, &off, n);
    memcpy(frame + off, data, n);
    off += n;
    return (int)off;
}

static size_t pack_calc_entry(uint8_t *buf, const struct bulk_calc_entry *e) {
    size_t off = 0;
    pack_be64(buf, &off, e->mission_time);
    pack_be_float_array(buf, &off, e->angular_accel, 3);
    pack_be_float_array(buf, &off, e->commanded_torque, 4);
    return off;
}

static size_t pack_ft_entry(uint8_t *buf, const struct bulk_ft_entry *e) {
    size_t off = 0;
    pack_be64(buf, &off, e->mission_time);
    pack_be_float_array(buf, &off, e->force_vector, 3);
    pack_be_float_array(buf, &off, e->torque_vector, 3);
    return off;
}

// Output handler functions
// These are non blocking so they can snp_write can directly be called here

//TODO: MAKE THE WRITES NON BLOCKING
// APID 0x011
int send_health_telemetry(uint64_t met, uint8_t state, uint8_t exp_id, float temp, uint32_t storage) {
    uint8_t buf[8 + 1 + 1 + 4 + 4];
    size_t off = 0;

    pack_be64(buf, &off, met);
    pack_u8(buf, &off, state);
    pack_u8(buf, &off, exp_id);
    pack_be_float(buf, &off, temp);
    pack_be32(buf, &off, storage);

    int ret = snp_write(app_telem_tx, buf, (int)off);
    return ret < 0 ? ret : 0;
}

// APID 0x012
int send_command_response(uint8_t opcode, uint8_t status) {
    uint8_t buf[2];
    size_t off = 0;

    pack_u8(buf, &off, opcode);
    pack_u8(buf, &off, status);

    int ret = snp_write(app_cmd_resp_tx, buf, (int)off);
    return ret < 0 ? ret : 0;
}

// APID 0x021
int send_adcs_request(uint8_t request_type, float target_quat[4], float target_rates[3], uint32_t duration) {
    uint8_t buf[1 + 16 + 12 + 4];
    size_t off = 0;

    pack_u8(buf, &off, request_type);
    if(request_type != 0xA1 && request_type != 0xA0) {
        pack_be_float_array(buf, &off, target_quat, 4);
        pack_be_float_array(buf, &off, target_rates, 3);
        pack_be32(buf, &off, duration);
    }

    int ret = snp_write(app_adcs_req_tx, buf, (int)off);
    return ret < 0 ? ret : 0;
}

// APID 0x030
int send_bulk_calculation_data(uint32_t transfer_id, struct bulk_calc_entry *entries, uint32_t num_entries) {
    uint32_t total_chunks = (num_entries + MAX_CALC_ENTRIES_PER_CHUNK - 1) / MAX_CALC_ENTRIES_PER_CHUNK;
    uint8_t chunk_data[MAX_CALC_ENTRIES_PER_CHUNK * BULK_CALC_ENTRY_SIZE];
    uint8_t frame[MAX_PACKET_LENGTH];

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start = chunk * MAX_CALC_ENTRIES_PER_CHUNK;
        uint32_t remaining = num_entries - start;
        uint32_t count = remaining < MAX_CALC_ENTRIES_PER_CHUNK ? remaining : MAX_CALC_ENTRIES_PER_CHUNK;

        size_t data_off = 0;
        for (uint32_t i = 0; i < count; i++) {
            data_off += pack_calc_entry(chunk_data + data_off, &entries[start + i]);
        }

        int frame_len = pack_bulk_wrapper(frame, transfer_id, chunk, total_chunks, chunk_data, (uint16_t)data_off);
        int ret = snp_write(app_bulk_calc_tx, frame, frame_len);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

// APID 0x031
int send_bulk_force_torque_data(uint32_t transfer_id, struct bulk_ft_entry *entries, uint32_t num_entries) {
    uint32_t total_chunks = (num_entries + MAX_FT_ENTRIES_PER_CHUNK - 1) / MAX_FT_ENTRIES_PER_CHUNK;
    uint8_t chunk_data[MAX_FT_ENTRIES_PER_CHUNK * BULK_FT_ENTRY_SIZE];
    uint8_t frame[MAX_PACKET_LENGTH];

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start = chunk * MAX_FT_ENTRIES_PER_CHUNK;
        uint32_t remaining = num_entries - start;
        uint32_t count = remaining < MAX_FT_ENTRIES_PER_CHUNK ? remaining : MAX_FT_ENTRIES_PER_CHUNK;

        size_t data_off = 0;
        for (uint32_t i = 0; i < count; i++) {
            data_off += pack_ft_entry(chunk_data + data_off, &entries[start + i]);
        }

        int frame_len = pack_bulk_wrapper(frame, transfer_id, chunk, total_chunks, chunk_data, (uint16_t)data_off);
        int ret = snp_write(app_bulk_ft_tx, frame, frame_len);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

// APID 0x032
int send_bulk_video_data(uint32_t transfer_id, uint8_t *video_buffer, uint32_t total_bytes) {
    uint32_t total_chunks = (total_bytes + MAX_CHUNK_DATA - 1) / MAX_CHUNK_DATA;
    uint8_t frame[MAX_PACKET_LENGTH];

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start = chunk * MAX_CHUNK_DATA;
        uint32_t remaining = total_bytes - start;
        uint16_t count = (uint16_t)(remaining < MAX_CHUNK_DATA ? remaining : MAX_CHUNK_DATA);

        int frame_len = pack_bulk_wrapper(frame, transfer_id, chunk, total_chunks, video_buffer + start, count);
        int ret = snp_write(app_bulk_vid_tx, frame, frame_len);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

// APID 0x035
int send_bulk_ack(uint32_t transfer_id, uint8_t status, uint32_t *missing_chunks, uint32_t num_missing_chunks) {
    uint8_t buf[MAX_PACKET_LENGTH];
    size_t off = 0;

    if (status == 0x00) {
        num_missing_chunks = 0;
    } else if (num_missing_chunks > MAX_MISSING_CHUNKS) {
        num_missing_chunks = MAX_MISSING_CHUNKS;
    }

    pack_be32(buf, &off, transfer_id);
    pack_u8(buf, &off, status);
    for (uint32_t i = 0; i < num_missing_chunks; i++) {
        pack_be32(buf, &off, missing_chunks[i]);
    }

    int ret = snp_write(app_bulk_ack, buf, (int)off);
    return ret < 0 ? ret : 0;
}

static void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
}

int main(int argc, char **argv) {
    int fd;
    const char *portname = argc > 1 ? argv[1] : "/dev/ttyS0";
    uint8_t rx_buffer[512]; // Max payload MTU 512 bytes

    // Connected before touching the serial link so the daemon side of this
    // bridge (payload app traffic) doesn't depend on RS-422 hardware being
    // present. satellite_state_t is canonical to the daemon (daemon/state.h)
    // - this process no longer keeps its own copy of the state file.
    int daemon_connected = 0;
    for (int attempt = 0; attempt < 10 && !daemon_connected; attempt++) {
        if (daemon_client_connect(NET_IPC_NETWORK_NAME, &g_daemon) == 0) {
            daemon_connected = 1;
            break;
        }
        sleep_ms(300);
    }
    if (!daemon_connected) {
        fprintf(stderr, "[Failed to connect to daemon]: %s\n", strerror(errno));
        return EXIT_FAILURE;
    }
    printf("[Connected to daemon as \"%s\"]\n", NET_IPC_NETWORK_NAME);

    // Read up front, before any peer traffic can arrive - see the ordering
    // caveat on daemon_client_get_state in daemon/client.h.
    satellite_state_t boot_state;
    if (daemon_client_get_state(&g_daemon, &boot_state) == 0) {
        printf("[Payload State Restored] op_state=0x%02X active_experiment_id=%u dce_authority=%u\n",
               boot_state.op_state, boot_state.active_experiment_id, boot_state.dce_authority_held);
    }

    if (snp_open(&fd, (char *)portname) != 0) {
        perror("[Failed to open serial connection]");
        return EXIT_FAILURE;
    }
    printf("[Opened Serial Connection] %s\n", portname);

    if (snp_listen(fd) != 0) {
        printf("[Serial Connection Invalid]\n");
        return EXIT_FAILURE;
    }
    printf("[Serial Connection Confirmed]\n");

    // Connect to all listening APIDs, plus one extra pollfd for the daemon
    // connection (payload app traffic).
    snp_app_t *apps[NUM_LISTEN_APIDS];
    struct pollfd pollfds[NUM_LISTEN_APIDS + 1];
    const int daemon_pfd_idx = NUM_LISTEN_APIDS;

    for (int i = 0; i < NUM_LISTEN_APIDS; i++) {
        if (snp_connect(listen_apids[i], &apps[i]) != 0) {
            fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", listen_apids[i]);
            return EXIT_FAILURE;
        }

        printf("[Connected to apid] 0x%03X\n", listen_apids[i]);
        pollfds[i].fd = apps[i]->read[0];
        pollfds[i].events = POLLIN;

        if (listen_apids[i] == APID_BULK_ACK_BIDI) {
            app_bulk_ack = apps[i];
        }
    }

    // Connect to all transmitting APIDs
    if (snp_connect(APID_TELEM_TX, &app_telem_tx) != 0) {
        fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", APID_TELEM_TX);
        return EXIT_FAILURE;
    }

    if (snp_connect(APID_CMD_RESP_TX, &app_cmd_resp_tx) != 0) {
        fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", APID_CMD_RESP_TX);
        return EXIT_FAILURE;
    }

    if (snp_connect(APID_ADCS_REQ_TX, &app_adcs_req_tx) != 0) {
        fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", APID_ADCS_REQ_TX);
        return EXIT_FAILURE;
    }

    if (snp_connect(APID_BULK_CALC_TX, &app_bulk_calc_tx) != 0) {
        fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", APID_BULK_CALC_TX);
        return EXIT_FAILURE;
    }

    if (snp_connect(APID_BULK_FT_TX, &app_bulk_ft_tx) != 0) {
        fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", APID_BULK_FT_TX);
        return EXIT_FAILURE;
    }

    if (snp_connect(APID_BULK_VID_TX, &app_bulk_vid_tx) != 0) {
        fprintf(stderr, "[Failed to connect to apid 0x%03X]\n", APID_BULK_VID_TX);
        return EXIT_FAILURE;
    }

    pollfds[daemon_pfd_idx].fd     = daemon_client_fd(&g_daemon);
    pollfds[daemon_pfd_idx].events = POLLIN;

    // Event Loop
    while (1) {
        int ret = poll(pollfds, NUM_LISTEN_APIDS + 1, -1);

        if (ret > 0) {
            if (pollfds[daemon_pfd_idx].revents & POLLIN) {
                uint16_t type;
                char     src[DAEMON_NAME_MAX];
                uint8_t  daemon_buf[DAEMON_MAX_MSG_PAYLOAD];
                uint32_t len;
                if (daemon_client_recv(&g_daemon, &type, src, daemon_buf, sizeof(daemon_buf), &len) != 0) {
                    fprintf(stderr, "[network] connection to daemon lost\n");
                } else if (type == DMSG_DELIVER) {
                    handle_app_delivery(daemon_buf, len);
                } else if (type == DMSG_ERROR) {
                    fprintf(stderr, "[network] daemon reported an error (app not up yet?)\n");
                }
            }

            for (int i = 0; i < NUM_LISTEN_APIDS; i++) {
                if (pollfds[i].revents & POLLIN) {
                    
                    int bytes_read = snp_read(apps[i], rx_buffer, sizeof(rx_buffer));
                    
                    if (bytes_read > 0) {
                        // Route to appropriate handler based on APID
                        switch(apps[i]->apid) {
                            case APID_CMD_RX:
                                handle_payload_command(rx_buffer, bytes_read);
                                break;
                            case APID_ADCS_TELEM_RX:
                                handle_adcs_telemetry(rx_buffer, bytes_read);
                                break;
                            case APID_ADCS_RESP_RX:
                                handle_adcs_response(rx_buffer, bytes_read);
                                break;
                            case APID_BULK_EXP_RX:
                                handle_bulk_experiment_table(rx_buffer, bytes_read);
                                break;
                            case APID_BULK_CTRL_RX:
                                handle_bulk_control_algo(rx_buffer, bytes_read);
                                break;
                            case APID_BULK_ACK_BIDI:
                                handle_bulk_ack(rx_buffer, bytes_read);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        }
    }

    return EXIT_SUCCESS;
}