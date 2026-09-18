#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <poll.h>
#include <fcntl.h>
#include <termios.h>

#include <star/payload_apids.h>
#include <payload_link/frame.h>
#include <payload_link/pack.h>

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

// Bulk Data Transfer Wrapper layout constants (application-level, carried as
// the payload of a STAR_APID_* bulk-transfer message inside a payload-link
// frame body - see pack_bulk_wrapper()).
#define BULK_WRAPPER_HEADER_SIZE 14
#define MAX_CHUNK_DATA (NET_IPC_MAX_DATA - BULK_WRAPPER_HEADER_SIZE)

#define BULK_CALC_ENTRY_SIZE 36
#define MAX_CALC_ENTRIES_PER_CHUNK (MAX_CHUNK_DATA / BULK_CALC_ENTRY_SIZE)

#define BULK_FT_ENTRY_SIZE 32
#define MAX_FT_ENTRIES_PER_CHUNK (MAX_CHUNK_DATA / BULK_FT_ENTRY_SIZE)

#define BULK_ACK_HEADER_SIZE 5
#define MAX_MISSING_CHUNKS ((NET_IPC_MAX_DATA - BULK_ACK_HEADER_SIZE) / 4)

// Payload-link body layout (our own convention on top of the opaque body
// that payload_link/frame.h transports):
//
//   | APID (2B BE) | LEN (2B BE) | DATA (LEN bytes) | zero pad, if needed |
//
// LEN is the real byte count of DATA. Zero padding is appended only to meet
// PL_MIN_BODY_LEN (payload_link enforces a 7-byte body floor) and MUST be
// ignored by the receiver, which trusts LEN rather than the frame's own
// decoded body length.
#define LINK_BODY_HDR_SIZE 4
#define LINK_BODY_MAX_DATA (PL_MAX_BODY_LEN - LINK_BODY_HDR_SIZE)

_Static_assert(LINK_BODY_MAX_DATA >= NET_IPC_MAX_DATA,
               "payload-link body can't fit a full-size packet");

// Serial link parameters. Not specified anywhere else in this repo -
// adjust to match the actual RS-422 transceiver/OBC configuration.
#define SERIAL_BAUD B115200

// APIDs to listen for from the OBC (i.e. legal on the bus->payload or
// bidirectional direction, per PayloadApids).
#define NUM_LISTEN_APIDS 6
static const uint16_t listen_apids[NUM_LISTEN_APIDS] = {
    STAR_APID_PAYLOAD_COMMAND,
    STAR_APID_ADCS_TELEMETRY_PASSTHROUGH,
    STAR_APID_ADCS_REQUEST_RESPONSE,
    STAR_APID_EXPERIMENT_TABLE,
    STAR_APID_CONTROL_ALGORITHM,
    STAR_APID_BULK_DATA_RESPONSE,
};

// Connection to daemond. Payload application processes (e.g. payload_app)
// exchange net_ipc_hdr_t-framed messages with us through this, keyed by the
// app's registered name NET_IPC_PAYLOAD_APP_NAME.
static daemon_client_t g_daemon;

// The single RS-422 serial link to the OBC. Every APID shares this one fd;
// payload-link frames carry the APID inside their body (see above), unlike
// the old SPICEnet driver which gave each APID its own pipe/fd.
static int g_serial_fd;

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
    forward_to_app(STAR_APID_PAYLOAD_COMMAND, buf, len);
}

// APID 0x020
void handle_adcs_telemetry(uint8_t *buf, int len) {
    forward_to_app(STAR_APID_ADCS_TELEMETRY_PASSTHROUGH, buf, len);
}

// APID 0x022
void handle_adcs_response(uint8_t *buf, int len) {
    forward_to_app(STAR_APID_ADCS_REQUEST_RESPONSE, buf, len);
}

// APID 0x033
void handle_bulk_experiment_table(uint8_t *buf, int len) {
    forward_to_app(STAR_APID_EXPERIMENT_TABLE, buf, len);
}

// APID 0x034
void handle_bulk_control_algo(uint8_t *buf, int len) {
    forward_to_app(STAR_APID_CONTROL_ALGORITHM, buf, len);
}

// APID 0x035
void handle_bulk_ack(uint8_t *buf, int len) {
    forward_to_app(STAR_APID_BULK_DATA_RESPONSE, buf, len);
}

static void dispatch_from_obc(uint16_t apid, uint8_t *buf, int len) {
    switch (apid) {
        case STAR_APID_PAYLOAD_COMMAND:
            handle_payload_command(buf, len);
            break;
        case STAR_APID_ADCS_TELEMETRY_PASSTHROUGH:
            handle_adcs_telemetry(buf, len);
            break;
        case STAR_APID_ADCS_REQUEST_RESPONSE:
            handle_adcs_response(buf, len);
            break;
        case STAR_APID_EXPERIMENT_TABLE:
            handle_bulk_experiment_table(buf, len);
            break;
        case STAR_APID_CONTROL_ALGORITHM:
            handle_bulk_control_algo(buf, len);
            break;
        case STAR_APID_BULK_DATA_RESPONSE:
            handle_bulk_ack(buf, len);
            break;
        default:
            fprintf(stderr, "[network] frame on unexpected apid 0x%03X\n", apid);
            break;
    }
}

// Writes every byte in `buf` to `fd`, retrying on short writes/EINTR/EAGAIN.
//TODO: MAKE THE WRITES NON BLOCKING
static int write_all(int fd, const uint8_t *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

// Packs `payload` into a payload-link body (APID + LEN + DATA, zero-padded
// to the library's minimum body size if needed), frames it, and writes it
// out over the serial link.
static int link_send(uint16_t apid, const uint8_t *payload, uint16_t payload_len) {
    if (payload_len > LINK_BODY_MAX_DATA) {
        fprintf(stderr, "[network] payload too large for apid 0x%03X (%u bytes)\n", apid, payload_len);
        return -1;
    }

    uint8_t body[LINK_BODY_HDR_SIZE + LINK_BODY_MAX_DATA];
    size_t off = 0;
    pack_be16(body, &off, apid);
    pack_be16(body, &off, payload_len);
    memcpy(body + off, payload, payload_len);
    off += payload_len;
    if (off < PL_MIN_BODY_LEN) {
        memset(body + off, 0, PL_MIN_BODY_LEN - off);
        off = PL_MIN_BODY_LEN;
    }

    uint8_t frame[PL_MAX_FRAME_LEN];
    size_t frame_len = plframe_encode(body, off, frame);
    if (frame_len == 0) {
        fprintf(stderr, "[network] failed to frame apid 0x%03X\n", apid);
        return -1;
    }

    return write_all(g_serial_fd, frame, frame_len);
}

// Handles one frame delivered by the daemon from a payload application
// process: unwraps the net_ipc_hdr_t and sends the data out over the
// serial link on the APID it names.
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

    if (!star_payload_apid_allowed_pay_to_bus(hdr.apid)) {
        fprintf(stderr, "[network] app requested send on unroutable apid 0x%03X\n", hdr.apid);
        return;
    }

    int ret = link_send(hdr.apid, data, hdr.len);
    if (ret < 0) {
        fprintf(stderr, "[network] link_send failed for apid 0x%03X\n", hdr.apid);
    }
}

// Packing helpers
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

    return link_send(STAR_APID_PAYLOAD_TELEMETRY, buf, (uint16_t)off);
}

// APID 0x012
int send_command_response(uint8_t opcode, uint8_t status) {
    uint8_t buf[2];
    size_t off = 0;

    pack_u8(buf, &off, opcode);
    pack_u8(buf, &off, status);

    return link_send(STAR_APID_PAYLOAD_COMMAND_RESPONSE, buf, (uint16_t)off);
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

    return link_send(STAR_APID_ADCS_POINTING_REQUEST, buf, (uint16_t)off);
}

// APID 0x030
int send_bulk_calculation_data(uint32_t transfer_id, struct bulk_calc_entry *entries, uint32_t num_entries) {
    uint32_t total_chunks = (num_entries + MAX_CALC_ENTRIES_PER_CHUNK - 1) / MAX_CALC_ENTRIES_PER_CHUNK;
    uint8_t chunk_data[MAX_CALC_ENTRIES_PER_CHUNK * BULK_CALC_ENTRY_SIZE];
    uint8_t packet[NET_IPC_MAX_DATA];

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start = chunk * MAX_CALC_ENTRIES_PER_CHUNK;
        uint32_t remaining = num_entries - start;
        uint32_t count = remaining < MAX_CALC_ENTRIES_PER_CHUNK ? remaining : MAX_CALC_ENTRIES_PER_CHUNK;

        size_t data_off = 0;
        for (uint32_t i = 0; i < count; i++) {
            data_off += pack_calc_entry(chunk_data + data_off, &entries[start + i]);
        }

        int packet_len = pack_bulk_wrapper(packet, transfer_id, chunk, total_chunks, chunk_data, (uint16_t)data_off);
        int ret = link_send(STAR_APID_PAYLOAD_CALCULATION_DATA, packet, (uint16_t)packet_len);
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
    uint8_t packet[NET_IPC_MAX_DATA];

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start = chunk * MAX_FT_ENTRIES_PER_CHUNK;
        uint32_t remaining = num_entries - start;
        uint32_t count = remaining < MAX_FT_ENTRIES_PER_CHUNK ? remaining : MAX_FT_ENTRIES_PER_CHUNK;

        size_t data_off = 0;
        for (uint32_t i = 0; i < count; i++) {
            data_off += pack_ft_entry(chunk_data + data_off, &entries[start + i]);
        }

        int packet_len = pack_bulk_wrapper(packet, transfer_id, chunk, total_chunks, chunk_data, (uint16_t)data_off);
        int ret = link_send(STAR_APID_FORCE_TORQUE_SENSOR_DATA, packet, (uint16_t)packet_len);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

// APID 0x032
int send_bulk_video_data(uint32_t transfer_id, uint8_t *video_buffer, uint32_t total_bytes) {
    uint32_t total_chunks = (total_bytes + MAX_CHUNK_DATA - 1) / MAX_CHUNK_DATA;
    uint8_t packet[NET_IPC_MAX_DATA];

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start = chunk * MAX_CHUNK_DATA;
        uint32_t remaining = total_bytes - start;
        uint16_t count = (uint16_t)(remaining < MAX_CHUNK_DATA ? remaining : MAX_CHUNK_DATA);

        int packet_len = pack_bulk_wrapper(packet, transfer_id, chunk, total_chunks, video_buffer + start, count);
        int ret = link_send(STAR_APID_IMAGE_VIDEO_DATA, packet, (uint16_t)packet_len);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

// APID 0x035
int send_bulk_ack(uint32_t transfer_id, uint8_t status, uint32_t *missing_chunks, uint32_t num_missing_chunks) {
    uint8_t buf[NET_IPC_MAX_DATA];
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

    return link_send(STAR_APID_BULK_DATA_RESPONSE, buf, (uint16_t)off);
}

static void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
}

// Opens and configures the RS-422 serial link: 8N1, no flow control, raw
// mode. poll()-driven, so the fd is left non-blocking.
static int open_serial_port(const char *portname) {
    int fd = open(portname, O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) {
        return -1;
    }

    struct termios tty;
    if (tcgetattr(fd, &tty) != 0) {
        close(fd);
        return -1;
    }

    cfsetispeed(&tty, SERIAL_BAUD);
    cfsetospeed(&tty, SERIAL_BAUD);

    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;
    tty.c_cflag &= ~CRTSCTS;
    tty.c_cflag |= CREAD | CLOCAL;

    tty.c_lflag &= ~(ICANON | ECHO | ECHOE | ECHONL | ISIG);
    tty.c_iflag &= ~(IXON | IXOFF | IXANY);
    tty.c_iflag &= ~(IGNBRK | BRKINT | PARMRK | ISTRIP | INLCR | IGNCR | ICRNL);
    tty.c_oflag &= ~(OPOST | ONLCR);

    tty.c_cc[VTIME] = 0;
    tty.c_cc[VMIN]  = 0;

    if (tcsetattr(fd, TCSANOW, &tty) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

int main(int argc, char **argv) {
    const char *portname = argc > 1 ? argv[1] : "/dev/ttyS0";
    uint8_t rx_buffer[256]; // raw bytes read per poll() wakeup

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

    g_serial_fd = open_serial_port(portname);
    if (g_serial_fd < 0) {
        perror("[Failed to open serial connection]");
        return EXIT_FAILURE;
    }
    printf("[Opened Serial Connection] %s\n", portname);

    plframe_decode_ctx_t decoder;
    plframe_decode_init(&decoder);

    // Poll on the serial link plus the daemon connection (payload app
    // traffic). Unlike the old SPICEnet driver, there's only one serial fd
    // for every APID - decoded frames are dispatched by the APID embedded
    // in their body (see dispatch_from_obc).
    struct pollfd pollfds[2];
    const int serial_pfd_idx = 0;
    const int daemon_pfd_idx = 1;

    pollfds[serial_pfd_idx].fd     = g_serial_fd;
    pollfds[serial_pfd_idx].events = POLLIN;
    pollfds[daemon_pfd_idx].fd     = daemon_client_fd(&g_daemon);
    pollfds[daemon_pfd_idx].events = POLLIN;

    // Event Loop
    while (1) {
        int ret = poll(pollfds, 2, -1);

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

            if (pollfds[serial_pfd_idx].revents & POLLIN) {
                int bytes_read = (int)read(g_serial_fd, rx_buffer, sizeof(rx_buffer));

                for (int i = 0; i < bytes_read; i++) {
                    plframe_decode_result_t result = plframe_decode_feed(&decoder, rx_buffer[i]);

                    switch (result) {
                        case PL_DECODE_NEED_MORE:
                            break;

                        case PL_DECODE_OK: {
                            size_t off = 0;
                            if (decoder.body_len < LINK_BODY_HDR_SIZE) {
                                fprintf(stderr, "[network] runt payload-link body (%u bytes)\n", decoder.body_len);
                                break;
                            }
                            uint16_t apid = unpack_be16(decoder.body, &off);
                            uint16_t data_len = unpack_be16(decoder.body, &off);
                            if ((size_t)off + data_len > decoder.body_len) {
                                fprintf(stderr, "[network] apid 0x%03X claims %u bytes, only %u in body\n",
                                        apid, data_len, decoder.body_len);
                                break;
                            }
                            dispatch_from_obc(apid, decoder.body + off, data_len);
                            break;
                        }

                        case PL_DECODE_BAD_CRC:
                            fprintf(stderr, "[network] dropped payload-link frame with bad CRC\n");
                            break;

                        case PL_DECODE_RESYNC:
                            fprintf(stderr, "[network] payload-link decoder resynced\n");
                            break;
                    }
                }
            }
        }
    }

    return EXIT_SUCCESS;
}
