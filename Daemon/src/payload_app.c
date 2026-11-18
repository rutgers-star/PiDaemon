// Dummy payload application process. Stands in for the real Pi-side
// experiment/ADCS-control software described in the ICD: it keeps a small
// in-memory view of "the satellite" (mission time, payload health, current
// attitude, DCE authority) and periodically issues the same kinds of
// traffic that software would - health telemetry (APID 0x011), ADCS
// pointing requests (APID 0x021), and bulk data transfers (APIDs
// 0x030-0x032) - plus reacts to commands, ADCS telemetry, bulk transfers,
// and bulk ACK/NACKs arriving from the OBC (APIDs 0x010, 0x020, 0x022,
// 0x033-0x035).
//
// It never touches the serial link itself, and it never packs ICD Section 5
// wire bytes either: outbound traffic is handed to "network"
// (Network/src/main.c) as semantic values over the NET_IPC_CTRL_SEND_*
// control messages in net_ipc.h (a quaternion, a status code, a batch of
// sensor entries), and "network" owns turning that into the actual wire
// format - including fields like Request Counter / Command Counter that
// are purely protocol bookkeeping, not application state. This process
// still parses *inbound* bytes itself (APIDs 0x010/0x020/0x022/0x033-0x035)
// since interpreting what the OBC sent is application logic, not framing.
//
// usage: payload_app [registration_name]

#include "daemon/client.h"
#include "daemon/ipc.h"
#include "daemon/proto.h"
#include "net_ipc.h"

#include <endian.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// Mirrors the OBC -> Pi opcodes in ICD Section 5.2.
#define CMD_GRANT_DCE_AUTHORITY  0x01
#define CMD_REVOKE_DCE_AUTHORITY 0x02
#define CMD_TIME_SYNC_PING       0x03
#define CMD_RUN_EXPERIMENT       0x20
#define CMD_ABORT_EXPERIMENT     0x21
#define CMD_EMERGENCY_HALT       0xFF

// ADCS pointing request types, ICD Section 5.6.
#define ADCS_REQ_TARGET_ORIENTATION 0x01
#define ADCS_REQ_TARGET_SPIN_RATE   0x02
#define ADCS_REQ_REQUEST_DCE        0xA0
#define ADCS_REQ_RELINQUISH_DCE     0xA1

// APIDs this process receives on (ICD Section 3.2). Outbound traffic goes
// through the NET_IPC_CTRL_SEND_* control messages instead of a real APID -
// "network" is the one that stamps the APID once it's built the frame.
#define APID_CMD_RX        0x010
#define APID_ADCS_TELEM_RX 0x020
#define APID_ADCS_RESP_RX  0x022
#define APID_BULK_EXP_RX   0x033
#define APID_BULK_CTRL_RX  0x034
#define APID_BULK_ACK_BIDI  0x035

// Bulk Data Transfer Wrapper layout, ICD Section 5.8 - only needed here for
// parsing inbound chunks (0x033/0x034); outbound chunking (0x030/0x031)
// is "network"'s job now.
#define BULK_WRAPPER_HEADER_SIZE 14
#define MAX_CHUNK_DATA (NET_IPC_MAX_DATA - BULK_WRAPPER_HEADER_SIZE)

// ICD Section 5.9: Transfer ID(4) + Status(1) + Chunks Received(4), before
// the optional Missing Chunks list.
#define BULK_ACK_HEADER_SIZE 9

// Dummy reassembly limits for inbound bulk transfers (Section 5.8.4/5.8.5).
// Real payload software would size this to whatever experiment
// tables/control algorithms are expected to be; this is enough for a
// demonstration file well under a megabyte.
#define BULK_RX_MAX_CHUNKS 64

// This process's view of the satellite, populated from boot defaults, the
// daemon's persisted satellite_state_t, and whatever arrives from the OBC.
typedef struct {
    uint64_t met_ms;
    uint8_t  op_state;             // payload_op_state_t values (ICD 5.3)
    uint8_t  active_experiment_id;
    float    cpu_temp_c;
    uint32_t storage_mb_remaining;
    float    current_quat[4];      // last ADCS telemetry passthrough seen
    float    angular_rates[3];
    uint8_t  dce_authority_held;
} sim_state_t;

static void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
}

// ---- unpacking helpers for inbound bytes ("network" forwards these raw -
// interpreting them is this process's job, not "network"'s) ----

static inline uint8_t unpack_u8(const uint8_t *buf, size_t *off) {
    uint8_t val = buf[*off];
    *off += 1;
    return val;
}

static inline uint16_t unpack_be16(const uint8_t *buf, size_t *off) {
    uint16_t be;
    memcpy(&be, buf + *off, sizeof(be));
    *off += sizeof(be);
    return be16toh(be);
}

static inline uint32_t unpack_be32(const uint8_t *buf, size_t *off) {
    uint32_t be;
    memcpy(&be, buf + *off, sizeof(be));
    *off += sizeof(be);
    return be32toh(be);
}

static inline float unpack_be_float(const uint8_t *buf, size_t *off) {
    uint32_t bits = unpack_be32(buf, off);
    float val;
    memcpy(&val, &bits, sizeof(val));
    return val;
}

static inline void unpack_be_float_array(const uint8_t *buf, size_t *off, float *vals, int n) {
    for (int i = 0; i < n; i++) {
        vals[i] = unpack_be_float(buf, off);
    }
}

// Wraps `data` in net_ipc_hdr_t and hands it to the daemon to relay to the
// "network" process - used for the NET_IPC_CTRL_* control messages (network
// builds the real ICD frame from these semantic values) and for
// NET_IPC_CTRL_BULK_UPLOAD. Fire-and-forget: a bad/absent peer surfaces
// later as a DMSG_ERROR on our next recv, which the main loop just logs.
static int send_to_network(daemon_client_t *c, uint16_t ctrl_apid, const void *data, uint16_t len) {
    uint8_t frame[sizeof(net_ipc_hdr_t) + NET_IPC_CTRL_MAX_DATA];
    net_ipc_hdr_t hdr = { .apid = ctrl_apid, .len = len };
    memcpy(frame, &hdr, sizeof(hdr));
    memcpy(frame + sizeof(hdr), data, len);
    return daemon_client_send(c, NET_IPC_NETWORK_NAME, frame, (uint32_t)(sizeof(hdr) + len));
}

// ---- bulk data transfer, ICD Section 5.8 ----
//
// Entry-based transfers (calc/force-torque) hand "network" the semantic
// entries and let it pack/chunk/send them (and assign the Transfer ID).
// Video stays file-based like Daemon/src/video_uploader.c's real uploads -
// this dummy just writes its synthetic frame to a scratch file first so it
// can go through the same NET_IPC_CTRL_BULK_UPLOAD path instead of needing
// its own chunking logic.

static uint32_t g_next_video_transfer_id = 1;

// APID 0x030: a handful of dummy attitude-control-loop samples.
static void send_bulk_calc_dummy(daemon_client_t *c, const sim_state_t *s) {
    net_ipc_calc_data_t d = { .num_entries = 5 };
    for (uint32_t i = 0; i < d.num_entries; i++) {
        net_ipc_calc_entry_t *e = &d.entries[i];
        e->mission_time        = s->met_ms + (uint64_t)i * 100;
        e->angular_accel[0]    = 0.001f * (float)i;
        e->angular_accel[1]    = -0.002f * (float)i;
        e->angular_accel[2]    = 0.0f;
        e->commanded_torque[0] = 0.01f;
        e->commanded_torque[1] = 0.02f;
        e->commanded_torque[2] = 0.0f;
        e->commanded_torque[3] = -0.01f;
    }
    fprintf(stderr, "[payload_app] -> bulk calculation data (%u entries)\n", d.num_entries);
    send_to_network(c, NET_IPC_CTRL_SEND_CALC_DATA, &d, sizeof(d));
}

// APID 0x031: a handful of dummy force-torque sensor samples.
static void send_bulk_ft_dummy(daemon_client_t *c, const sim_state_t *s) {
    net_ipc_ft_data_t d = { .num_entries = 5 };
    for (uint32_t i = 0; i < d.num_entries; i++) {
        net_ipc_ft_entry_t *e = &d.entries[i];
        e->mission_time     = s->met_ms + (uint64_t)i * 100;
        e->force_vector[0]  = 0.1f * (float)i;
        e->force_vector[1]  = -0.05f;
        e->force_vector[2]  = 0.0f;
        e->torque_vector[0] = 0.0f;
        e->torque_vector[1] = 0.02f;
        e->torque_vector[2] = -0.01f * (float)i;
    }
    fprintf(stderr, "[payload_app] -> bulk force-torque data (%u entries)\n", d.num_entries);
    send_to_network(c, NET_IPC_CTRL_SEND_FT_DATA, &d, sizeof(d));
}

// APID 0x032: a small dummy "video frame" (no entry structure - raw bytes
// per ICD 5.8.3), deliberately larger than one chunk to exercise multi-chunk
// reassembly on the OBC side.
#define DUMMY_VIDEO_SCRATCH_PATH "/tmp/payload_app_dummy_video.bin"

static void send_bulk_video_dummy(daemon_client_t *c) {
    uint8_t frame_data[1200];
    for (size_t i = 0; i < sizeof(frame_data); i++) {
        frame_data[i] = (uint8_t)(i & 0xFF);
    }

    FILE *f = fopen(DUMMY_VIDEO_SCRATCH_PATH, "wb");
    if (f == NULL || fwrite(frame_data, 1, sizeof(frame_data), f) != sizeof(frame_data)) {
        fprintf(stderr, "[payload_app] failed to write dummy video scratch file %s: %s\n",
                DUMMY_VIDEO_SCRATCH_PATH, strerror(errno));
        if (f != NULL) {
            fclose(f);
        }
        return;
    }
    fclose(f);

    uint32_t              tid = g_next_video_transfer_id++;
    net_ipc_bulk_upload_t req = { .transfer_id = tid };
    strncpy(req.file_path, DUMMY_VIDEO_SCRATCH_PATH, sizeof(req.file_path) - 1);
    fprintf(stderr, "[payload_app] -> bulk video data transfer id=%u (%zu bytes)\n", tid, sizeof(frame_data));
    send_to_network(c, NET_IPC_CTRL_BULK_UPLOAD, &req, sizeof(req));
}

// Cycles through the three Pi -> OBC bulk data sources.
static void send_dummy_bulk(daemon_client_t *c, const sim_state_t *s, int step) {
    switch (step % 3) {
        case 0: send_bulk_calc_dummy(c, s); break;
        case 1: send_bulk_ft_dummy(c, s); break;
        default: send_bulk_video_dummy(c); break;
    }
}

// Reassembly state for one in-flight inbound bulk transfer (ICD 5.8.4 /
// 5.8.5: Experiment Table and Control Algorithm are both OBC -> Pi raw byte
// streams, chunked via the same wrapper). Bounded to BULK_RX_MAX_CHUNKS for
// this dummy - see the comment at its definition.
typedef struct {
    const char *label;
    int         active;
    uint32_t    transfer_id;
    uint32_t    total_chunks;
    uint32_t    chunks_received;
    uint8_t     seen[BULK_RX_MAX_CHUNKS];
    uint16_t    chunk_len[BULK_RX_MAX_CHUNKS];
    uint8_t     data[BULK_RX_MAX_CHUNKS * MAX_CHUNK_DATA];
} bulk_rx_t;

static bulk_rx_t g_exp_table_rx = { .label = "experiment table" };
static bulk_rx_t g_ctrl_algo_rx = { .label = "control algorithm" };

// Shared scratch buffer for reassembling a completed transfer in order -
// static rather than a large stack array, reused one completion at a time.
static uint8_t g_bulk_assemble_buf[BULK_RX_MAX_CHUNKS * MAX_CHUNK_DATA];

// Feeds one received chunk into `rx`. On the last chunk of a session,
// reassembles the full byte stream, logs it, and ACKs it back to the OBC
// via APID 0x035 (ICD 5.9). Chunks/transfers outside BULK_RX_MAX_CHUNKS are
// dropped with a log line rather than overflowing the dummy reassembly
// buffer - a real implementation would size this to the real max transfer.
static void handle_bulk_chunk(bulk_rx_t *rx, daemon_client_t *c, const uint8_t *data, uint16_t len) {
    if (len < BULK_WRAPPER_HEADER_SIZE) return;

    size_t   off          = 0;
    uint32_t transfer_id  = unpack_be32(data, &off);
    uint32_t chunk_seq    = unpack_be32(data, &off);
    uint32_t total_chunks = unpack_be32(data, &off);
    uint16_t n            = unpack_be16(data, &off);
    const uint8_t *chunk_data = data + off;

    if (!rx->active || rx->transfer_id != transfer_id) {
        memset(rx->seen, 0, sizeof(rx->seen));
        rx->active          = 1;
        rx->transfer_id     = transfer_id;
        rx->total_chunks    = total_chunks;
        rx->chunks_received = 0;
    }

    if (chunk_seq >= BULK_RX_MAX_CHUNKS || total_chunks > BULK_RX_MAX_CHUNKS || n > MAX_CHUNK_DATA) {
        fprintf(stderr, "[payload_app] %s transfer %u: chunk %u exceeds dummy reassembly limits, dropping session\n",
                rx->label, transfer_id, chunk_seq);
        rx->active = 0;
        return;
    }

    if (!rx->seen[chunk_seq]) {
        memcpy(rx->data + (size_t)chunk_seq * MAX_CHUNK_DATA, chunk_data, n);
        rx->chunk_len[chunk_seq] = n;
        rx->seen[chunk_seq]      = 1;
        rx->chunks_received++;
    }

    if (rx->chunks_received < rx->total_chunks) {
        return; // still waiting on more chunks
    }

    size_t total_len = 0;
    for (uint32_t i = 0; i < rx->total_chunks; i++) {
        memcpy(g_bulk_assemble_buf + total_len, rx->data + (size_t)i * MAX_CHUNK_DATA, rx->chunk_len[i]);
        total_len += rx->chunk_len[i];
    }
    fprintf(stderr, "[payload_app] %s transfer %u complete: %zu bytes in %u chunks\n", rx->label, transfer_id,
            total_len, rx->total_chunks);
    if (rx == &g_exp_table_rx) {
        int preview_len = (int)(total_len < 120 ? total_len : 120);
        fprintf(stderr, "[payload_app] experiment table preview: %.*s\n", preview_len,
                (const char *)g_bulk_assemble_buf);
    }
    rx->active = 0;

    // ACK - a real implementation would NACK with the gaps in rx->seen[] on
    // a session timeout instead of only ever acking on completion.
    net_ipc_bulk_ack_t ack = {
        .transfer_id     = transfer_id,
        .status          = 0x00,
        .chunks_received = rx->chunks_received,
        .num_missing     = 0,
    };
    send_to_network(c, NET_IPC_CTRL_SEND_BULK_ACK, &ack, sizeof(ack));
}

// APID 0x035, when it's the OBC responding to one of *our* 0x030/0x031/0x032
// sends (as opposed to us responding to the OBC's 0x033/0x034 sends above).
static void handle_bulk_ack_response(const uint8_t *data, uint16_t len) {
    if (len < BULK_ACK_HEADER_SIZE) return;
    size_t   off             = 0;
    uint32_t transfer_id     = unpack_be32(data, &off);
    uint8_t  status           = unpack_u8(data, &off);
    uint32_t chunks_received = unpack_be32(data, &off);

    if (status == 0x00) {
        fprintf(stderr, "[payload_app] bulk transfer %u ACKed by OBC (%u chunks received)\n", transfer_id,
                chunks_received);
    } else {
        uint32_t num_missing = (len - BULK_ACK_HEADER_SIZE) / 4;
        fprintf(stderr, "[payload_app] bulk transfer %u NACKed by OBC, %u chunks received so far, %u missing chunk(s)\n",
                transfer_id, chunks_received, num_missing);
    }
}

// Mirrors sim_state_t into the daemon's canonical satellite_state_t and
// pushes it. Sent as a raw DMSG_STATE_SET (not daemon_client_set_state)
// because this process also receives DMSG_DELIVER traffic - see the caveat
// in daemon/client.h. The DMSG_STATE_VALUE reply is picked up and ignored
// in the main recv loop.
static void push_state(daemon_client_t *c, const sim_state_t *s) {
    satellite_state_t st;
    st.op_state             = (sat_op_state_t)s->op_state;
    st.active_experiment_id = s->active_experiment_id;
    st.dce_authority_held   = s->dce_authority_held;
    st.last_updated_met     = s->met_ms;
    daemon_send(daemon_client_fd(c), DMSG_STATE_SET, &st, sizeof(st));
}

static void handle_payload_command(sim_state_t *s, daemon_client_t *c, const uint8_t *data, uint16_t len) {
    if (len < 3) return;
    size_t   off             = 0;
    uint8_t  opcode          = unpack_u8(data, &off);
    uint16_t command_counter = unpack_be16(data, &off);
    uint32_t arg             = (len >= 7) ? unpack_be32(data, &off) : 0;
    uint8_t  status          = 0x00; // ACK unless noted otherwise

    switch (opcode) {
        case CMD_GRANT_DCE_AUTHORITY:
            s->dce_authority_held = 1;
            fprintf(stderr, "[payload_app] DCE authority granted by OBC\n");
            break;
        case CMD_REVOKE_DCE_AUTHORITY:
            s->dce_authority_held = 0;
            fprintf(stderr, "[payload_app] DCE authority revoked by OBC\n");
            break;
        case CMD_TIME_SYNC_PING:
            s->met_ms = arg;
            fprintf(stderr, "[payload_app] time sync: met_ms=%llu\n", (unsigned long long)s->met_ms);
            break;
        case CMD_RUN_EXPERIMENT:
            s->op_state             = 0x01; // Experiment Running
            s->active_experiment_id = (uint8_t)arg;
            fprintf(stderr, "[payload_app] running experiment %u\n", s->active_experiment_id);
            break;
        case CMD_ABORT_EXPERIMENT:
            s->op_state             = 0x00; // Idle
            s->active_experiment_id = 0;
            fprintf(stderr, "[payload_app] experiment aborted\n");
            break;
        case CMD_EMERGENCY_HALT:
            s->op_state = 0xFF; // Fault
            fprintf(stderr, "[payload_app] EMERGENCY HALT commanded\n");
            break;
        default:
            fprintf(stderr, "[payload_app] unknown command opcode 0x%02X\n", opcode);
            status = 0x01; // NACK
            break;
    }

    net_ipc_cmd_response_t resp = { .opcode = opcode, .command_counter = command_counter, .status = status };
    send_to_network(c, NET_IPC_CTRL_SEND_CMD_RESPONSE, &resp, sizeof(resp));
    push_state(c, s);
}

static void handle_adcs_telemetry(sim_state_t *s, const uint8_t *data, uint16_t len) {
    if (len < 80) return;
    size_t off = 8; // skip Mission Time, we track our own MET
    unpack_be_float_array(data, &off, s->current_quat, 4);
    off = 68; // Angular Rates field
    unpack_be_float_array(data, &off, s->angular_rates, 3);
    fprintf(stderr, "[payload_app] ADCS telemetry: quat=[%.3f %.3f %.3f %.3f]\n", (double)s->current_quat[0],
            (double)s->current_quat[1], (double)s->current_quat[2], (double)s->current_quat[3]);
}

static void handle_adcs_response(sim_state_t *s, daemon_client_t *c, const uint8_t *data, uint16_t len) {
    if (len < 4) return;
    size_t   off            = 0;
    uint8_t  request_type   = unpack_u8(data, &off);
    uint16_t request_counter = unpack_be16(data, &off);
    uint8_t  status          = unpack_u8(data, &off);
    fprintf(stderr, "[payload_app] ADCS response: request_type=0x%02X counter=%u status=%s\n", request_type,
            request_counter, status == 0x00 ? "ACK" : "NACK");

    if (request_type == ADCS_REQ_REQUEST_DCE) {
        s->dce_authority_held = (status == 0x00) ? 1 : 0;
        push_state(c, s);
    } else if (request_type == ADCS_REQ_RELINQUISH_DCE && status == 0x00) {
        s->dce_authority_held = 0;
        push_state(c, s);
    }
}

static void handle_deliver(sim_state_t *s, daemon_client_t *c, const uint8_t *buf, uint32_t len) {
    if (len < sizeof(net_ipc_hdr_t)) {
        fprintf(stderr, "[payload_app] short net_ipc frame (%u bytes)\n", len);
        return;
    }
    net_ipc_hdr_t hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    const uint8_t *data = buf + sizeof(hdr);
    if (sizeof(hdr) + hdr.len > len) {
        fprintf(stderr, "[payload_app] truncated net_ipc frame (apid=0x%03X len=%u)\n", hdr.apid, hdr.len);
        return;
    }

    switch (hdr.apid) {
        case APID_CMD_RX:
            handle_payload_command(s, c, data, hdr.len);
            break;
        case APID_ADCS_TELEM_RX:
            handle_adcs_telemetry(s, data, hdr.len);
            break;
        case APID_ADCS_RESP_RX:
            handle_adcs_response(s, c, data, hdr.len);
            break;
        case APID_BULK_EXP_RX:
            handle_bulk_chunk(&g_exp_table_rx, c, data, hdr.len);
            break;
        case APID_BULK_CTRL_RX:
            handle_bulk_chunk(&g_ctrl_algo_rx, c, data, hdr.len);
            break;
        case APID_BULK_ACK_BIDI:
            handle_bulk_ack_response(data, hdr.len);
            break;
        default:
            fprintf(stderr, "[payload_app] delivery on unhandled apid 0x%03X (%u bytes)\n", hdr.apid, hdr.len);
            break;
    }
}

// Cycles through a short, harmless sequence of dummy commands so there's
// always some ADCS request traffic to observe: request authority, ask for
// an orientation, ask for a spin rate, then relinquish authority. The
// Request Counter is "network"'s to assign - this only supplies the
// semantic fields the ICD defines (quat/rates/duration are zeroed by the
// caller for 0xA0/0xA1 and ignored by the receiver; "network" still sends
// them as the full fixed-size message).
static void send_dummy_command(sim_state_t *s, daemon_client_t *c, int step) {
    net_ipc_adcs_request_t req = { 0 };

    switch (step % 4) {
        case 0:
            req.request_type = ADCS_REQ_REQUEST_DCE;
            fprintf(stderr, "[payload_app] -> ADCS request: request DCE authority\n");
            break;
        case 1:
            req.request_type    = ADCS_REQ_TARGET_ORIENTATION;
            req.target_quat[3]  = 1.0f;
            req.target_rates[0] = 0.01f;
            req.target_rates[1] = -0.02f;
            req.duration_ms     = 5000;
            fprintf(stderr, "[payload_app] -> ADCS request: target orientation\n");
            break;
        case 2:
            req.request_type    = ADCS_REQ_TARGET_SPIN_RATE;
            req.target_quat[3]  = 1.0f;
            req.target_rates[0] = 0.01f;
            req.target_rates[1] = -0.02f;
            req.duration_ms     = 3000;
            fprintf(stderr, "[payload_app] -> ADCS request: target spin rate\n");
            break;
        default:
            req.request_type = ADCS_REQ_RELINQUISH_DCE;
            fprintf(stderr, "[payload_app] -> ADCS request: relinquish DCE authority\n");
            break;
    }

    send_to_network(c, NET_IPC_CTRL_SEND_ADCS_REQUEST, &req, sizeof(req));
    (void)s;
}

int main(int argc, char **argv) {
    const char *name = argc > 1 ? argv[1] : NET_IPC_PAYLOAD_APP_NAME;

    daemon_client_t c;
    int              connected = 0;
    for (int attempt = 0; attempt < 10 && !connected; attempt++) {
        if (daemon_client_connect(name, &c) == 0) {
            connected = 1;
            break;
        }
        sleep_ms(300);
    }
    if (!connected) {
        fprintf(stderr, "[%s] could not connect to daemon: %s\n", name, strerror(errno));
        return 1;
    }
    fprintf(stderr, "[%s] connected and registered\n", name);

    sim_state_t s;
    memset(&s, 0, sizeof(s));
    s.cpu_temp_c            = 35.0f;
    s.storage_mb_remaining  = 32000;
    s.current_quat[3]       = 1.0f; // identity quaternion until real telemetry arrives

    // Seed from persisted state before any peer traffic can arrive - see
    // the ordering caveat on daemon_client_get_state in daemon/client.h.
    satellite_state_t persisted;
    if (daemon_client_get_state(&c, &persisted) == 0) {
        s.op_state             = (uint8_t)persisted.op_state;
        s.active_experiment_id = persisted.active_experiment_id;
        s.dce_authority_held   = persisted.dce_authority_held;
        s.met_ms                = persisted.last_updated_met;
        fprintf(stderr, "[%s] seeded state: op_state=0x%02X active_experiment_id=%u dce_authority=%u\n", name,
                s.op_state, s.active_experiment_id, s.dce_authority_held);
    }

    struct pollfd pfd = { .fd = daemon_client_fd(&c), .events = POLLIN };
    int            tick      = 0;
    int            cmd_step  = 0;
    int            bulk_step = 0;

    // Paced off a monotonic deadline rather than "one loop iteration ~= one
    // second": poll() returns immediately whenever there's data to read
    // (e.g. every DMSG_ERROR the daemon queues back because "network" isn't
    // registered yet), so counting iterations as ticks would spin as fast
    // as replies arrive instead of at ~1 Hz.
    struct timespec next_tick;
    clock_gettime(CLOCK_MONOTONIC, &next_tick);
    next_tick.tv_sec += 1;

    for (;;) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        long remaining_ms = (next_tick.tv_sec - now.tv_sec) * 1000 +
                             (next_tick.tv_nsec - now.tv_nsec) / 1000000;
        if (remaining_ms < 0) {
            remaining_ms = 0;
        }

        int ret = poll(&pfd, 1, (int)remaining_ms);
        if (ret < 0) {
            if (errno == EINTR) continue;
            break;
        }

        if (ret > 0 && (pfd.revents & POLLIN)) {
            uint16_t type;
            char     src[DAEMON_NAME_MAX];
            uint8_t  buf[DAEMON_MAX_MSG_PAYLOAD];
            uint32_t len;
            if (daemon_client_recv(&c, &type, src, buf, sizeof(buf), &len) != 0) {
                fprintf(stderr, "[%s] connection to daemon lost\n", name);
                break;
            }

            if (type == DMSG_DELIVER) {
                handle_deliver(&s, &c, buf, len);
            } else if (type == DMSG_ERROR) {
                fprintf(stderr, "[%s] daemon reported an error (peer not up yet?)\n", name);
            }
            // DMSG_STATE_VALUE replies to our fire-and-forget push_state()
            // are read here and intentionally ignored.
            continue; // re-check the deadline instead of ticking early
        }

        // Timed out: the deadline passed with nothing to read, so exactly
        // ~1 Hz tick, matching the ICD's minimum heartbeat rate (Section 2.5).
        next_tick.tv_sec += 1;
        tick++;
        s.met_ms += 1000;
        s.cpu_temp_c = 35.0f + (float)(tick % 10) * 0.3f;

        net_ipc_telemetry_t telem = {
            .met_ms               = s.met_ms,
            .op_state              = s.op_state,
            .active_experiment_id = s.active_experiment_id,
            .cpu_temp_c            = s.cpu_temp_c,
            .storage_mb_remaining = s.storage_mb_remaining,
        };
        send_to_network(&c, NET_IPC_CTRL_SEND_TELEMETRY, &telem, sizeof(telem));

        if (tick % 5 == 0) {
            send_dummy_command(&s, &c, cmd_step++);
        }
        if (tick % 10 == 0) {
            send_dummy_bulk(&c, &s, bulk_step++);
        }
    }

    daemon_client_close(&c);
    return 0;
}
