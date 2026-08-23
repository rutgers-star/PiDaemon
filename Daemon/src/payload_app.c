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
// It never touches the serial link itself. It only talks to the "network"
// process (Network/src/main.c) via daemond, using the net_ipc_hdr_t framing
// in net_ipc.h: daemon_client_send(..., "network", ...) to transmit, and
// DMSG_DELIVER frames from "network" for whatever the OBC sent.
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

// APIDs this process sends on / receives on (ICD Section 3.2).
#define APID_CMD_RX        0x010
#define APID_TELEM_TX       0x011
#define APID_CMD_RESP_TX    0x012
#define APID_ADCS_TELEM_RX 0x020
#define APID_ADCS_REQ_TX    0x021
#define APID_ADCS_RESP_RX  0x022
#define APID_BULK_CALC_TX   0x030
#define APID_BULK_FT_TX     0x031
#define APID_BULK_VID_TX    0x032
#define APID_BULK_EXP_RX   0x033
#define APID_BULK_CTRL_RX  0x034
#define APID_BULK_ACK_BIDI  0x035

// Bulk Data Transfer Wrapper layout, ICD Section 5.8.
#define BULK_WRAPPER_HEADER_SIZE 14
#define MAX_CHUNK_DATA (NET_IPC_MAX_DATA - BULK_WRAPPER_HEADER_SIZE)

#define BULK_CALC_ENTRY_SIZE 36
#define MAX_CALC_ENTRIES_PER_CHUNK (MAX_CHUNK_DATA / BULK_CALC_ENTRY_SIZE)

#define BULK_FT_ENTRY_SIZE 32
#define MAX_FT_ENTRIES_PER_CHUNK (MAX_CHUNK_DATA / BULK_FT_ENTRY_SIZE)

#define BULK_ACK_HEADER_SIZE 5

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

// Bulk data entry structures, ICD Sections 5.8.1/5.8.2.
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

// ---- packing helpers (mirrors Network/src/main.c) ----

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

// ---- outbound message builders, ICD Section 5 ----

// APID 0x011
static size_t build_telemetry(const sim_state_t *s, uint8_t *out) {
    size_t off = 0;
    pack_be64(out, &off, s->met_ms);
    pack_u8(out, &off, s->op_state);
    pack_u8(out, &off, s->active_experiment_id);
    pack_be_float(out, &off, s->cpu_temp_c);
    pack_be32(out, &off, s->storage_mb_remaining);
    return off;
}

// APID 0x012
static size_t build_cmd_response(uint8_t opcode, uint8_t status, uint8_t *out) {
    size_t off = 0;
    pack_u8(out, &off, opcode);
    pack_u8(out, &off, status);
    return off;
}

// APID 0x021
static size_t build_adcs_request(uint8_t request_type, const float quat[4], const float rates[3],
                                  uint32_t duration_ms, uint8_t *out) {
    size_t off = 0;
    pack_u8(out, &off, request_type);
    if (request_type != ADCS_REQ_REQUEST_DCE && request_type != ADCS_REQ_RELINQUISH_DCE) {
        pack_be_float_array(out, &off, quat, 4);
        pack_be_float_array(out, &off, rates, 3);
        pack_be32(out, &off, duration_ms);
    }
    return off;
}

// Wraps `data` in net_ipc_hdr_t and hands it to the daemon to relay to the
// "network" process. Fire-and-forget: a bad/absent peer surfaces later as a
// DMSG_ERROR on our next recv, which the main loop just logs.
static int send_to_network(daemon_client_t *c, uint16_t apid, const uint8_t *data, uint16_t len) {
    uint8_t frame[sizeof(net_ipc_hdr_t) + NET_IPC_MAX_DATA];
    net_ipc_hdr_t hdr = { .apid = apid, .len = len };
    memcpy(frame, &hdr, sizeof(hdr));
    memcpy(frame + sizeof(hdr), data, len);
    return daemon_client_send(c, NET_IPC_NETWORK_NAME, frame, (uint32_t)(sizeof(hdr) + len));
}

// ---- bulk data transfer, ICD Section 5.8 ----

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

// Packs the common Bulk Data Transfer Wrapper header + data, returns total
// wrapper length.
static size_t pack_bulk_wrapper(uint8_t *frame, uint32_t transfer_id, uint32_t chunk_seq, uint32_t total_chunks,
                                 const uint8_t *data, uint16_t n) {
    size_t off = 0;
    pack_be32(frame, &off, transfer_id);
    pack_be32(frame, &off, chunk_seq);
    pack_be32(frame, &off, total_chunks);
    pack_be16(frame, &off, n);
    memcpy(frame + off, data, n);
    off += n;
    return off;
}

// Splits `data` (total_len bytes, already flattened/serialized) into
// max_chunk_data-sized Bulk Data Transfer Wrapper chunks and sends each one
// to "network" on `apid`.
static void send_bulk(daemon_client_t *c, uint16_t apid, uint32_t transfer_id, const uint8_t *data,
                       uint32_t total_len, uint16_t max_chunk_data) {
    uint32_t total_chunks = (total_len + max_chunk_data - 1) / max_chunk_data;
    if (total_chunks == 0) {
        total_chunks = 1; // still send one (empty) chunk so the OBC sees the session
    }

    uint8_t wrapper[BULK_WRAPPER_HEADER_SIZE + MAX_CHUNK_DATA];
    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t start     = chunk * max_chunk_data;
        uint32_t remaining = total_len - start;
        uint16_t n         = (uint16_t)(remaining < max_chunk_data ? remaining : max_chunk_data);

        size_t wlen = pack_bulk_wrapper(wrapper, transfer_id, chunk, total_chunks, data + start, n);
        send_to_network(c, apid, wrapper, (uint16_t)wlen);
    }
}

static uint32_t g_next_transfer_id = 1;

// APID 0x030: a handful of dummy attitude-control-loop samples.
static void send_bulk_calc_dummy(daemon_client_t *c, const sim_state_t *s) {
    struct bulk_calc_entry entries[5];
    for (int i = 0; i < 5; i++) {
        entries[i].mission_time      = s->met_ms + (uint64_t)i * 100;
        entries[i].angular_accel[0]  = 0.001f * (float)i;
        entries[i].angular_accel[1]  = -0.002f * (float)i;
        entries[i].angular_accel[2]  = 0.0f;
        entries[i].commanded_torque[0] = 0.01f;
        entries[i].commanded_torque[1] = 0.02f;
        entries[i].commanded_torque[2] = 0.0f;
        entries[i].commanded_torque[3] = -0.01f;
    }

    uint8_t flat[5 * BULK_CALC_ENTRY_SIZE];
    size_t  off = 0;
    for (int i = 0; i < 5; i++) {
        off += pack_calc_entry(flat + off, &entries[i]);
    }

    uint32_t tid = g_next_transfer_id++;
    fprintf(stderr, "[payload_app] -> bulk calculation data transfer id=%u (5 entries)\n", tid);
    send_bulk(c, APID_BULK_CALC_TX, tid, flat, (uint32_t)off, MAX_CALC_ENTRIES_PER_CHUNK * BULK_CALC_ENTRY_SIZE);
}

// APID 0x031: a handful of dummy force-torque sensor samples.
static void send_bulk_ft_dummy(daemon_client_t *c, const sim_state_t *s) {
    struct bulk_ft_entry entries[5];
    for (int i = 0; i < 5; i++) {
        entries[i].mission_time    = s->met_ms + (uint64_t)i * 100;
        entries[i].force_vector[0] = 0.1f * (float)i;
        entries[i].force_vector[1] = -0.05f;
        entries[i].force_vector[2] = 0.0f;
        entries[i].torque_vector[0] = 0.0f;
        entries[i].torque_vector[1] = 0.02f;
        entries[i].torque_vector[2] = -0.01f * (float)i;
    }

    uint8_t flat[5 * BULK_FT_ENTRY_SIZE];
    size_t  off = 0;
    for (int i = 0; i < 5; i++) {
        off += pack_ft_entry(flat + off, &entries[i]);
    }

    uint32_t tid = g_next_transfer_id++;
    fprintf(stderr, "[payload_app] -> bulk force-torque data transfer id=%u (5 entries)\n", tid);
    send_bulk(c, APID_BULK_FT_TX, tid, flat, (uint32_t)off, MAX_FT_ENTRIES_PER_CHUNK * BULK_FT_ENTRY_SIZE);
}

// APID 0x032: a small dummy "video frame" (no entry structure - raw bytes
// per ICD 5.8.3), deliberately larger than one chunk to exercise multi-chunk
// reassembly on the OBC side.
static void send_bulk_video_dummy(daemon_client_t *c) {
    uint8_t frame_data[1200];
    for (size_t i = 0; i < sizeof(frame_data); i++) {
        frame_data[i] = (uint8_t)(i & 0xFF);
    }

    uint32_t tid = g_next_transfer_id++;
    fprintf(stderr, "[payload_app] -> bulk video data transfer id=%u (%zu bytes)\n", tid, sizeof(frame_data));
    send_bulk(c, APID_BULK_VID_TX, tid, frame_data, (uint32_t)sizeof(frame_data), MAX_CHUNK_DATA);
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

    uint8_t resp[BULK_ACK_HEADER_SIZE];
    size_t  roff = 0;
    pack_be32(resp, &roff, transfer_id);
    pack_u8(resp, &roff, 0x00); // ACK - a real implementation would NACK with
                                // the gaps in rx->seen[] on a session timeout
    send_to_network(c, APID_BULK_ACK_BIDI, resp, (uint16_t)roff);
}

// APID 0x035, when it's the OBC responding to one of *our* 0x030/0x031/0x032
// sends (as opposed to us responding to the OBC's 0x033/0x034 sends above).
static void handle_bulk_ack_response(const uint8_t *data, uint16_t len) {
    if (len < BULK_ACK_HEADER_SIZE) return;
    size_t   off          = 0;
    uint32_t transfer_id  = unpack_be32(data, &off);
    uint8_t  status        = unpack_u8(data, &off);

    if (status == 0x00) {
        fprintf(stderr, "[payload_app] bulk transfer %u ACKed by OBC\n", transfer_id);
    } else {
        uint32_t num_missing = (len - BULK_ACK_HEADER_SIZE) / 4;
        fprintf(stderr, "[payload_app] bulk transfer %u NACKed by OBC, %u missing chunk(s)\n", transfer_id,
                num_missing);
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
    if (len < 1) return;
    size_t  off    = 0;
    uint8_t opcode = unpack_u8(data, &off);
    uint32_t arg   = (len >= 5) ? unpack_be32(data, &off) : 0;
    uint8_t status = 0x00; // ACK unless noted otherwise

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

    uint8_t resp[2];
    size_t  rlen = build_cmd_response(opcode, status, resp);
    send_to_network(c, APID_CMD_RESP_TX, resp, (uint16_t)rlen);
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
    if (len < 2) return;
    uint8_t request_type = data[0];
    uint8_t status        = data[1];
    fprintf(stderr, "[payload_app] ADCS response: request_type=0x%02X status=%s\n", request_type,
            status == 0x00 ? "ACK" : "NACK");

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
// an orientation, ask for a spin rate, then relinquish authority.
static void send_dummy_command(sim_state_t *s, daemon_client_t *c, int step) {
    static const float quat[4]  = { 0.0f, 0.0f, 0.0f, 1.0f };
    static const float rates[3] = { 0.01f, -0.02f, 0.00f };
    uint8_t            buf[33];
    size_t              len;

    switch (step % 4) {
        case 0:
            len = build_adcs_request(ADCS_REQ_REQUEST_DCE, quat, rates, 0, buf);
            fprintf(stderr, "[payload_app] -> ADCS request: request DCE authority\n");
            break;
        case 1:
            len = build_adcs_request(ADCS_REQ_TARGET_ORIENTATION, quat, rates, 5000, buf);
            fprintf(stderr, "[payload_app] -> ADCS request: target orientation\n");
            break;
        case 2:
            len = build_adcs_request(ADCS_REQ_TARGET_SPIN_RATE, quat, rates, 3000, buf);
            fprintf(stderr, "[payload_app] -> ADCS request: target spin rate\n");
            break;
        default:
            len = build_adcs_request(ADCS_REQ_RELINQUISH_DCE, quat, rates, 0, buf);
            fprintf(stderr, "[payload_app] -> ADCS request: relinquish DCE authority\n");
            break;
    }

    send_to_network(c, APID_ADCS_REQ_TX, buf, (uint16_t)len);
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

        uint8_t telem[18];
        size_t  telem_len = build_telemetry(&s, telem);
        send_to_network(&c, APID_TELEM_TX, telem, (uint16_t)telem_len);

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
