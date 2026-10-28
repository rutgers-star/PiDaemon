// Rough busOBC-side stand-in for the SPICEnet link described in the ICD.
//
// This is the "other end of the wire" from Daemon/ + Network/ (the payOBC /
// Raspberry Pi side): it owns the RS-422 serial link directly (no daemon/IPC
// layer - the real busOBC is a different physical machine running Yocto +
// cFS, not this repo's daemond architecture) and speaks the exact same
// payload-link framing (PayloadLink) and APID dictionary (PayloadApids,
// ICD Section 5) that Network/src/main.c implements on the Pi side.
//
// It is not cFS. It is a single-process reference/test harness you can run
// on a dev machine (optionally over a USB-serial adapter wired to the Pi's
// RS-422 transceiver) to exercise the real wire protocol end-to-end:
// sending payload commands, approving/vetoing ADCS requests, forwarding
// simulated ADCS telemetry, and receiving + reassembling bulk data
// (calculation/force-torque/video) to disk. See README.md for usage and
// known limitations.
//
// usage: obc_daemon [serial-port]   (default: /dev/ttyUSB0)

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <poll.h>
#include <fcntl.h>
#include <termios.h>

#include <star/payload_apids.h>
#include <payload_link/frame.h>
#include <payload_link/pack.h>

// ---- wire-layout constants, ICD Section 2.3 / 5.8 ----
// Mirrors Network/src/main.c's NET_IPC_MAX_DATA / BULK_WRAPPER_HEADER_SIZE -
// these must match the Pi side exactly or chunk sizing disagrees.
#define OBC_MAX_PACKET_LEN 512 // ICD MAX_PACKET_LENGTH
#define BULK_WRAPPER_HDR_LEN 14 // Transfer ID(4) + Chunk Seq(4) + Total Chunks(4) + Chunk Size(2)
#define MAX_CHUNK_DATA (OBC_MAX_PACKET_LEN - BULK_WRAPPER_HDR_LEN) // 498
#define BULK_ACK_HDR_LEN 5 // Transfer ID(4) + Status(1)
#define MAX_MISSING_CHUNKS ((OBC_MAX_PACKET_LEN - BULK_ACK_HDR_LEN) / 4) // 126

// Payload-link body: one complete CCSDS space packet (same as
// Network/src/main.c): | CCSDS primary header (6B) | DATA |
// Payload commands (APID 0x010) are telecommands and everything else is
// telemetry; none carry a secondary header (ICD RevB D6).
#define LINK_BODY_HDR_SIZE 6
#define LINK_BODY_MAX_DATA (PL_MAX_BODY_LEN - LINK_BODY_HDR_SIZE)

#define CCSDS_VERSION_MASK          UINT16_C(0xE000)
#define CCSDS_TYPE_CMD              UINT16_C(0x1000)
#define CCSDS_APID_MASK             UINT16_C(0x07FF)
#define CCSDS_SEQ_FLAGS_MASK        UINT16_C(0xC000)
#define CCSDS_SEQ_FLAGS_UNSEGMENTED UINT16_C(0xC000)
#define CCSDS_SEQ_COUNT_MASK        UINT16_C(0x3FFF)
_Static_assert(LINK_BODY_MAX_DATA >= OBC_MAX_PACKET_LEN,
               "payload-link body can't fit a full-size packet");

#define SERIAL_BAUD B115200

// Payload command opcodes, ICD Section 5.2.
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

// ICD Section 4.3: busOBC reclaims DCE authority after 10 missed heartbeats
// at the 1 Hz minimum rate (Section 2.5). Reused here as a generic
// "payload link considered down" threshold.
#define WATCHDOG_TIMEOUT_SEC 10

// How long a bulk-data receive session can go without a new chunk before we
// NACK the gaps (ICD 5.9 / 4.5 retransmission behavior).
#define BULK_RX_IDLE_TIMEOUT_SEC 5

// Guards the per-session "received chunk" bitmap against a bogus/corrupt
// Total Chunks field turning into a huge allocation. ~200000 chunks is
// ~100MB of video at the 498-byte chunk payload size, generous for this
// reference harness.
#define MAX_RX_CHUNKS_SUPPORTED 200000u

#define RX_OUTPUT_DIR "obc_rx"
#define MAX_PENDING_SENDS 8

static volatile sig_atomic_t g_running = 1;
static int g_serial_fd = -1;

static int g_link_up = 0;
static time_t g_last_rx_time = 0;

static int g_dce_held_by_pay = 0;
static int g_adcs_auto = 1;
static double g_sim_angle = 0.0; // radians; drives the simulated ADCS passthrough quaternion

typedef struct {
    int      valid;
    uint64_t met_ms;
    uint8_t  op_state;
    uint8_t  active_experiment_id;
    float    cpu_temp_c;
    uint32_t storage_mb;
} pay_telemetry_t;

static pay_telemetry_t g_last_telem;

// One in-flight inbound bulk-data session (ICD 5.8). Raw chunk bytes are
// written straight to disk at their chunk-relative offset (fseek + fwrite)
// rather than reassembled in memory, so sessions aren't bounded by RAM and
// out-of-order chunk arrival still reconstructs the right file. calc/ft
// files are the packed entry structs from ICD 5.8.1/5.8.2 back-to-back, with
// no separate framing - parse offline with Python's struct module using
// those layouts. video files are the raw bytes the payload sent (ICD 5.8.3).
typedef struct {
    const char *label; // human-readable, used in log lines
    const char *slug;  // short tag used in output filenames
    int         active;
    uint32_t    transfer_id;
    uint32_t    total_chunks;
    uint32_t    chunks_received;
    uint8_t    *received; // malloc'd total_chunks bytes, 1 = chunk already on disk
    FILE       *f;
    char        path[512];
    time_t      last_chunk_time;
} bulk_rx_t;

static bulk_rx_t g_calc_rx  = { .label = "payload calculation data", .slug = "calc" };
static bulk_rx_t g_ft_rx    = { .label = "force-torque sensor data", .slug = "ft" };
static bulk_rx_t g_video_rx = { .label = "image/video data",         .slug = "video" };

// Outbound bulk transfers we've started (ICD 5.8.4 Experiment Table / 5.8.5
// Control Algorithm) and are waiting on a Bulk Data Response (0x035) for.
typedef struct {
    int         used;
    uint32_t    transfer_id;
    const char *label;
} pending_send_t;

static pending_send_t g_pending_sends[MAX_PENDING_SENDS];
static uint32_t g_next_transfer_id = 0;

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

// ---- float pack/unpack helpers (payload_link/pack.h only covers integers) ----

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

// ---- serial link ----

// Opens and configures the RS-422 serial link: 8N1, no flow control, raw
// mode. poll()-driven, so the fd is left non-blocking. Mirrors
// Network/src/main.c's open_serial_port() exactly - both ends of the same
// physical link must agree (ICD Section 1.2.2).
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
// to the library's minimum body size), frames it, and writes it to the
// serial link. Refuses to send on an APID the ICD doesn't allow busOBC ->
// payOBC on (Section 3.2 direction column), same guard Network/src/main.c
// applies to app-originated sends.
static int link_send(uint16_t apid, const uint8_t *payload, uint16_t payload_len) {
    if (!star_payload_apid_allowed_bus_to_pay(apid)) {
        fprintf(stderr, "[obc] refusing to send on non-bus->pay apid 0x%03X\n", apid);
        return -1;
    }
    if (payload_len > LINK_BODY_MAX_DATA) {
        fprintf(stderr, "[obc] payload too large for apid 0x%03X (%u bytes)\n", apid, payload_len);
        return -1;
    }

    static uint16_t tx_sequence_count;

    uint8_t  body[LINK_BODY_HDR_SIZE + LINK_BODY_MAX_DATA];
    uint16_t data_len = payload_len > 0 ? payload_len : 1;
    uint16_t type     = apid == STAR_APID_PAYLOAD_COMMAND ? CCSDS_TYPE_CMD : 0;
    size_t   off      = 0;
    pack_be16(body, &off, type | (apid & CCSDS_APID_MASK));
    pack_be16(body, &off, CCSDS_SEQ_FLAGS_UNSEGMENTED | tx_sequence_count);
    pack_be16(body, &off, (uint16_t)(data_len - 1));
    memset(body + off, 0, data_len);
    if (payload_len > 0) {
        memcpy(body + off, payload, payload_len);
    }
    off += data_len;
    tx_sequence_count = (tx_sequence_count + 1) & CCSDS_SEQ_COUNT_MASK;

    uint8_t frame[PL_MAX_FRAME_LEN];
    size_t  frame_len = plframe_encode(body, off, frame);
    if (frame_len == 0) {
        fprintf(stderr, "[obc] failed to frame apid 0x%03X\n", apid);
        return -1;
    }

    return write_all(g_serial_fd, frame, frame_len);
}

// ---- outbound message builders, ICD Section 5 ----

// APID 0x010
static int send_command(uint8_t opcode, uint32_t arg) {
    uint8_t buf[5];
    size_t  off = 0;
    pack_u8(buf, &off, opcode);
    pack_be32(buf, &off, arg);
    return link_send(STAR_APID_PAYLOAD_COMMAND, buf, (uint16_t)off);
}

// APID 0x022
static int send_adcs_response(uint8_t request_type, uint8_t status) {
    uint8_t buf[2];
    size_t  off = 0;
    pack_u8(buf, &off, request_type);
    pack_u8(buf, &off, status);
    return link_send(STAR_APID_ADCS_REQUEST_RESPONSE, buf, (uint16_t)off);
}

// APID 0x020. Stands in for real attitude data the flight software would
// forward - rotates a dummy quaternion slowly about Z so there's always
// something changing to observe on the payload side.
static void send_adcs_telemetry_tick(void) {
    static uint64_t met_ms = 0;
    met_ms += 1000;
    g_sim_angle += 0.05;

    float current_quat[4] = { 0.0f, 0.0f, (float)sin(g_sim_angle / 2.0), (float)cos(g_sim_angle / 2.0) };
    float target_quat[4]  = { 0.0f, 0.0f, 0.0f, 1.0f };
    float torque[4]       = { 0.0f, 0.0f, 0.0f, 0.0f };
    float accel[3]        = { 0.0f, 0.0f, 0.0f };
    float rates[3]         = { 0.0f, 0.0f, 0.05f };

    uint8_t buf[80];
    size_t  off = 0;
    pack_be64(buf, &off, met_ms);
    pack_be_float_array(buf, &off, current_quat, 4);
    pack_be_float_array(buf, &off, target_quat, 4);
    pack_be_float_array(buf, &off, torque, 4);
    pack_be_float_array(buf, &off, accel, 3);
    pack_be_float_array(buf, &off, rates, 3);
    link_send(STAR_APID_ADCS_TELEMETRY_PASSTHROUGH, buf, (uint16_t)off);
}

// APID 0x035
static int send_bulk_ack(uint32_t transfer_id, uint8_t status, const uint32_t *missing, uint32_t num_missing) {
    if (num_missing > MAX_MISSING_CHUNKS) {
        num_missing = MAX_MISSING_CHUNKS;
    }
    uint8_t buf[OBC_MAX_PACKET_LEN];
    size_t  off = 0;
    pack_be32(buf, &off, transfer_id);
    pack_u8(buf, &off, status);
    for (uint32_t i = 0; i < num_missing; i++) {
        pack_be32(buf, &off, missing[i]);
    }
    return link_send(STAR_APID_BULK_DATA_RESPONSE, buf, (uint16_t)off);
}

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

static void register_pending_send(uint32_t transfer_id, const char *label) {
    for (int i = 0; i < MAX_PENDING_SENDS; i++) {
        if (!g_pending_sends[i].used) {
            g_pending_sends[i].used        = 1;
            g_pending_sends[i].transfer_id = transfer_id;
            g_pending_sends[i].label       = label;
            return;
        }
    }
    fprintf(stderr, "[obc] pending-send table full, dropping ack tracking for transfer %u\n", transfer_id);
}

static const char *find_and_clear_pending_send(uint32_t transfer_id) {
    for (int i = 0; i < MAX_PENDING_SENDS; i++) {
        if (g_pending_sends[i].used && g_pending_sends[i].transfer_id == transfer_id) {
            g_pending_sends[i].used = 0;
            return g_pending_sends[i].label;
        }
    }
    return NULL;
}

// APIDs 0x033 / 0x034: chunks `path` through the Bulk Data Transfer Wrapper
// (ICD 5.8) and sends it out over the link. Mirrors
// Daemon/src/video_uploader.c's approach on the Pi side, just reading
// straight off disk instead of going through daemond.
static int send_bulk_file(uint16_t apid, const char *path, const char *label) {
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        fprintf(stderr, "[obc] failed to open %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (fseek(f, 0, SEEK_END) != 0 || ftell(f) < 0) {
        fprintf(stderr, "[obc] failed to size %s: %s\n", path, strerror(errno));
        fclose(f);
        return -1;
    }
    long file_size = ftell(f);
    if ((unsigned long)file_size > UINT32_MAX) {
        fprintf(stderr, "[obc] %s is too large (%ld bytes)\n", path, file_size);
        fclose(f);
        return -1;
    }
    rewind(f);

    uint32_t total_len    = (uint32_t)file_size;
    uint32_t total_chunks = (total_len + MAX_CHUNK_DATA - 1) / MAX_CHUNK_DATA;
    if (total_chunks == 0) {
        total_chunks = 1; // still send one empty chunk so the payload sees the session
    }

    if (g_next_transfer_id == 0) {
        g_next_transfer_id = (uint32_t)time(NULL);
    }
    uint32_t tid = g_next_transfer_id++;

    printf("[obc] -> %s transfer %u: %s (%u bytes, %u chunks)\n", label, tid, path, total_len, total_chunks);

    uint8_t  chunk_buf[MAX_CHUNK_DATA];
    uint8_t  wrapper[BULK_WRAPPER_HDR_LEN + MAX_CHUNK_DATA];
    uint32_t sent = 0;
    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t remaining = total_len - sent;
        uint16_t n          = (uint16_t)(remaining < MAX_CHUNK_DATA ? remaining : MAX_CHUNK_DATA);

        if (n > 0 && fread(chunk_buf, 1, n, f) != n) {
            fprintf(stderr, "[obc] short read on %s at chunk %u: %s\n", path, chunk, strerror(errno));
            fclose(f);
            return -1;
        }

        size_t wlen = pack_bulk_wrapper(wrapper, tid, chunk, total_chunks, chunk_buf, n);
        if (link_send(apid, wrapper, (uint16_t)wlen) != 0) {
            fprintf(stderr, "[obc] send failed at chunk %u\n", chunk);
            fclose(f);
            return -1;
        }
        sent += n;
    }
    fclose(f);

    register_pending_send(tid, label);
    printf("[obc] %s transfer %u fully sent (%u bytes), awaiting payload ack\n", label, tid, sent);
    return 0;
}

// ---- inbound bulk-data reassembly (ICD 5.8), APIDs 0x030/0x031/0x032 ----

static void ensure_output_dir(void) {
    if (mkdir(RX_OUTPUT_DIR, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "[obc] failed to create %s: %s\n", RX_OUTPUT_DIR, strerror(errno));
    }
}

static void bulk_rx_abandon(bulk_rx_t *rx) {
    if (rx->f != NULL) {
        fclose(rx->f);
        rx->f = NULL;
    }
    free(rx->received);
    rx->received = NULL;
    rx->active    = 0;
}

static void bulk_rx_start_session(bulk_rx_t *rx, uint32_t transfer_id, uint32_t total_chunks) {
    if (rx->active) {
        fprintf(stderr, "[obc] %s transfer %u abandoned (%u/%u chunks received) - new transfer %u started\n",
                rx->label, rx->transfer_id, rx->chunks_received, rx->total_chunks, transfer_id);
        bulk_rx_abandon(rx);
    }

    if (total_chunks == 0 || total_chunks > MAX_RX_CHUNKS_SUPPORTED) {
        fprintf(stderr, "[obc] %s transfer %u has unsupported total_chunks=%u, dropping session\n", rx->label,
                transfer_id, total_chunks);
        return;
    }

    rx->received = calloc(total_chunks, 1);
    if (rx->received == NULL) {
        fprintf(stderr, "[obc] %s transfer %u: out of memory for %u-chunk tracking bitmap\n", rx->label,
                transfer_id, total_chunks);
        return;
    }

    time_t    now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    snprintf(rx->path, sizeof(rx->path), "%s/%s_%u_%04d%02d%02d-%02d%02d%02d.bin", RX_OUTPUT_DIR, rx->slug,
             transfer_id, tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday, tmv.tm_hour, tmv.tm_min, tmv.tm_sec);

    rx->f = fopen(rx->path, "w+b");
    if (rx->f == NULL) {
        fprintf(stderr, "[obc] failed to open %s: %s\n", rx->path, strerror(errno));
        free(rx->received);
        rx->received = NULL;
        return;
    }

    rx->active          = 1;
    rx->transfer_id     = transfer_id;
    rx->total_chunks    = total_chunks;
    rx->chunks_received = 0;
    rx->last_chunk_time = now;
    printf("[obc] %s transfer %u started -> %s (%u chunks expected)\n", rx->label, transfer_id, rx->path,
           total_chunks);
}

// Feeds one received Bulk Data Transfer Wrapper chunk (ICD 5.8) into `rx`.
// Writes the chunk's data at its chunk-relative byte offset so out-of-order
// or retransmitted chunks still land in the right place, and ACKs (0x035)
// once every chunk index 0..total_chunks-1 has been seen.
static void bulk_rx_feed_chunk(bulk_rx_t *rx, const uint8_t *data, uint16_t len) {
    if (len < BULK_WRAPPER_HDR_LEN) {
        fprintf(stderr, "[obc] runt %s chunk (%u bytes)\n", rx->label, len);
        return;
    }

    size_t   off           = 0;
    uint32_t transfer_id   = unpack_be32(data, &off);
    uint32_t chunk_seq     = unpack_be32(data, &off);
    uint32_t total_chunks  = unpack_be32(data, &off);
    uint16_t n              = unpack_be16(data, &off);
    const uint8_t *chunk_data = data + off;

    if ((size_t)off + n > (size_t)len) {
        fprintf(stderr, "[obc] %s transfer %u chunk %u claims %u bytes, only %zu in frame\n", rx->label,
                transfer_id, chunk_seq, n, (size_t)len - off);
        return;
    }

    if (!rx->active || rx->transfer_id != transfer_id) {
        bulk_rx_start_session(rx, transfer_id, total_chunks);
        if (!rx->active) {
            return;
        }
    }

    if (chunk_seq >= rx->total_chunks || n > MAX_CHUNK_DATA) {
        fprintf(stderr, "[obc] %s transfer %u: chunk %u out of bounds, dropping\n", rx->label, transfer_id,
                chunk_seq);
        return;
    }

    rx->last_chunk_time = time(NULL);

    if (!rx->received[chunk_seq]) {
        if (fseek(rx->f, (long)chunk_seq * MAX_CHUNK_DATA, SEEK_SET) != 0 ||
            (n > 0 && fwrite(chunk_data, 1, n, rx->f) != n)) {
            fprintf(stderr, "[obc] %s transfer %u: write failed at chunk %u: %s\n", rx->label, transfer_id,
                    chunk_seq, strerror(errno));
            return;
        }
        rx->received[chunk_seq] = 1;
        rx->chunks_received++;
    }

    if (rx->chunks_received < rx->total_chunks) {
        return;
    }

    fflush(rx->f);
    fclose(rx->f);
    rx->f = NULL;
    printf("[obc] %s transfer %u complete -> %s (%u chunks)\n", rx->label, transfer_id, rx->path, rx->total_chunks);
    send_bulk_ack(transfer_id, 0x00, NULL, 0);
    free(rx->received);
    rx->received = NULL;
    rx->active    = 0;
}

// Called once per second. NACKs a stalled session with the chunk indices
// still missing (ICD 5.9), then waits another full timeout window before
// NACKing again so a slow sender isn't spammed every tick.
static void bulk_rx_check_timeout(bulk_rx_t *rx, time_t now) {
    if (!rx->active || now - rx->last_chunk_time < BULK_RX_IDLE_TIMEOUT_SEC) {
        return;
    }

    uint32_t missing[MAX_MISSING_CHUNKS];
    uint32_t num_missing = 0;
    for (uint32_t i = 0; i < rx->total_chunks && num_missing < MAX_MISSING_CHUNKS; i++) {
        if (!rx->received[i]) {
            missing[num_missing++] = i;
        }
    }
    fprintf(stderr, "[obc] %s transfer %u stalled (%u/%u chunks) - NACKing %u missing chunk(s)\n", rx->label,
            rx->transfer_id, rx->chunks_received, rx->total_chunks, num_missing);
    send_bulk_ack(rx->transfer_id, 0x01, missing, num_missing);
    rx->last_chunk_time = now;
}

// ---- inbound dispatch ----

static void handle_payload_telemetry(const uint8_t *data, uint16_t len) {
    if (len < 18) {
        fprintf(stderr, "[obc] runt telemetry frame (%u bytes)\n", len);
        return;
    }
    size_t off              = 0;
    g_last_telem.met_ms      = unpack_be64(data, &off);
    g_last_telem.op_state    = unpack_u8(data, &off);
    g_last_telem.active_experiment_id = unpack_u8(data, &off);
    g_last_telem.cpu_temp_c  = unpack_be_float(data, &off);
    g_last_telem.storage_mb  = unpack_be32(data, &off);
    g_last_telem.valid       = 1;
    printf("[obc] <- telemetry met=%llums state=0x%02X exp=%u temp=%.1fC storage=%uMB\n",
           (unsigned long long)g_last_telem.met_ms, g_last_telem.op_state, g_last_telem.active_experiment_id,
           (double)g_last_telem.cpu_temp_c, g_last_telem.storage_mb);
}

static void handle_payload_command_response(const uint8_t *data, uint16_t len) {
    if (len < 2) {
        return;
    }
    size_t  off    = 0;
    uint8_t opcode = unpack_u8(data, &off);
    uint8_t status = unpack_u8(data, &off);
    printf("[obc] <- command response opcode=0x%02X status=%s\n", opcode, status == 0x00 ? "ACK" : "NACK");
}

// APID 0x021. The ICD notes the busOBC "retains the authority to veto"
// these; this harness always ACKs - a real flight software ADCS/safety
// check (torque limits, current mode, etc., ICD Section 4.3) would replace
// that policy here.
static void handle_adcs_pointing_request(const uint8_t *data, uint16_t len) {
    if (len < 1) {
        fprintf(stderr, "[obc] runt ADCS pointing request\n");
        return;
    }
    size_t  off          = 0;
    uint8_t request_type = unpack_u8(data, &off);

    switch (request_type) {
        case ADCS_REQ_TARGET_ORIENTATION:
        case ADCS_REQ_TARGET_SPIN_RATE: {
            float    quat[4]  = { 0 };
            float    rates[3] = { 0 };
            uint32_t duration = 0;
            if (len >= 29) {
                unpack_be_float_array(data, &off, quat, 4);
                unpack_be_float_array(data, &off, rates, 3);
            }
            if (len >= 33) {
                duration = unpack_be32(data, &off);
            }
            printf("[obc] <- ADCS pointing request: %s quat=[%.3f %.3f %.3f %.3f] rates=[%.3f %.3f %.3f] "
                   "duration=%ums\n",
                   request_type == ADCS_REQ_TARGET_ORIENTATION ? "target orientation" : "target spin rate",
                   (double)quat[0], (double)quat[1], (double)quat[2], (double)quat[3], (double)rates[0],
                   (double)rates[1], (double)rates[2], duration);
            break;
        }
        case ADCS_REQ_REQUEST_DCE:
            printf("[obc] <- ADCS pointing request: request DCE authority\n");
            break;
        case ADCS_REQ_RELINQUISH_DCE:
            printf("[obc] <- ADCS pointing request: relinquish DCE authority\n");
            break;
        default:
            printf("[obc] <- ADCS pointing request: unknown type 0x%02X\n", request_type);
            break;
    }

    send_adcs_response(request_type, 0x00);

    if (request_type == ADCS_REQ_REQUEST_DCE) {
        g_dce_held_by_pay = 1;
        printf("[obc] -> DCE authority granted to payload\n");
    } else if (request_type == ADCS_REQ_RELINQUISH_DCE) {
        g_dce_held_by_pay = 0;
        printf("[obc] -> DCE authority relinquished\n");
    }
}

// APID 0x035, received here only as a response to a 0x033/0x034 transfer we
// initiated (the payload is the one ACKing/NACKing our bulk sends).
static void handle_bulk_data_response(const uint8_t *data, uint16_t len) {
    if (len < BULK_ACK_HDR_LEN) {
        return;
    }
    size_t   off          = 0;
    uint32_t transfer_id  = unpack_be32(data, &off);
    uint8_t  status        = unpack_u8(data, &off);
    uint32_t num_missing   = (uint32_t)(len - BULK_ACK_HDR_LEN) / 4;
    const char *label      = find_and_clear_pending_send(transfer_id);

    if (status == 0x00) {
        printf("[obc] <- bulk transfer %u (%s) ACKed by payload\n", transfer_id, label != NULL ? label : "?");
    } else {
        printf("[obc] <- bulk transfer %u (%s) NACKed by payload, %u missing chunk(s)\n", transfer_id,
               label != NULL ? label : "?", num_missing);
    }
}

static void dispatch_from_pay(uint16_t apid, const uint8_t *data, uint16_t len) {
    if (!star_payload_apid_allowed_pay_to_bus(apid)) {
        fprintf(stderr, "[obc] frame on unexpected apid 0x%03X (%u bytes)\n", apid, len);
        return;
    }

    switch (apid) {
        case STAR_APID_PAYLOAD_TELEMETRY:
            handle_payload_telemetry(data, len);
            break;
        case STAR_APID_PAYLOAD_COMMAND_RESPONSE:
            handle_payload_command_response(data, len);
            break;
        case STAR_APID_ADCS_POINTING_REQUEST:
            handle_adcs_pointing_request(data, len);
            break;
        case STAR_APID_PAYLOAD_CALCULATION_DATA:
            bulk_rx_feed_chunk(&g_calc_rx, data, len);
            break;
        case STAR_APID_FORCE_TORQUE_SENSOR_DATA:
            bulk_rx_feed_chunk(&g_ft_rx, data, len);
            break;
        case STAR_APID_IMAGE_VIDEO_DATA:
            bulk_rx_feed_chunk(&g_video_rx, data, len);
            break;
        case STAR_APID_BULK_DATA_RESPONSE:
            handle_bulk_data_response(data, len);
            break;
        default:
            fprintf(stderr, "[obc] no handler for apid 0x%03X (%u bytes)\n", apid, len);
            break;
    }
}

// ---- watchdog (ICD 4.2 / 4.3) ----

static void check_watchdog(time_t now) {
    if (g_last_rx_time == 0) {
        return; // haven't heard from the payload at all yet
    }
    if (g_link_up && now - g_last_rx_time > WATCHDOG_TIMEOUT_SEC) {
        g_link_up = 0;
        fprintf(stderr,
                "[obc] WATCHDOG: no traffic from payload for >%ds - link may be down (ICD 4.2/4.3)\n",
                WATCHDOG_TIMEOUT_SEC);
    }
}

// ---- console ----

static void print_help(void) {
    printf(
        "commands:\n"
        "  grant                 send Grant DCE Authority command (0x01)\n"
        "  revoke                send Revoke DCE Authority command (0x02)\n"
        "  timesync              send Time Sync Ping command (0x03)\n"
        "  run <experiment_id>   send Run Experiment command (0x20)\n"
        "  abort                 send Abort Current Experiment command (0x21)\n"
        "  halt                  send Emergency Halt / Safe Mode command (0xFF)\n"
        "  sendtable <file.csv>  bulk-transfer an experiment table to the payload (APID 0x033)\n"
        "  sendalgo <file>       bulk-transfer a control algorithm to the payload (APID 0x034)\n"
        "  adcs <on|off>         toggle simulated ADCS telemetry passthrough (APID 0x020)\n"
        "  status                print link / telemetry / transfer status\n"
        "  help                  show this message\n"
        "  quit                  shut down\n"
        "\n"
        "Inbound payload telemetry (0x011), command responses (0x012), ADCS pointing\n"
        "requests (0x021, auto-ACKed), and bulk data (0x030/0x031/0x032, auto-ACKed and\n"
        "written under '" RX_OUTPUT_DIR "/') are all handled automatically in the background,\n"
        "so this also runs fine headless (stdin closed/redirected) as a one-way logger.\n");
}

static void print_bulk_rx_status(const bulk_rx_t *rx) {
    if (rx->active) {
        printf("  %s: transfer %u in progress (%u/%u chunks) -> %s\n", rx->label, rx->transfer_id,
               rx->chunks_received, rx->total_chunks, rx->path);
    } else {
        printf("  %s: idle\n", rx->label);
    }
}

static void print_status(void) {
    time_t now = time(NULL);
    printf("---- obc status ----\n");
    printf("link: %s", g_link_up ? "up" : (g_last_rx_time == 0 ? "never heard from payload" : "down (watchdog)"));
    if (g_last_rx_time != 0) {
        printf(" (last rx %lds ago)", (long)(now - g_last_rx_time));
    }
    printf("\n");
    printf("dce authority held by payload: %s\n", g_dce_held_by_pay ? "yes" : "no");
    printf("adcs telemetry passthrough: %s\n", g_adcs_auto ? "on" : "off");
    if (g_last_telem.valid) {
        printf("last telemetry: met=%llums state=0x%02X exp=%u temp=%.1fC storage=%uMB\n",
               (unsigned long long)g_last_telem.met_ms, g_last_telem.op_state, g_last_telem.active_experiment_id,
               (double)g_last_telem.cpu_temp_c, g_last_telem.storage_mb);
    } else {
        printf("last telemetry: none received yet\n");
    }
    print_bulk_rx_status(&g_calc_rx);
    print_bulk_rx_status(&g_ft_rx);
    print_bulk_rx_status(&g_video_rx);

    int pending = 0;
    for (int i = 0; i < MAX_PENDING_SENDS; i++) {
        if (g_pending_sends[i].used) {
            pending++;
        }
    }
    printf("pending outbound bulk sends awaiting ack: %d\n", pending);
    printf("---------------------\n");
}

static void handle_console_line(char *line) {
    char *cmd = strtok(line, " \t\r\n");
    if (cmd == NULL) {
        return;
    }

    if (strcmp(cmd, "help") == 0) {
        print_help();
    } else if (strcmp(cmd, "grant") == 0) {
        send_command(CMD_GRANT_DCE_AUTHORITY, 0);
        printf("[obc] -> grant DCE authority\n");
    } else if (strcmp(cmd, "revoke") == 0) {
        send_command(CMD_REVOKE_DCE_AUTHORITY, 0);
        printf("[obc] -> revoke DCE authority\n");
    } else if (strcmp(cmd, "timesync") == 0) {
        uint32_t met = (uint32_t)time(NULL); // stand-in MET source for this harness
        send_command(CMD_TIME_SYNC_PING, met);
        printf("[obc] -> time sync ping (demo met=%u)\n", met);
    } else if (strcmp(cmd, "run") == 0) {
        char    *arg = strtok(NULL, " \t\r\n");
        uint32_t id  = arg != NULL ? (uint32_t)strtoul(arg, NULL, 0) : 0;
        send_command(CMD_RUN_EXPERIMENT, id);
        printf("[obc] -> run experiment %u\n", id);
    } else if (strcmp(cmd, "abort") == 0) {
        send_command(CMD_ABORT_EXPERIMENT, 0);
        printf("[obc] -> abort experiment\n");
    } else if (strcmp(cmd, "halt") == 0) {
        send_command(CMD_EMERGENCY_HALT, 0);
        printf("[obc] -> EMERGENCY HALT\n");
    } else if (strcmp(cmd, "sendtable") == 0) {
        char *path = strtok(NULL, " \t\r\n");
        if (path == NULL) {
            printf("usage: sendtable <file.csv>\n");
        } else {
            send_bulk_file(STAR_APID_EXPERIMENT_TABLE, path, "experiment table");
        }
    } else if (strcmp(cmd, "sendalgo") == 0) {
        char *path = strtok(NULL, " \t\r\n");
        if (path == NULL) {
            printf("usage: sendalgo <file>\n");
        } else {
            send_bulk_file(STAR_APID_CONTROL_ALGORITHM, path, "control algorithm");
        }
    } else if (strcmp(cmd, "adcs") == 0) {
        char *arg = strtok(NULL, " \t\r\n");
        if (arg != NULL && strcmp(arg, "off") == 0) {
            g_adcs_auto = 0;
            printf("[obc] ADCS telemetry passthrough: off\n");
        } else if (arg != NULL && strcmp(arg, "on") == 0) {
            g_adcs_auto = 1;
            printf("[obc] ADCS telemetry passthrough: on\n");
        } else {
            printf("usage: adcs <on|off> (currently %s)\n", g_adcs_auto ? "on" : "off");
        }
    } else if (strcmp(cmd, "status") == 0) {
        print_status();
    } else if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        printf("[obc] shutting down\n");
        g_running = 0;
    } else {
        printf("[obc] unknown command '%s' (try 'help')\n", cmd);
    }
}

int main(int argc, char **argv) {
    const char *portname = argc > 1 ? argv[1] : "/dev/ttyUSB0";

    // stdout is fully-buffered (not line-buffered) by glibc whenever it
    // isn't a tty - i.e. whenever this runs as an actual daemon, piped to a
    // log file or a supervisor. Without this, log lines sit in a 4KB buffer
    // instead of appearing as they happen.
    setvbuf(stdout, NULL, _IOLBF, BUFSIZ);

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    ensure_output_dir();

    g_serial_fd = open_serial_port(portname);
    if (g_serial_fd < 0) {
        perror("[obc] failed to open serial port");
        return EXIT_FAILURE;
    }
    printf("[obc] opened serial link on %s @ 115200 8N1\n", portname);

    plframe_decode_ctx_t decoder;
    plframe_decode_init(&decoder);

    printf("[obc] busOBC SPICEnet link harness - rough reference, not flight software\n");
    print_help();
    printf("obc> ");
    fflush(stdout);

    struct pollfd pfds[2];
    pfds[0].fd     = g_serial_fd;
    pfds[0].events = POLLIN;
    pfds[1].fd     = STDIN_FILENO;
    pfds[1].events = POLLIN;

    uint8_t rx_buf[256];
    char    line[256];
    time_t  last_tick = 0;

    while (g_running) {
        int ret = poll(pfds, 2, 250);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("[obc] poll");
            break;
        }

        if (ret > 0) {
            if (pfds[1].fd >= 0 && (pfds[1].revents & POLLIN)) {
                if (fgets(line, sizeof(line), stdin) != NULL) {
                    handle_console_line(line);
                    if (g_running) {
                        printf("obc> ");
                        fflush(stdout);
                    }
                } else {
                    // stdin closed (e.g. running under a service manager) -
                    // keep running headless as a background logger/responder.
                    pfds[1].fd = -1;
                }
            }

            if (pfds[0].revents & POLLIN) {
                int bytes_read = (int)read(g_serial_fd, rx_buf, sizeof(rx_buf));
                for (int i = 0; i < bytes_read; i++) {
                    plframe_decode_result_t result = plframe_decode_feed(&decoder, rx_buf[i]);

                    switch (result) {
                        case PL_DECODE_NEED_MORE:
                            break;

                        case PL_DECODE_OK: {
                            g_last_rx_time = time(NULL);
                            if (!g_link_up) {
                                g_link_up = 1;
                                printf("[obc] payload link up\n");
                            }
                            size_t off = 0;
                            if (decoder.body_len <= LINK_BODY_HDR_SIZE) {
                                fprintf(stderr, "[obc] runt payload-link body (%u bytes)\n", decoder.body_len);
                                break;
                            }
                            uint16_t id       = unpack_be16(decoder.body, &off);
                            uint16_t sequence = unpack_be16(decoder.body, &off);
                            uint16_t data_len = (uint16_t)(unpack_be16(decoder.body, &off) + 1);
                            uint16_t apid     = id & CCSDS_APID_MASK;
                            if ((id & CCSDS_VERSION_MASK) != 0) {
                                fprintf(stderr, "[obc] dropped packet: CCSDS version is not 0\n");
                                break;
                            }
                            if ((size_t)off + data_len != decoder.body_len) {
                                fprintf(stderr, "[obc] apid 0x%03X: CCSDS length implies %u data bytes, body has %u\n",
                                        apid, data_len, (unsigned)(decoder.body_len - off));
                                break;
                            }
                            if ((sequence & CCSDS_SEQ_FLAGS_MASK) != CCSDS_SEQ_FLAGS_UNSEGMENTED) {
                                fprintf(stderr, "[obc] apid 0x%03X: dropped segmented packet\n", apid);
                                break;
                            }
                            dispatch_from_pay(apid, decoder.body + off, data_len);
                            break;
                        }

                        case PL_DECODE_BAD_CRC:
                            fprintf(stderr, "[obc] dropped payload-link frame with bad CRC\n");
                            break;

                        case PL_DECODE_RESYNC:
                            fprintf(stderr, "[obc] payload-link decoder resynced\n");
                            break;
                    }
                }
            }
        }

        time_t now = time(NULL);
        if (now != last_tick) {
            last_tick = now;
            if (g_adcs_auto) {
                send_adcs_telemetry_tick();
            }
            check_watchdog(now);
            bulk_rx_check_timeout(&g_calc_rx, now);
            bulk_rx_check_timeout(&g_ft_rx, now);
            bulk_rx_check_timeout(&g_video_rx, now);
        }
    }

    bulk_rx_abandon(&g_calc_rx);
    bulk_rx_abandon(&g_ft_rx);
    bulk_rx_abandon(&g_video_rx);
    close(g_serial_fd);
    return EXIT_SUCCESS;
}
