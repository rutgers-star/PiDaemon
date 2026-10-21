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

// Bulk Data Transfer Wrapper layout constants (application-level, carried as
// the payload of a STAR_APID_* bulk-transfer message inside a payload-link
// frame body - see pack_bulk_wrapper()).
#define BULK_WRAPPER_HEADER_SIZE 14
#define MAX_CHUNK_DATA (NET_IPC_MAX_DATA - BULK_WRAPPER_HEADER_SIZE)

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

// ---- outbound send scheduling ----
//
// Every frame bound for the OBC is encoded once and enqueued onto one of
// two priority bands instead of being written immediately:
//
//   NORMAL - telemetry, command responses, ADCS requests, bulk acks. Small
//            and latency sensitive: the ICD's 1 Hz minimum heartbeat (and
//            obc_daemon's missed-heartbeat watchdog) depend on these not
//            queuing up behind a bulk transfer.
//   BULK   - calculation/force-torque/video chunks. Fine to wait for
//            whatever bandwidth NORMAL traffic isn't using.
//
// service_outbound() sends at most one frame's worth of progress per call:
// NORMAL is always drained first, so a multi-minute bulk transfer can never
// delay NORMAL traffic queued behind it by more than a single frame's write
// time. The main loop calls it every iteration, which is also what gives a
// long transfer a chance to proceed without ever blocking the loop on a
// slow serial write.
typedef struct {
    uint8_t data[PL_MAX_FRAME_LEN];
    size_t  len;
} tx_frame_t;

#define TX_QUEUE_CAP 64

typedef struct {
    tx_frame_t frames[TX_QUEUE_CAP];
    int        head;
    int        count;
} tx_queue_t;

static tx_queue_t g_normal_tx;
static tx_queue_t g_bulk_tx;

static int tx_queue_push(tx_queue_t *q, const uint8_t *data, size_t len) {
    if (q->count >= TX_QUEUE_CAP) {
        return -1;
    }
    int slot = (q->head + q->count) % TX_QUEUE_CAP;
    memcpy(q->frames[slot].data, data, len);
    q->frames[slot].len = len;
    q->count++;
    return 0;
}

static int tx_queue_pop(tx_queue_t *q, uint8_t *out, size_t *out_len) {
    if (q->count == 0) {
        return -1;
    }
    memcpy(out, q->frames[q->head].data, q->frames[q->head].len);
    *out_len = q->frames[q->head].len;
    q->head = (q->head + 1) % TX_QUEUE_CAP;
    q->count--;
    return 0;
}

// The frame currently being written to the serial fd, possibly across
// multiple non-blocking write()s / poll() wakeups.
static uint8_t g_inflight_frame[PL_MAX_FRAME_LEN];
static size_t  g_inflight_len;
static size_t  g_inflight_sent;
static int     g_inflight_active;

// File-based outbound bulk transfer (NET_IPC_CTRL_BULK_UPLOAD) in progress,
// if any. Chunks are read and sent one at a time from service_outbound(),
// not all at once, so a large file can't monopolize the event loop - see
// generate_video_chunk(). Only one at a time: a new request simply
// supersedes whatever was in progress, same as the dummy bulk sessions
// Daemon/src/payload_app.c tracks for inbound transfers.
typedef struct {
    int      active;
    FILE    *f;
    uint32_t transfer_id;
    uint32_t total_bytes;
    uint32_t total_chunks;
    uint32_t next_chunk;
} file_bulk_tx_t;

static file_bulk_tx_t g_video_tx;

static void abort_video_tx(void) {
    if (g_video_tx.f != NULL) {
        fclose(g_video_tx.f);
    }
    memset(&g_video_tx, 0, sizeof(g_video_tx));
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

// Encodes `payload` (one APID's User Data Field) as a full payload-link
// frame (body header + zero-padding + sync/length/CRC framing) into
// `frame_out`. Returns the frame length, or 0 on failure (logged here).
static size_t encode_link_frame(uint16_t apid, const uint8_t *payload, uint16_t payload_len, uint8_t *frame_out) {
    if (payload_len > LINK_BODY_MAX_DATA) {
        fprintf(stderr, "[network] payload too large for apid 0x%03X (%u bytes)\n", apid, payload_len);
        return 0;
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

    size_t frame_len = plframe_encode(body, off, frame_out);
    if (frame_len == 0) {
        fprintf(stderr, "[network] failed to frame apid 0x%03X\n", apid);
    }
    return frame_len;
}

static int apid_is_bulk(uint16_t apid) {
    return apid == STAR_APID_PAYLOAD_CALCULATION_DATA || apid == STAR_APID_FORCE_TORQUE_SENSOR_DATA ||
           apid == STAR_APID_IMAGE_VIDEO_DATA;
}

// Encodes and enqueues one frame onto the priority band matching its APID.
// Used for every outbound send except the active file transfer's chunks,
// which are generated directly into the in-flight slot by
// generate_video_chunk() instead of round-tripping through the BULK queue.
static int enqueue_send(uint16_t apid, const uint8_t *payload, uint16_t payload_len) {
    uint8_t frame[PL_MAX_FRAME_LEN];
    size_t  frame_len = encode_link_frame(apid, payload, payload_len, frame);
    if (frame_len == 0) {
        return -1;
    }

    tx_queue_t *q = apid_is_bulk(apid) ? &g_bulk_tx : &g_normal_tx;
    if (tx_queue_push(q, frame, frame_len) != 0) {
        fprintf(stderr, "[network] tx queue full, dropping apid 0x%03X frame\n", apid);
        return -1;
    }
    return 0;
}

// Starts (or begins restarting, from offset 0) an outbound file transfer
// requested via NET_IPC_CTRL_BULK_UPLOAD. "network" owns reading the file
// from here on - the sender only told us where it is.
static void handle_bulk_upload(const uint8_t *data, uint16_t len) {
    if (len < sizeof(net_ipc_bulk_upload_t)) {
        fprintf(stderr, "[network] short bulk upload request (%u bytes)\n", len);
        return;
    }
    net_ipc_bulk_upload_t req;
    memcpy(&req, data, sizeof(req));
    req.file_path[sizeof(req.file_path) - 1] = '\0';

    if (g_video_tx.active) {
        fprintf(stderr, "[network] bulk upload %u superseded by transfer %u\n", g_video_tx.transfer_id,
                req.transfer_id);
        abort_video_tx();
    }

    FILE *f = fopen(req.file_path, "rb");
    if (f == NULL) {
        fprintf(stderr, "[network] bulk upload %u: failed to open %s: %s\n", req.transfer_id, req.file_path,
                strerror(errno));
        return;
    }
    if (fseek(f, 0, SEEK_END) != 0 || ftell(f) < 0) {
        fprintf(stderr, "[network] bulk upload %u: failed to size %s: %s\n", req.transfer_id, req.file_path,
                strerror(errno));
        fclose(f);
        return;
    }
    long size = ftell(f);
    rewind(f);

    uint32_t total_chunks = ((uint32_t)size + MAX_CHUNK_DATA - 1) / MAX_CHUNK_DATA;
    if (total_chunks == 0) {
        total_chunks = 1; // still send one (empty) chunk so the OBC sees the session
    }

    g_video_tx.active       = 1;
    g_video_tx.f             = f;
    g_video_tx.transfer_id   = req.transfer_id;
    g_video_tx.total_bytes   = (uint32_t)size;
    g_video_tx.total_chunks  = total_chunks;
    g_video_tx.next_chunk    = 0;
    fprintf(stderr, "[network] bulk upload %u started: %s (%u bytes, %u chunks)\n", req.transfer_id, req.file_path,
            (uint32_t)size, total_chunks);
}

// Reads and encodes exactly one chunk of the active file transfer into
// `frame_out`, advancing (and, on the last chunk, closing) g_video_tx.
// Returns the encoded frame length, or 0 if nothing was produced (read
// failure, already logged here).
static size_t generate_video_chunk(uint8_t *frame_out) {
    uint32_t chunk     = g_video_tx.next_chunk;
    uint32_t start      = chunk * MAX_CHUNK_DATA;
    uint32_t remaining  = g_video_tx.total_bytes - start;
    uint16_t n          = (uint16_t)(remaining < MAX_CHUNK_DATA ? remaining : MAX_CHUNK_DATA);

    uint8_t chunk_data[MAX_CHUNK_DATA];
    if (n > 0 && fread(chunk_data, 1, n, g_video_tx.f) != n) {
        fprintf(stderr, "[network] bulk upload %u: read failed at chunk %u: %s\n", g_video_tx.transfer_id, chunk,
                strerror(errno));
        abort_video_tx();
        return 0;
    }

    uint8_t wrapper[BULK_WRAPPER_HEADER_SIZE + MAX_CHUNK_DATA];
    size_t  wlen = pack_bulk_wrapper(wrapper, g_video_tx.transfer_id, chunk, g_video_tx.total_chunks, chunk_data, n);

    size_t frame_len = encode_link_frame(STAR_APID_IMAGE_VIDEO_DATA, wrapper, (uint16_t)wlen, frame_out);

    g_video_tx.next_chunk++;
    if (g_video_tx.next_chunk >= g_video_tx.total_chunks) {
        fprintf(stderr, "[network] bulk upload %u complete: %u bytes in %u chunks\n", g_video_tx.transfer_id,
                g_video_tx.total_bytes, g_video_tx.total_chunks);
        abort_video_tx();
    }
    return frame_len;
}

static void start_inflight(const uint8_t *frame, size_t len) {
    memcpy(g_inflight_frame, frame, len);
    g_inflight_len    = len;
    g_inflight_sent    = 0;
    g_inflight_active = 1;
}

// Writes as much of the in-flight frame as the serial fd will currently
// accept. g_serial_fd is non-blocking: a short/zero write just means the
// fd's output buffer is full right now, so the remainder stays queued and
// g_inflight_active stays set for the next poll() wakeup (with POLLOUT
// registered) to continue from - never busy-spins waiting for room.
static void try_write_inflight(void) {
    while (g_inflight_sent < g_inflight_len) {
        ssize_t n = write(g_serial_fd, g_inflight_frame + g_inflight_sent, g_inflight_len - g_inflight_sent);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return; // still active; resume on next POLLOUT
            }
            fprintf(stderr, "[network] serial write failed: %s\n", strerror(errno));
            g_inflight_active = 0; // drop the frame rather than retry a broken fd forever
            return;
        }
        g_inflight_sent += (size_t)n;
    }
    g_inflight_active = 0; // fully sent
}

// Makes one unit of outbound progress: finishes whatever frame is already
// in flight, or starts the next one (NORMAL band, then BULK band, then one
// freshly-read chunk of the active file transfer, in that priority order).
// Called every main-loop iteration regardless of why poll() returned, so a
// long bulk transfer makes steady progress without ever blocking the loop.
static void service_outbound(void) {
    if (g_inflight_active) {
        try_write_inflight();
        return;
    }

    uint8_t frame[PL_MAX_FRAME_LEN];
    size_t  len;

    if (tx_queue_pop(&g_normal_tx, frame, &len) == 0) {
        start_inflight(frame, len);
        try_write_inflight();
        return;
    }
    if (tx_queue_pop(&g_bulk_tx, frame, &len) == 0) {
        start_inflight(frame, len);
        try_write_inflight();
        return;
    }
    if (g_video_tx.active) {
        len = generate_video_chunk(frame);
        if (len > 0) {
            start_inflight(frame, len);
            try_write_inflight();
        }
        return;
    }
}

static int outbound_work_pending(void) {
    return g_inflight_active || g_normal_tx.count > 0 || g_bulk_tx.count > 0 || g_video_tx.active;
}

// Handles one frame delivered by the daemon from a payload application
// process. NET_IPC_CTRL_BULK_UPLOAD starts a file transfer (see above);
// anything else unwraps the net_ipc_hdr_t as-is and enqueues it for sending
// out over the serial link on the APID it names.
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

    if (hdr.apid == NET_IPC_CTRL_BULK_UPLOAD) {
        handle_bulk_upload(data, hdr.len);
        return;
    }

    if (!star_payload_apid_allowed_pay_to_bus(hdr.apid)) {
        fprintf(stderr, "[network] app requested send on unroutable apid 0x%03X\n", hdr.apid);
        return;
    }

    if (enqueue_send(hdr.apid, data, hdr.len) < 0) {
        fprintf(stderr, "[network] failed to enqueue apid 0x%03X\n", hdr.apid);
    }
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

    pollfds[daemon_pfd_idx].fd     = daemon_client_fd(&g_daemon);
    pollfds[daemon_pfd_idx].events = POLLIN;

    // Event Loop
    while (1) {
        pollfds[serial_pfd_idx].fd     = g_serial_fd;
        pollfds[serial_pfd_idx].events = POLLIN | (g_inflight_active ? POLLOUT : 0);

        // Outbound work (queued frames or an active file transfer) can't
        // wait on an external event to make progress, so poll with a
        // zero timeout and keep cycling through service_outbound() until
        // it's all drained; otherwise block indefinitely like before.
        int timeout_ms = outbound_work_pending() ? 0 : -1;
        int ret = poll(pollfds, 2, timeout_ms);

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

        service_outbound();
    }

    return EXIT_SUCCESS;
}
