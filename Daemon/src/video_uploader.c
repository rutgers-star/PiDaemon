// Test utility: connects to daemond like any other payload process and
// uploads a real file to the OBC as a bulk Image/Video Data Transfer (APID
// 0x032, ICD Section 5.8.3), through the real "network" bridge
// (Network/src/main.c) and out over the actual RS-422 link - unlike
// payload_app's send_bulk_video_dummy(), which just sends a small hardcoded
// buffer.
//
// It only ever talks to "network" via daemond, using the same net_ipc_hdr_t
// + Bulk Data Transfer Wrapper framing as payload_app.c's send_bulk().
//
// Fire-and-forget by design: "network" forwards everything it receives from
// the OBC (including the Bulk Data Response ACK/NACK on APID 0x035) only to
// the process registered as NET_IPC_PAYLOAD_APP_NAME ("payload_app") - see
// forward_to_app() in Network/src/main.c. A differently-named process here
// would never see the response, so this doesn't wait for one; it just gets
// the real bytes onto the wire through the real daemon-managed stack.
// Routing responses back to whichever app actually owns a transfer would be
// a separate change to Network's dispatch, not something this test probe
// should take on.
//
// usage: video_uploader <file> [registration_name]

#include "daemon/client.h"
#include "net_ipc.h"

#include <endian.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define STAR_APID_IMAGE_VIDEO_DATA UINT16_C(0x032)

// Bulk Data Transfer Wrapper layout, ICD Section 5.8.
#define BULK_WRAPPER_HEADER_SIZE 14
#define MAX_CHUNK_DATA (NET_IPC_MAX_DATA - BULK_WRAPPER_HEADER_SIZE)

static void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
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

// Packs the Bulk Data Transfer Wrapper header + data, ICD Section 5.8.
// Mirrors pack_bulk_wrapper() in Daemon/src/payload_app.c.
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

// Wraps `wrapper` in net_ipc_hdr_t and hands it to the daemon to relay to
// the "network" process, same convention as send_to_network() in
// payload_app.c.
static int send_chunk(daemon_client_t *c, const uint8_t *wrapper, uint16_t wrapper_len) {
    uint8_t frame[sizeof(net_ipc_hdr_t) + NET_IPC_MAX_DATA];
    net_ipc_hdr_t hdr = { .apid = STAR_APID_IMAGE_VIDEO_DATA, .len = wrapper_len };
    memcpy(frame, &hdr, sizeof(hdr));
    memcpy(frame + sizeof(hdr), wrapper, wrapper_len);
    return daemon_client_send(c, NET_IPC_NETWORK_NAME, frame, (uint32_t)(sizeof(hdr) + wrapper_len));
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <video-file> [registration-name]\n", argv[0]);
        return 1;
    }
    const char *file_path = argv[1];
    const char *name      = argc > 2 ? argv[2] : "video_uploader";

    FILE *f = fopen(file_path, "rb");
    if (f == NULL) {
        fprintf(stderr, "[%s] failed to open %s: %s\n", name, file_path, strerror(errno));
        return 1;
    }
    if (fseek(f, 0, SEEK_END) != 0 || ftell(f) < 0) {
        fprintf(stderr, "[%s] failed to size %s: %s\n", name, file_path, strerror(errno));
        fclose(f);
        return 1;
    }
    long file_size = ftell(f);
    if ((unsigned long)file_size > UINT32_MAX) {
        fprintf(stderr, "[%s] %s is too large (%ld bytes)\n", name, file_path, file_size);
        fclose(f);
        return 1;
    }
    rewind(f);

    uint32_t total_len    = (uint32_t)file_size;
    uint32_t total_chunks = (total_len + MAX_CHUNK_DATA - 1) / MAX_CHUNK_DATA;
    if (total_chunks == 0) {
        total_chunks = 1; // still send one (empty) chunk so the OBC sees the session
    }

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
        fclose(f);
        return 1;
    }
    fprintf(stderr, "[%s] connected and registered\n", name);

    uint32_t transfer_id = (uint32_t)time(NULL);
    fprintf(stderr, "[%s] -> bulk video data transfer id=%u file=%s size=%u bytes chunks=%u (max %u bytes/chunk)\n",
            name, transfer_id, file_path, total_len, total_chunks, (unsigned)MAX_CHUNK_DATA);

    uint8_t  chunk_data[MAX_CHUNK_DATA];
    uint8_t  wrapper[BULK_WRAPPER_HEADER_SIZE + MAX_CHUNK_DATA];
    uint32_t sent_bytes = 0;
    int      ok         = 1;

    for (uint32_t chunk = 0; chunk < total_chunks; chunk++) {
        uint32_t remaining = total_len - sent_bytes;
        uint16_t n         = (uint16_t)(remaining < MAX_CHUNK_DATA ? remaining : MAX_CHUNK_DATA);

        if (n > 0 && fread(chunk_data, 1, n, f) != n) {
            fprintf(stderr, "\n[%s] short read on %s at chunk %u: %s\n", name, file_path, chunk, strerror(errno));
            ok = 0;
            break;
        }

        size_t wlen = pack_bulk_wrapper(wrapper, transfer_id, chunk, total_chunks, chunk_data, n);
        if (send_chunk(&c, wrapper, (uint16_t)wlen) != 0) {
            fprintf(stderr, "\n[%s] failed to send chunk %u: %s\n", name, chunk, strerror(errno));
            ok = 0;
            break;
        }
        sent_bytes += n;
        fprintf(stderr, "\r[%s] sent chunk %u/%u (%u/%u bytes)", name, chunk + 1, total_chunks, sent_bytes,
                total_len);
    }
    fprintf(stderr, "\n");

    fclose(f);
    daemon_client_close(&c);

    if (!ok) {
        return 1;
    }
    fprintf(stderr, "[%s] transfer %u complete: %u bytes in %u chunks sent to \"%s\"\n", name, transfer_id,
            total_len, total_chunks, NET_IPC_NETWORK_NAME);
    return 0;
}
