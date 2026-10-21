// Test utility: connects to daemond like any other payload process and
// tells "network" (Network/src/main.c) to send a real file to the OBC as a
// bulk Image/Video Data Transfer (APID 0x032, ICD Section 5.8.3) over the
// real RS-422 link.
//
// This process does no file reading or ICD framing itself - it only sends
// "network" the file's path via the NET_IPC_CTRL_BULK_UPLOAD control
// message (see net_ipc.h). "network" opens the file, chunks it, and paces
// sending it out over the serial link itself, interleaved with its other
// outbound traffic - see the tx priority queues in Network/src/main.c.
// Sender and "network" must share a filesystem namespace, which holds here
// since both run on the same Pi.
//
// Fire-and-forget by design: "network" forwards everything it receives from
// the OBC (including the Bulk Data Response ACK/NACK on APID 0x035) only to
// the process registered as NET_IPC_PAYLOAD_APP_NAME ("payload_app") - see
// forward_to_app() in Network/src/main.c. A differently-named process here
// would never see the response, so this doesn't wait for one.
//
// usage: video_uploader <file> [registration_name]

#include "daemon/client.h"
#include "net_ipc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

static void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <video-file> [registration-name]\n", argv[0]);
        return 1;
    }
    const char *file_path = argv[1];
    const char *name      = argc > 2 ? argv[2] : "video_uploader";

    if (strlen(file_path) >= NET_IPC_BULK_UPLOAD_PATH_MAX) {
        fprintf(stderr, "[%s] path too long for the upload request (max %d chars): %s\n", name,
                NET_IPC_BULK_UPLOAD_PATH_MAX - 1, file_path);
        return 1;
    }

    struct stat st;
    if (stat(file_path, &st) != 0) {
        fprintf(stderr, "[%s] failed to stat %s: %s\n", name, file_path, strerror(errno));
        return 1;
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
        return 1;
    }
    fprintf(stderr, "[%s] connected and registered\n", name);

    uint32_t              transfer_id = (uint32_t)time(NULL);
    net_ipc_bulk_upload_t req         = { .transfer_id = transfer_id };
    strncpy(req.file_path, file_path, sizeof(req.file_path) - 1);

    uint8_t       frame[sizeof(net_ipc_hdr_t) + sizeof(req)];
    net_ipc_hdr_t hdr = { .apid = NET_IPC_CTRL_BULK_UPLOAD, .len = sizeof(req) };
    memcpy(frame, &hdr, sizeof(hdr));
    memcpy(frame + sizeof(hdr), &req, sizeof(req));

    int ok = daemon_client_send(&c, NET_IPC_NETWORK_NAME, frame, sizeof(frame)) == 0;
    if (!ok) {
        fprintf(stderr, "[%s] failed to send upload request: %s\n", name, strerror(errno));
    } else {
        fprintf(stderr, "[%s] -> bulk video upload id=%u file=%s size=%lld bytes requested from \"%s\"\n", name,
                transfer_id, file_path, (long long)st.st_size, NET_IPC_NETWORK_NAME);
    }

    daemon_client_close(&c);
    return ok ? 0 : 1;
}
