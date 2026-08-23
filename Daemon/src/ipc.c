#include "daemon/ipc.h"
#include "daemon/proto.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>

// TODO: MAKE ALL OF THIS MULTITHREADED SO IT DOENS'T BLOCK FOR LARGE FILES
int daemon_write_all(int fd, const void *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, (const uint8_t *)buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            errno = EPIPE;
            return -2;
        }
        off += (size_t)n;
    }
    return 0;
}

int daemon_read_all(int fd, void *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, (uint8_t *)buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            return -2;
        }
        off += (size_t)n;
    }
    return 0;
}

int daemon_send(int fd, uint16_t type, const void *payload, uint32_t len) {
    daemon_hdr_t hdr = { .length = len, .type = type };

    if (daemon_write_all(fd, &hdr, sizeof(hdr)) != 0) {
        return -1;
    }
    if (len > 0 && daemon_write_all(fd, payload, len) != 0) {
        return -1;
    }
    return 0;
}

int daemon_recv(int fd, uint16_t *type, void *buf, uint32_t bufsize, uint32_t *out_len) {
    daemon_hdr_t hdr;
    int ret = daemon_read_all(fd, &hdr, sizeof(hdr));
    if (ret != 0) {
        return -1;
    }

    if (hdr.length > bufsize) {
        // Drain and discard so a caller that chooses to keep the
        // connection open anyway isn't left with a desynchronized stream.
        uint8_t scratch[256];
        uint32_t remaining = hdr.length;
        while (remaining > 0) {
            uint32_t chunk = remaining < sizeof(scratch) ? remaining : sizeof(scratch);
            if (daemon_read_all(fd, scratch, chunk) != 0) {
                break;
            }
            remaining -= chunk;
        }
        return -2;
    }

    if (hdr.length > 0 && daemon_read_all(fd, buf, hdr.length) != 0) {
        return -1;
    }

    *type = hdr.type;
    *out_len = hdr.length;
    return 0;
}
