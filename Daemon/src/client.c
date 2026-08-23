#include "daemon/client.h"
#include "daemon/config.h"
#include "daemon/ipc.h"
#include "daemon/proto.h"

#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

//TODO: ADD PRINTOUTS AND LOGS FOR STUFF ESP STUFF LIKE ERROS
int daemon_client_connect(const char *name, daemon_client_t *out) {
    if (name == NULL || out == NULL) {
        errno = EINVAL;
        return -1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, daemon_socket_path(), sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    daemon_register_body_t body;
    memset(&body, 0, sizeof(body));
    strncpy(body.name, name, sizeof(body.name) - 1);

    if (daemon_send(fd, DMSG_REGISTER, &body, sizeof(body)) != 0) {
        close(fd);
        return -1;
    }

    uint16_t type;
    uint32_t len;
    uint8_t reply[sizeof(daemon_error_body_t)];
    if (daemon_recv(fd, &type, reply, sizeof(reply), &len) != 0) {
        close(fd);
        return -1;
    }

    if (type == DMSG_ERROR) {
        daemon_error_body_t *err = (daemon_error_body_t *)reply;
        close(fd);
        errno = (len >= sizeof(*err) && err->code == DAEMON_ERR_NAME_TAKEN) ? EEXIST : EPROTO;
        return -1;
    }
    if (type != DMSG_REGISTER_OK) {
        close(fd);
        errno = EPROTO;
        return -1;
    }

    out->fd = fd;
    return 0;
}

int daemon_client_fd(const daemon_client_t *c) {
    return c->fd;
}

int daemon_client_get_state(daemon_client_t *c, satellite_state_t *out) {
    if (daemon_send(c->fd, DMSG_STATE_GET, NULL, 0) != 0) {
        return -1;
    }

    uint16_t type;
    uint32_t len;
    if (daemon_recv(c->fd, &type, out, sizeof(*out), &len) != 0) {
        return -1;
    }
    if (type != DMSG_STATE_VALUE || len != sizeof(*out)) {
        errno = EPROTO;
        return -1;
    }
    return 0;
}

int daemon_client_set_state(daemon_client_t *c, const satellite_state_t *state) {
    if (daemon_send(c->fd, DMSG_STATE_SET, state, sizeof(*state)) != 0) {
        return -1;
    }

    satellite_state_t confirmed;
    uint16_t type;
    uint32_t len;
    if (daemon_recv(c->fd, &type, &confirmed, sizeof(confirmed), &len) != 0) {
        return -1;
    }
    if (type != DMSG_STATE_VALUE || len != sizeof(confirmed)) {
        errno = EPROTO;
        return -1;
    }
    return 0;
}

int daemon_client_send(daemon_client_t *c, const char *dest, const void *data, uint32_t len) {
    uint8_t buf[sizeof(daemon_send_body_t) + DAEMON_MAX_MSG_PAYLOAD];
    if (len > sizeof(buf) - sizeof(daemon_send_body_t)) {
        errno = EMSGSIZE;
        return -1;
    }

    daemon_send_body_t *body = (daemon_send_body_t *)buf;
    memset(body->dest, 0, sizeof(body->dest));
    strncpy(body->dest, dest, sizeof(body->dest) - 1);
    if (len > 0) {
        memcpy(buf + sizeof(*body), data, len);
    }

    return daemon_send(c->fd, DMSG_SEND, buf, (uint32_t)(sizeof(*body) + len));
}

int daemon_client_recv(daemon_client_t *c, uint16_t *type, char *src_out, void *buf, uint32_t bufsize,
                        uint32_t *out_len) {
    uint8_t frame[sizeof(daemon_deliver_body_t) + DAEMON_MAX_MSG_PAYLOAD];
    uint32_t frame_len;
    if (daemon_recv(c->fd, type, frame, sizeof(frame), &frame_len) != 0) {
        return -1;
    }

    if (*type == DMSG_DELIVER) {
        if (frame_len < sizeof(daemon_deliver_body_t)) {
            errno = EPROTO;
            return -1;
        }
        daemon_deliver_body_t *body = (daemon_deliver_body_t *)frame;
        if (src_out != NULL) {
            memcpy(src_out, body->src, DAEMON_NAME_MAX);
        }
        uint32_t payload_len = frame_len - (uint32_t)sizeof(*body);
        if (payload_len > bufsize) {
            return -2;
        }
        if (payload_len > 0) {
            memcpy(buf, frame + sizeof(*body), payload_len);
        }
        *out_len = payload_len;
        return 0;
    }

    if (src_out != NULL) {
        memset(src_out, 0, DAEMON_NAME_MAX);
    }
    if (frame_len > bufsize) {
        return -2;
    }
    if (frame_len > 0) {
        memcpy(buf, frame, frame_len);
    }
    *out_len = frame_len;
    return 0;
}

void daemon_client_close(daemon_client_t *c) {
    if (c->fd >= 0) {
        close(c->fd);
        c->fd = -1;
    }
}
