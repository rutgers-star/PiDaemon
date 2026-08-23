#define _GNU_SOURCE

#include "daemon/ipc_server.h"
#include "daemon/config.h"
#include "daemon/proto.h"
#include "daemon/util.h"

#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

typedef struct {
    int  fd;
    int  in_use;
    int  registered;
    int  should_close;
    char name[DAEMON_NAME_MAX]; // always NUL-terminated once registered

    uint8_t  rxbuf[sizeof(daemon_hdr_t) + DAEMON_MAX_MSG_PAYLOAD];
    uint32_t rx_len;

    uint8_t *txbuf;
    size_t   tx_len;
    size_t   tx_sent;
    size_t   tx_cap;
} client_conn_t;

struct ipc_server {
    int  listen_fd;
    char socket_path[256];
    client_conn_t clients[DAEMON_MAX_CLIENTS];

    // Set by ipc_server_collect_pollfds() and consumed by
    // ipc_server_service(): slot_map[k] is the clients[] index that fds[k]
    // corresponds to (-1 for the listener at fds[0]). Needed because
    // accepting new connections during service() can change which array
    // slots are in_use, so we can't just re-derive the mapping by
    // rescanning in_use flags at service time.
    int slot_map[DAEMON_MAX_CLIENTS + 1];
};

static const size_t TX_BACKLOG_LIMIT = 256 * 1024;
static const size_t TX_MIN_CAP = 4096;

ipc_server_t *ipc_server_create(const char *socket_path) {
    char dir_buf[PATH_MAX];
    strncpy(dir_buf, socket_path, sizeof(dir_buf) - 1);
    dir_buf[sizeof(dir_buf) - 1] = '\0';
    if (daemon_ensure_dir(dirname(dir_buf)) != 0) {
        return NULL;
    }

    if (strlen(socket_path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
        errno = ENAMETOOLONG;
        return NULL;
    }

    // SOCK_CLOEXEC: forked managed processes must not inherit the listening
    // socket.
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return NULL;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, socket_path);

    // Safe to remove a stale socket file here: the caller is required to
    // have already taken the daemon's singleton pidfile lock, so nothing
    // else can legitimately be listening on this path.
    unlink(socket_path);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return NULL;
    }
    if (listen(fd, DAEMON_MAX_CLIENTS) != 0) {
        close(fd);
        unlink(socket_path);
        return NULL;
    }

    ipc_server_t *srv = calloc(1, sizeof(*srv));
    if (srv == NULL) {
        close(fd);
        unlink(socket_path);
        return NULL;
    }
    srv->listen_fd = fd;
    strncpy(srv->socket_path, socket_path, sizeof(srv->socket_path) - 1);
    for (int i = 0; i < DAEMON_MAX_CLIENTS; i++) {
        srv->clients[i].fd = -1;
    }

    return srv;
}

void ipc_server_destroy(ipc_server_t *srv) {
    if (srv == NULL) {
        return;
    }
    for (int i = 0; i < DAEMON_MAX_CLIENTS; i++) {
        if (srv->clients[i].in_use) {
            close(srv->clients[i].fd);
            free(srv->clients[i].txbuf);
        }
    }
    close(srv->listen_fd);
    unlink(srv->socket_path);
    free(srv);
}

int ipc_server_collect_pollfds(ipc_server_t *srv, struct pollfd *fds, int max_fds) {
    int n = 0;

    if (n < max_fds) {
        fds[n].fd      = srv->listen_fd;
        fds[n].events  = POLLIN;
        fds[n].revents = 0;
        srv->slot_map[n] = -1;
        n++;
    }

    for (int i = 0; i < DAEMON_MAX_CLIENTS && n < max_fds; i++) {
        client_conn_t *c = &srv->clients[i];
        if (!c->in_use) {
            continue;
        }
        fds[n].fd     = c->fd;
        fds[n].events = POLLIN;
        if (c->tx_sent < c->tx_len) {
            fds[n].events |= POLLOUT;
        }
        fds[n].revents = 0;
        srv->slot_map[n] = i;
        n++;
    }

    return n;
}

static void close_client(client_conn_t *c) {
    close(c->fd);
    free(c->txbuf);
    memset(c, 0, sizeof(*c));
    c->fd = -1;
}

static client_conn_t *find_client_by_name(ipc_server_t *srv, const char *name, const client_conn_t *exclude) {
    for (int i = 0; i < DAEMON_MAX_CLIENTS; i++) {
        client_conn_t *c = &srv->clients[i];
        if (c->in_use && c->registered && c != exclude && strncmp(c->name, name, DAEMON_NAME_MAX) == 0) {
            return c;
        }
    }
    return NULL;
}

static int append_tx(client_conn_t *c, const void *data, size_t len) {
    if (c->tx_sent > 0) {
        size_t remaining = c->tx_len - c->tx_sent;
        if (remaining > 0) {
            memmove(c->txbuf, c->txbuf + c->tx_sent, remaining);
        }
        c->tx_len  = remaining;
        c->tx_sent = 0;
    }

    size_t needed = c->tx_len + len;
    if (needed > TX_BACKLOG_LIMIT) {
        return -1;
    }
    if (needed > c->tx_cap) {
        size_t newcap = c->tx_cap ? c->tx_cap * 2 : TX_MIN_CAP;
        while (newcap < needed) {
            newcap *= 2;
        }
        uint8_t *nb = realloc(c->txbuf, newcap);
        if (nb == NULL) {
            return -1;
        }
        c->txbuf = nb;
        c->tx_cap = newcap;
    } else if (c->tx_cap > TX_MIN_CAP && needed < c->tx_cap / 4) {
        size_t newcap = TX_MIN_CAP;
        while (newcap < needed) {
            newcap *= 2;
        }
        if (newcap < c->tx_cap) {
            uint8_t *nb = realloc(c->txbuf, newcap);
            if (nb != NULL) { // shrink failure is non-fatal; keep the larger buffer
                c->txbuf = nb;
                c->tx_cap = newcap;
            }
        }
    }

    memcpy(c->txbuf + c->tx_len, data, len);
    c->tx_len += len;
    return 0;
}

static void flush_tx(client_conn_t *c) {
    while (c->tx_sent < c->tx_len) {
        ssize_t n = send(c->fd, c->txbuf + c->tx_sent, c->tx_len - c->tx_sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            c->should_close = 1;
            return;
        }
        c->tx_sent += (size_t)n;
    }
    c->tx_len  = 0;
    c->tx_sent = 0;
}

static void queue_send(client_conn_t *c, uint16_t type, const void *payload, uint32_t len) {
    uint8_t frame[sizeof(daemon_hdr_t) + DAEMON_MAX_MSG_PAYLOAD];
    daemon_hdr_t hdr = { .length = len, .type = type };
    memcpy(frame, &hdr, sizeof(hdr));
    if (len > 0) {
        memcpy(frame + sizeof(hdr), payload, len);
    }
    size_t framelen = sizeof(hdr) + len;

    if (c->tx_len == c->tx_sent) {
        ssize_t n = send(c->fd, frame, framelen, MSG_NOSIGNAL);
        if (n == (ssize_t)framelen) {
            return;
        }
        if (n < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                c->should_close = 1;
                return;
            }
            n = 0;
        }
        if (append_tx(c, frame + n, framelen - (size_t)n) != 0) {
            c->should_close = 1;
        }
        return;
    }

    if (append_tx(c, frame, framelen) != 0) {
        c->should_close = 1;
    }
}

static void handle_message(ipc_server_t *srv, satellite_state_t *state, client_conn_t *c, uint16_t type,
                            const uint8_t *payload, uint32_t len) {
    if (!c->registered) {
        if (type != DMSG_REGISTER || len < sizeof(daemon_register_body_t)) {
            daemon_error_body_t err = { .code = type != DMSG_REGISTER ? DAEMON_ERR_NOT_REGISTERED
                                                                       : DAEMON_ERR_BAD_MESSAGE };
            queue_send(c, DMSG_ERROR, &err, sizeof(err));
            c->should_close = 1;
            return;
        }

        daemon_register_body_t body;
        memcpy(&body, payload, sizeof(body));
        char name[DAEMON_NAME_MAX];
        memcpy(name, body.name, DAEMON_NAME_MAX);
        name[DAEMON_NAME_MAX - 1] = '\0';

        if (name[0] == '\0') {
            daemon_error_body_t err = { .code = DAEMON_ERR_BAD_MESSAGE };
            queue_send(c, DMSG_ERROR, &err, sizeof(err));
            c->should_close = 1;
            return;
        }
        if (find_client_by_name(srv, name, c) != NULL) {
            daemon_error_body_t err = { .code = DAEMON_ERR_NAME_TAKEN };
            queue_send(c, DMSG_ERROR, &err, sizeof(err));
            c->should_close = 1;
            return;
        }

        strncpy(c->name, name, DAEMON_NAME_MAX - 1);
        c->registered = 1;
        queue_send(c, DMSG_REGISTER_OK, NULL, 0);
        fprintf(stderr, "[IPC] '%s' registered (fd=%d)\n", c->name, c->fd);
        return;
    }

    switch (type) {
        case DMSG_STATE_GET:
            queue_send(c, DMSG_STATE_VALUE, state, sizeof(*state));
            break;

        case DMSG_STATE_SET:
            if (len != sizeof(satellite_state_t)) {
                daemon_error_body_t err = { .code = DAEMON_ERR_BAD_MESSAGE };
                queue_send(c, DMSG_ERROR, &err, sizeof(err));
                break;
            }
            memcpy(state, payload, sizeof(*state));
            if (sat_state_store(state) != 0) {
                fprintf(stderr, "[State] failed to persist update from '%s': %s\n", c->name, strerror(errno));
            }
            queue_send(c, DMSG_STATE_VALUE, state, sizeof(*state));
            break;

        case DMSG_SEND: {
            if (len < sizeof(daemon_send_body_t)) {
                daemon_error_body_t err = { .code = DAEMON_ERR_BAD_MESSAGE };
                queue_send(c, DMSG_ERROR, &err, sizeof(err));
                break;
            }
            daemon_send_body_t body;
            memcpy(&body, payload, sizeof(body));
            char dest[DAEMON_NAME_MAX];
            memcpy(dest, body.dest, DAEMON_NAME_MAX);
            dest[DAEMON_NAME_MAX - 1] = '\0';

            client_conn_t *target = find_client_by_name(srv, dest, NULL);
            if (target == NULL) {
                daemon_error_body_t err = { .code = DAEMON_ERR_UNKNOWN_DEST };
                queue_send(c, DMSG_ERROR, &err, sizeof(err));
                break;
            }

            uint8_t out[sizeof(daemon_deliver_body_t) + DAEMON_MAX_MSG_PAYLOAD];
            daemon_deliver_body_t dbody;
            memset(&dbody, 0, sizeof(dbody));
            memcpy(dbody.src, c->name, DAEMON_NAME_MAX);
            memcpy(out, &dbody, sizeof(dbody));
            uint32_t app_len = len - (uint32_t)sizeof(body);
            memcpy(out + sizeof(dbody), payload + sizeof(body), app_len);

            queue_send(target, DMSG_DELIVER, out, (uint32_t)sizeof(dbody) + app_len);
            break;
        }

        case DMSG_PING:
            queue_send(c, DMSG_PONG, NULL, 0);
            break;

        default: {
            daemon_error_body_t err = { .code = DAEMON_ERR_BAD_MESSAGE };
            queue_send(c, DMSG_ERROR, &err, sizeof(err));
            break;
        }
    }
}

static void accept_new_connections(ipc_server_t *srv) {
    for (;;) {
        // SOCK_CLOEXEC here too: a client connection accepted before a
        // pm_tick() restart-spawn must not leak into the new child.
        int cfd = accept4(srv->listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            if (errno == EINTR) {
                continue;
            }
            break; // EAGAIN/EWOULDBLOCK: no more pending connections
        }

        client_conn_t *slot = NULL;
        for (int i = 0; i < DAEMON_MAX_CLIENTS; i++) {
            if (!srv->clients[i].in_use) {
                slot = &srv->clients[i];
                break;
            }
        }
        if (slot == NULL) {
            fprintf(stderr, "[IPC] rejecting connection: max clients reached\n");
            close(cfd);
            continue;
        }

        memset(slot, 0, sizeof(*slot));
        slot->fd     = cfd;
        slot->in_use = 1;
        fprintf(stderr, "[IPC] accepted connection (fd=%d)\n", cfd);
    }
}

void ipc_server_service(ipc_server_t *srv, struct pollfd *fds, int n, satellite_state_t *state) {
    if (n > 0 && (fds[0].revents & (POLLIN | POLLERR))) {
        accept_new_connections(srv);
    }

    for (int k = 1; k < n; k++) {
        int slot = srv->slot_map[k];
        if (slot < 0) {
            continue;
        }
        client_conn_t *c = &srv->clients[slot];
        if (!c->in_use || c->fd != fds[k].fd) {
            continue; // slot was recycled since collect_pollfds(); handle next round
        }

        short rev = fds[k].revents;
        if (rev & (POLLHUP | POLLERR | POLLNVAL)) {
            c->should_close = 1;
        }

        if (!c->should_close && (rev & POLLIN)) {
            for (;;) {
                ssize_t nrecv = recv(c->fd, c->rxbuf + c->rx_len, sizeof(c->rxbuf) - c->rx_len, 0);
                if (nrecv < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    if (errno == EINTR) {
                        continue;
                    }
                    c->should_close = 1;
                    break;
                }
                if (nrecv == 0) {
                    c->should_close = 1;
                    break;
                }
                c->rx_len += (uint32_t)nrecv;
                if (c->rx_len >= sizeof(c->rxbuf)) {
                    break;
                }
            }

            while (!c->should_close && c->rx_len >= sizeof(daemon_hdr_t)) {
                daemon_hdr_t hdr;
                memcpy(&hdr, c->rxbuf, sizeof(hdr));
                if (hdr.length > DAEMON_MAX_MSG_PAYLOAD) {
                    fprintf(stderr, "[IPC] oversized frame from '%s' (fd=%d), closing\n", c->name, c->fd);
                    c->should_close = 1;
                    break;
                }
                uint32_t framelen = (uint32_t)sizeof(hdr) + hdr.length;
                if (c->rx_len < framelen) {
                    break; // wait for the rest
                }

                handle_message(srv, state, c, hdr.type, c->rxbuf + sizeof(hdr), hdr.length);

                memmove(c->rxbuf, c->rxbuf + framelen, c->rx_len - framelen);
                c->rx_len -= framelen;
            }
        }

        if (!c->should_close) {
            flush_tx(c);
        }
        if (c->should_close) {
            fprintf(stderr, "[IPC] closing connection '%s' (fd=%d)\n", c->name[0] ? c->name : "?", c->fd);
            close_client(c);
        }
    }
}
