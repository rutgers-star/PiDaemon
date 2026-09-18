#define _DEFAULT_SOURCE

#include "daemon/config.h"
#include "daemon/ipc_server.h"
#include "daemon/process.h"
#include "daemon/state.h"
#include "daemon/util.h"

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

// Written to by the signal handler, read by the main loop so all signal
// handling happens outside async-signal-unsafe context.
static int g_sig_pipe[2] = { -1, -1 };
static volatile sig_atomic_t g_got_term  = 0;
static volatile sig_atomic_t g_got_chld  = 0;

static void on_signal(int sig) {
    if (sig == SIGCHLD) {
        g_got_chld = 1;
    } else {
        g_got_term = 1;
    }
    char c = 0;
    ssize_t ignored = write(g_sig_pipe[1], &c, 1);
    (void)ignored;
}

static int install_signal_handlers(void) {
    if (pipe(g_sig_pipe) != 0) {
        return -1;
    }
    for (int i = 0; i < 2; i++) {
        int flags = fcntl(g_sig_pipe[i], F_GETFL, 0);
        fcntl(g_sig_pipe[i], F_SETFL, flags | O_NONBLOCK);
        // Must not leak into forked managed processes, or they inherit a
        // handle into the daemon's own signal-wakeup pipe.
        fcntl(g_sig_pipe[i], F_SETFD, FD_CLOEXEC);
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    if (sigaction(SIGTERM, &sa, NULL) != 0) return -1;
    if (sigaction(SIGINT, &sa, NULL) != 0) return -1;
    if (sigaction(SIGCHLD, &sa, NULL) != 0) return -1;
    signal(SIGPIPE, SIG_IGN);

    return 0;
}

// Holds an exclusive, non-blocking flock() for the daemon's whole lifetime
// so a second instance can't start against the same state/socket paths.
// Returns the held fd on success. On failure returns -1 with errno set to
// the real cause (e.g. EACCES creating the pid directory/file - a setup
// problem, not a running instance), or -2 specifically when the lock is
// held by another live process, so the caller can report the two cases
// differently instead of always guessing "already running".
static int acquire_singleton_lock(const char *pid_path) {
    char dir_buf[PATH_MAX];
    strncpy(dir_buf, pid_path, sizeof(dir_buf) - 1);
    dir_buf[sizeof(dir_buf) - 1] = '\0';
    if (daemon_ensure_dir(dirname(dir_buf)) != 0) {
        return -1;
    }

    // O_CLOEXEC: a managed process forked+exec'd after this point must not
    // inherit a handle to this lock. flock() is held by the open file
    // description, so a leaked copy of this fd would keep the lock held
    // (and a future daemond start refused) even after the daemon itself
    // has exited.
    int fd = open(pid_path, O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (fd < 0) {
        return -1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return (saved_errno == EWOULDBLOCK) ? -2 : -1;
    }
    if (ftruncate(fd, 0) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%d\n", (int)getpid());
    ssize_t ignored = write(fd, buf, (size_t)len);
    (void)ignored;

    return fd;
}

// Resolves the directory this binary was launched from, so managed
// processes that ship alongside daemond (like payload_app) can be found
// without a hardcoded install path.
static int self_exe_dir(char *buf, size_t bufsize) {
    char exe[PATH_MAX];
    ssize_t n = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n < 0) {
        return -1;
    }
    exe[n] = '\0';
    char *dir = dirname(exe);
    if (strlen(dir) >= bufsize) {
        errno = ENAMETOOLONG;
        return -1;
    }
    strcpy(buf, dir);
    return 0;
}

// The dummy payload app has no hardware dependency, so the daemon can
// safely supervise it like any other worker.
static void register_payload_app(process_manager_t *pm, const char *bin_dir) {
    static process_spec_t payload_app;

    memset(&payload_app, 0, sizeof(payload_app));
    snprintf(payload_app.name, sizeof(payload_app.name), "payload_app");
    snprintf(payload_app.path, sizeof(payload_app.path), "%s/payload_app", bin_dir);
    payload_app.argv[1] = "payload_app";
    payload_app.argv[2] = NULL;
    payload_app.restart = RESTART_ALWAYS;
    pm_add(pm, &payload_app);
}

#ifndef NETWORK_BIN_PATH
#define NETWORK_BIN_PATH NULL
#endif

// The real Network bridge (Communications, Network/src/main.c) - owns the
// RS-422/SPICEnet link to the OBC. Supervised the same as any other
// process: if it exits (e.g. no serial hardware attached, or the link
// dropped), pm_reap's restart backoff brings it back, matching the ICD's
// expectation that the link is re-established automatically (Section 4.2).
// NETWORK_BIN_PATH is only defined when built via the top-level
// CMakeLists.txt that builds Network alongside Daemon - see there.
static void register_network(process_manager_t *pm) {
    if (NETWORK_BIN_PATH == NULL) {
        fprintf(stderr, "[Daemon] NETWORK_BIN_PATH not set (standalone Daemon build?), not supervising "
                        "Communications\n");
        return;
    }

    static process_spec_t network;
    memset(&network, 0, sizeof(network));
    snprintf(network.name, sizeof(network.name), "network");
    snprintf(network.path, sizeof(network.path), "%s", NETWORK_BIN_PATH);
    network.argv[1] = NULL; // Communications defaults to /dev/ttyS0 when no port is given
    network.restart = RESTART_ALWAYS;
    pm_add(pm, &network);
}

int main(void) {
    if (install_signal_handlers() != 0) {
        perror("[Daemon] failed to install signal handlers");
        return 1;
    }

    int pid_fd = acquire_singleton_lock(daemon_pid_path());
    if (pid_fd == -2) {
        fprintf(stderr, "[Daemon] another instance is already running (%s locked)\n", daemon_pid_path());
        return 1;
    }
    if (pid_fd < 0) {
        fprintf(stderr, "[Daemon] failed to acquire singleton lock at %s: %s\n", daemon_pid_path(),
                strerror(errno));
        return 1;
    }

    satellite_state_t state;
    if (sat_state_init(daemon_state_path(), &state) != 0) {
        perror("[Daemon] failed to open state store");
        return 1;
    }
    fprintf(stderr, "[Daemon] state restored: op_state=0x%02X active_experiment_id=%u dce_authority=%u\n",
            state.op_state, state.active_experiment_id, state.dce_authority_held);

    process_manager_t pm;
    pm_init(&pm);

    char bin_dir[PATH_MAX];
    if (self_exe_dir(bin_dir, sizeof(bin_dir)) != 0) {
        perror("[Daemon] failed to resolve own executable path");
        return 1;
    }
    register_payload_app(&pm, bin_dir);
    register_network(&pm);
    pm_start_all(&pm);

    ipc_server_t *srv = ipc_server_create(daemon_socket_path());
    if (srv == NULL) {
        perror("[Daemon] failed to create IPC socket");
        pm_shutdown_all(&pm, DAEMON_SHUTDOWN_GRACE_MS);
        return 1;
    }
    fprintf(stderr, "[Daemon] listening on %s\n", daemon_socket_path());

    fprintf(stderr, "[Daemon] ready\n");

    while (!g_got_term) {
        struct pollfd fds[DAEMON_MAX_CLIENTS + 2];
        int n = ipc_server_collect_pollfds(srv, fds, DAEMON_MAX_CLIENTS + 1);

        int sig_idx = n;
        fds[sig_idx].fd     = g_sig_pipe[0];
        fds[sig_idx].events = POLLIN;
        fds[sig_idx].revents = 0;

        int ret = poll(fds, (nfds_t)(n + 1), DAEMON_TICK_MS);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("[Daemon] poll failed");
            break;
        }

        if (fds[sig_idx].revents & POLLIN) {
            char drain[64];
            while (read(g_sig_pipe[0], drain, sizeof(drain)) > 0) {
                // drain the self-pipe
            }
        }

        if (g_got_chld) {
            g_got_chld = 0;
            pm_reap(&pm);
        }

        ipc_server_service(srv, fds, n, &state);
        pm_tick(&pm);
    }

    fprintf(stderr, "[Daemon] shutting down\n");
    ipc_server_destroy(srv);
    pm_shutdown_all(&pm, DAEMON_SHUTDOWN_GRACE_MS);
    sat_state_close();
    close(pid_fd);
    unlink(daemon_pid_path());

    return 0;
}
