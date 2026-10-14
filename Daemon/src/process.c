#include "daemon/process.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

void pm_init(process_manager_t *pm) {
    memset(pm, 0, sizeof(*pm));
}

int pm_add(process_manager_t *pm, const process_spec_t *spec) {
    if (pm->count >= DAEMON_MAX_PROCESSES) {
        return -1;
    }

    managed_process_t *mp = &pm->procs[pm->count];
    memset(mp, 0, sizeof(*mp));
    mp->spec = *spec;
    // Always point argv[0] at this slot's own copy of the path, never the
    // caller's original struct (which may not outlive this call).
    mp->spec.argv[0] = mp->spec.path;
    mp->pid = 0;

    pm->count++;
    return 0;
}

static int start_one(managed_process_t *mp) {
    printf("[ProcMgr] starting '%s\n", mp->spec.name);
    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[ProcMgr] fork failed for '%s': %s\n", mp->spec.name, strerror(errno));
        return -1;
    }
    if (pid == 0) {
        execv(mp->spec.path, mp->spec.argv);
        fprintf(stderr, "[ProcMgr] execv failed for '%s' (%s): %s\n", mp->spec.name, mp->spec.path,
                strerror(errno));
        _exit(127);
    }

    mp->pid = pid;
    fprintf(stderr, "[ProcMgr] started '%s' pid=%d\n", mp->spec.name, (int)pid);
    return 0;
}

int pm_start_all(process_manager_t *pm) {
    int started = 0;
    time_t now = time(NULL);

    for (int i = 0; i < pm->count; i++) {
        managed_process_t *mp = &pm->procs[i];
        mp->attempts     = 0;
        mp->window_start = now;
        mp->next_start   = now;
        mp->given_up      = 0;

        if (mp->pid == 0 && start_one(mp) == 0) {
            started++;
        }
    }
    return started;
}

int pm_start_one(process_manager_t *pm, const char *name) {
    for (int i = 0; i < pm->count; i++) {
        managed_process_t *mp = &pm->procs[i];
        if (strcmp(mp->spec.name, name) != 0) {
            continue;
        }
        if (mp->pid != 0) {
            return -1; // already running
        }

        time_t now = time(NULL);
        mp->attempts     = 0;
        mp->window_start = now;
        mp->next_start   = now;
        mp->given_up      = 0;

        return start_one(mp);
    }
    return -1; // no process registered with that name
}

void pm_tick(process_manager_t *pm) {
    time_t now = time(NULL);
    for (int i = 0; i < pm->count; i++) {
        managed_process_t *mp = &pm->procs[i];

        if (mp->shutting_down) {
            if (mp->pid > 0 && !mp->kill_sent && now >= mp->kill_deadline) {
                fprintf(stderr, "[ProcMgr] '%s' pid=%d did not exit in time, killing\n", mp->spec.name,
                        (int)mp->pid);
                kill(mp->pid, SIGKILL);
                mp->kill_sent = 1;
            }
            continue;
        }

        if (mp->pid == 0 && !mp->given_up && now >= mp->next_start) {
            start_one(mp);
        }
    }
}

managed_process_t *pm_find_by_pid(process_manager_t *pm, pid_t pid) {
    for (int i = 0; i < pm->count; i++) {
        if (pm->procs[i].pid == pid) {
            return &pm->procs[i];
        }
    }
    return NULL;
}

int pm_reap(process_manager_t *pm) {
    int reaped = 0;

    for (;;) {
        int status;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid <= 0) {
            break;
        }
        reaped++;

        managed_process_t *mp = pm_find_by_pid(pm, pid);
        if (mp == NULL) {
            continue; // not a process we're supervising
        }
        mp->pid = 0;

        if (mp->shutting_down) {
            fprintf(stderr, "[ProcMgr] '%s' pid=%d exited (shutdown complete)\n", mp->spec.name, (int)pid);
            mp->shutting_down = 0;
            mp->kill_sent     = 0;
            continue; // intentional stop - restart policy doesn't apply
        }

        int failed = WIFSIGNALED(status) || (WIFEXITED(status) && WEXITSTATUS(status) != 0);
        fprintf(stderr, "[ProcMgr] '%s' pid=%d exited (%s)\n", mp->spec.name, (int)pid,
                failed ? "failure" : "ok");

        int should_restart = mp->spec.restart == RESTART_ALWAYS ||
                              (mp->spec.restart == RESTART_ON_FAILURE && failed);
        if (!should_restart) {
            // RESTART_NEVER, or RESTART_ON_FAILURE with a clean exit: stay
            // stopped. Without this, pm_tick's restart check (pid == 0 &&
            // !given_up) can't tell "doesn't want to restart" from "hasn't
            // restarted yet" and would relaunch it on the next tick anyway.
            mp->given_up = 1;
            continue;
        }

        time_t now = time(NULL);
        if (now - mp->window_start > DAEMON_RESTART_WINDOW_SEC) {
            mp->window_start = now;
            mp->attempts     = 0;
        }
        mp->attempts++;

        if (mp->attempts > DAEMON_RESTART_MAX_ATTEMPTS) {
            mp->given_up = 1;
            fprintf(stderr, "[ProcMgr] '%s' restarted too many times in %ds, giving up\n", mp->spec.name,
                    DAEMON_RESTART_WINDOW_SEC);
            continue;
        }

        long delay_ms   = (long)DAEMON_RESTART_BACKOFF_MS * mp->attempts;
        mp->next_start  = now + (delay_ms + 999) / 1000;
    }

    return reaped;
}

void pm_shutdown_all(process_manager_t *pm, int timeout_ms) {
    for (int i = 0; i < pm->count; i++) {
        // Prevent pm_tick from racing a restart in behind us; pm_start_one
        // or pm_start_all clears this if a process is started again.
        pm->procs[i].given_up = 1;
        if (pm->procs[i].pid > 0) {
            kill(pm->procs[i].pid, SIGTERM);
        }
    }

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (;;) {
        int any_running = 0;
        for (int i = 0; i < pm->count; i++) {
            if (pm->procs[i].pid > 0) {
                any_running = 1;
                break;
            }
        }
        if (!any_running) {
            break;
        }

        int status;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid > 0) {
            managed_process_t *mp = pm_find_by_pid(pm, pid);
            if (mp != NULL) {
                mp->pid = 0;
            }
            continue;
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        long elapsed_ms = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
        if (elapsed_ms >= timeout_ms) {
            break;
        }

        struct timespec nap = {0, 50 * 1000 * 1000};
        nanosleep(&nap, NULL);
    }

    for (int i = 0; i < pm->count; i++) {
        if (pm->procs[i].pid > 0) {
            fprintf(stderr, "[ProcMgr] '%s' pid=%d did not exit in time, killing\n", pm->procs[i].spec.name,
                    (int)pm->procs[i].pid);
            kill(pm->procs[i].pid, SIGKILL);
            waitpid(pm->procs[i].pid, NULL, 0);
            pm->procs[i].pid = 0;
        }
    }
}

int pm_shutdown_one(process_manager_t *pm, const char *name, int timeout_ms) {
    managed_process_t *mp = NULL;
    for (int i = 0; i < pm->count; i++) {
        if (strcmp(pm->procs[i].spec.name, name) == 0) {
            mp = &pm->procs[i];
            break;
        }
    }
    if (mp == NULL) {
        return -1; // no process registered with that name
    }

    // Prevent pm_tick from racing a restart in behind us; pm_start_one
    // clears this if the caller wants it back.
    mp->given_up = 1;

    if (mp->pid <= 0) {
        return 0;
    }

    kill(mp->pid, SIGTERM);
    mp->shutting_down = 1;
    mp->kill_sent      = 0;
    mp->kill_deadline  = time(NULL) + (timeout_ms + 999) / 1000;

    return 0;
}
