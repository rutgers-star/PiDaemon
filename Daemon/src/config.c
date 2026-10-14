#include "daemon/config.h"

#include <stdlib.h>

const char *daemon_state_path(void) {
    const char *env = getenv("PIOBC_STATE_PATH");
    return env != NULL ? env : DAEMON_STATE_PATH_DEFAULT;
}

const char *daemon_socket_path(void) {
    const char *env = getenv("PIOBC_SOCKET_PATH");
    return env != NULL ? env : DAEMON_SOCKET_PATH_DEFAULT;
}

const char *daemon_pid_path(void) {
    const char *env = getenv("PIOBC_PID_PATH");
    return env != NULL ? env : DAEMON_PID_PATH_DEFAULT;
}

const char *daemon_video_upload_path(void) {
    const char *env = getenv("PIOBC_VIDEO_UPLOAD_PATH");
    return env != NULL ? env : DAEMON_VIDEO_UPLOAD_PATH_DEFAULT;
}
