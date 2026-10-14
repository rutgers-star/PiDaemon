#ifndef DAEMON_CONFIG_H
#define DAEMON_CONFIG_H

// Compile-time defaults. Each can be overridden at runtime with the
// matching PIOBC_*_PATH environment variable, which is useful for running
// the daemon unprivileged during development/testing.
#define DAEMON_STATE_PATH_DEFAULT  "/var/lib/piobc/daemon_state.bin"
#define DAEMON_SOCKET_PATH_DEFAULT "/run/piobc/daemond.sock"
#define DAEMON_PID_PATH_DEFAULT    "/run/piobc/daemond.pid"

// File video_uploader (Daemon/src/video_uploader.c) reads and ships to the
// OBC as a bulk Image/Video Data Transfer (APID 0x032) on daemon startup.
// Drop a real video file at this path (or point PIOBC_VIDEO_UPLOAD_PATH at
// one) to exercise a real bulk upload through the real "network" bridge; if
// it's absent, register_video_uploader() just skips supervising the process.
#define DAEMON_VIDEO_UPLOAD_PATH_DEFAULT "/var/lib/piobc/video_upload.bin"

#define DAEMON_NAME_MAX         32
#define DAEMON_MAX_CLIENTS      32
#define DAEMON_MAX_PROCESSES    16
#define DAEMON_MAX_MSG_PAYLOAD  4096

// Process supervision
#define DAEMON_RESTART_MAX_ATTEMPTS  5   // within the window below, per process
#define DAEMON_RESTART_WINDOW_SEC    60
#define DAEMON_RESTART_BACKOFF_MS    500 // multiplied by attempt count

#define DAEMON_SHUTDOWN_GRACE_MS     3000
#define DAEMON_TICK_MS               1000

const char *daemon_state_path(void);
const char *daemon_socket_path(void);
const char *daemon_pid_path(void);
const char *daemon_video_upload_path(void);

#endif
