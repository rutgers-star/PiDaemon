#ifndef NET_IPC_H
#define NET_IPC_H

#include <stdint.h>

// Local IPC framing carried inside daemon/proto.h's DMSG_SEND/DMSG_DELIVER
// payload, between the "network" bridge process (Network/src/main.c, which
// owns the SPICEnet/RS-422 link to the OBC) and payload application
// processes registered with daemond.
//
// This is NOT the over-the-wire SPICEnet frame (ICD Section 2.4) - it only
// carries one APID's User Data Field, already packed per the ICD Section 5
// dictionary (big-endian fields), between two local processes on the same
// machine. Like daemon/proto.h, native byte order/struct layout is fine for
// this header itself since it never leaves the machine.
//
// app -> "network": transmit `data` on `apid` out over the SPICEnet link.
// "network" -> app: `data` was received on `apid` from the OBC.
typedef struct {
    uint16_t apid;
    uint16_t len; // bytes of `data` that immediately follow this header
} net_ipc_hdr_t;

#define NET_IPC_MAX_DATA 512 // matches ICD MAX_PACKET_LENGTH

#define NET_IPC_NETWORK_NAME     "network"
#define NET_IPC_PAYLOAD_APP_NAME "payload_app"

// Control pseudo-APID asking "network" to read a file straight off disk and
// send it as an ICD Bulk Data Transfer (Section 5.8), instead of the sender
// reading/chunking it and streaming the bytes over IPC itself. Real APIDs
// are CCSDS 11-bit values (0x000-0x7FF, see star/payload_apids.h) so this
// sentinel, placed above that range, can never collide with one.
//
// Sender and "network" must share a filesystem namespace (true today: both
// run on the same Pi) - this only carries a path, not the file's bytes.
// "network" paces reading/sending the file to its own schedule (see the tx
// priority queues in Network/src/main.c) rather than being handed data
// faster than the serial link can drain it.
#define NET_IPC_CTRL_BULK_UPLOAD UINT16_C(0xF000)
#define NET_IPC_BULK_UPLOAD_PATH_MAX 256

typedef struct {
    uint32_t transfer_id;
    char     file_path[NET_IPC_BULK_UPLOAD_PATH_MAX]; // NUL-terminated
} net_ipc_bulk_upload_t;

// ---- semantic "build and send" control messages ----
//
// For every other outbound APID, the app hands "network" the semantic
// values from ICD Section 5 (a quaternion, a status code, a batch of
// sensor entries) and "network" owns packing the actual wire format -
// including the per-message counters (Command Counter / Request Counter)
// and, for bulk data, the Section 5.8 chunking/wrapping. This is the same
// division NET_IPC_CTRL_BULK_UPLOAD already uses for file-based transfers,
// just generalized to every message type instead of only video.
//
// None of these are CCSDS/APID values (all >= 0xF000, outside the 11-bit
// APID range) so they can't collide with a real one.
#define NET_IPC_CTRL_SEND_TELEMETRY    UINT16_C(0xF001) // -> APID 0x011
#define NET_IPC_CTRL_SEND_CMD_RESPONSE UINT16_C(0xF002) // -> APID 0x012
#define NET_IPC_CTRL_SEND_ADCS_REQUEST UINT16_C(0xF003) // -> APID 0x021
#define NET_IPC_CTRL_SEND_BULK_ACK     UINT16_C(0xF004) // -> APID 0x035
#define NET_IPC_CTRL_SEND_CALC_DATA    UINT16_C(0xF005) // -> APID 0x030 (chunked)
#define NET_IPC_CTRL_SEND_FT_DATA      UINT16_C(0xF006) // -> APID 0x031 (chunked)

// ICD Section 5.3.
typedef struct {
    uint64_t met_ms;
    uint8_t  op_state;
    uint8_t  active_experiment_id;
    float    cpu_temp_c;
    uint32_t storage_mb_remaining;
} net_ipc_telemetry_t;

// ICD Section 5.4. command_counter is copied from the command being
// acknowledged - the app parses it out of the inbound APID 0x010 message
// and passes it straight through; "network" doesn't track command state.
typedef struct {
    uint8_t  opcode;
    uint16_t command_counter;
    uint8_t  status;
} net_ipc_cmd_response_t;

// ICD Section 5.6. request_type selects which fields the receiver honors,
// but "network" always sends the full fixed 35-byte message (the ICD
// requires target_quat/target_rates/duration_ms to be present and zeroed,
// not omitted, for 0xA0/0xA1/0xA2) and assigns the Request Counter itself.
typedef struct {
    uint8_t  request_type;
    float    target_quat[4];
    float    target_rates[3];
    uint32_t duration_ms;
} net_ipc_adcs_request_t;

// ICD Section 5.9. missing[] is only meaningful (and only sent) when status
// is NACK (0x01); NET_IPC_BULK_ACK_MAX_MISSING is below the ICD's wire cap
// of 125 only to keep this struct comfortably inside one IPC frame.
#define NET_IPC_BULK_ACK_MAX_MISSING 100
typedef struct {
    uint32_t transfer_id;
    uint8_t  status;
    uint32_t chunks_received;
    uint32_t num_missing; // <= NET_IPC_BULK_ACK_MAX_MISSING
    uint32_t missing[NET_IPC_BULK_ACK_MAX_MISSING];
} net_ipc_bulk_ack_t;

// ICD Section 5.8.1 entry ("network" packs, chunks at 13 entries/frame per
// the ICD, and sends as APID 0x030).
typedef struct {
    uint64_t mission_time;
    float    angular_accel[3];
    float    commanded_torque[4];
} net_ipc_calc_entry_t;

// ICD Section 5.8.2 entry ("network" packs, chunks at 15 entries/frame per
// the ICD, and sends as APID 0x031).
typedef struct {
    uint64_t mission_time;
    float    force_vector[3];
    float    torque_vector[3];
} net_ipc_ft_entry_t;

#define NET_IPC_MAX_CALC_ENTRIES_PER_CALL 16
typedef struct {
    uint32_t              num_entries; // <= NET_IPC_MAX_CALC_ENTRIES_PER_CALL
    net_ipc_calc_entry_t  entries[NET_IPC_MAX_CALC_ENTRIES_PER_CALL];
} net_ipc_calc_data_t;

#define NET_IPC_MAX_FT_ENTRIES_PER_CALL 16
typedef struct {
    uint32_t            num_entries; // <= NET_IPC_MAX_FT_ENTRIES_PER_CALL
    net_ipc_ft_entry_t  entries[NET_IPC_MAX_FT_ENTRIES_PER_CALL];
} net_ipc_ft_data_t;

// Generous bound for the control-message buffers above (the largest,
// net_ipc_calc_data_t, is ~660 bytes) - independent of NET_IPC_MAX_DATA,
// which is sized to one ICD wire packet, not an IPC-local struct.
#define NET_IPC_CTRL_MAX_DATA 2048

#endif
