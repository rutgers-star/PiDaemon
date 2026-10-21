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

#endif
