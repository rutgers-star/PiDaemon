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

#endif
