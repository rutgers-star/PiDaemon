# Obc

A rough, single-process stand-in for the busOBC (Zynq 7000 / cFS) side of
the SPICEnet link described in the ICD. It's the other end of the wire from
`Daemon/` + `Network/` (the payOBC / Raspberry Pi side): same RS-422 framing
(`PayloadLink`), same APID dictionary (`PayloadApids`), but running as its
own standalone program instead of real cFS flight software, since this repo
doesn't carry a cFS build.

It is **not** flight software. It exists so you can exercise the real wire
protocol end-to-end - commanding, ADCS request approval, and bulk data
(including video) transfer - against the real `Network`/`Daemon` stack on a
Pi, over an actual RS-422/USB-serial link, without cFS in the loop.

## Building

Built as part of the top-level CMake project (see repo root `CMakeLists.txt`):

```sh
cmake -S . -B build
cmake --build build
./build/Obc/obc_daemon [serial-port]
```

`serial-port` defaults to `/dev/ttyUSB0`. On the Pi side, run `daemond` +
`Communications` (`Network/`) against the other end of the same link.

## What it does

- Opens the serial port 8N1 @ 115200, matching ICD Section 1.2.2.
- Encodes/decodes the exact payload-link frame format (`PayloadLink`) the
  Pi side speaks, so it's wire-compatible with `Network/src/main.c`.
- **Commands** (APID 0x010): grant/revoke DCE authority, time sync ping, run
  experiment, abort experiment, emergency halt - sent interactively from the
  console (see below) and logs the ACK/NACK command response (0x012).
- **Telemetry** (APID 0x011): logs payload health (MET, state, CPU temp,
  storage) as it arrives; this is also what drives the link watchdog.
- **ADCS telemetry passthrough** (APID 0x020): sends a simulated, slowly
  rotating quaternion once a second by default (`adcs on|off` to toggle) -
  stands in for real attitude data the flight software would forward.
- **ADCS pointing requests** (APID 0x021 in, 0x022 out): logs every request
  and auto-ACKs it. A real busOBC would run a safety check (torque/attitude
  limits, current spacecraft mode) before approving - that policy isn't
  implemented here, just the plumbing.
- **Bulk data receive** (APIDs 0x030/0x031/0x032 - calculation data,
  force-torque data, video): reassembles chunks straight to disk under
  `obc_rx/` (created on startup), keyed by transfer ID, writing each chunk at
  its byte offset so out-of-order arrival still reconstructs correctly. ACKs
  (0x035) on completion; NACKs with the list of missing chunk indices if a
  session stalls for more than 5 seconds. Calc/force-torque files are the
  packed entry structs from ICD 5.8.1/5.8.2 back-to-back with no extra
  framing - parse offline with Python's `struct` module. Video files are the
  raw bytes the payload sent (ICD 5.8.3) - e.g. if the Pi is sending an H.264
  stream, the `.bin` file is that stream and can be remuxed/played directly.
- **Bulk data send** (`sendtable`/`sendalgo` console commands, APIDs
  0x033/0x034): chunks a local file and sends it; logs the resulting
  ACK/NACK (0x035) from the payload.
- A simple link watchdog: if no frame has been received from the payload for
  10 seconds (ICD 4.2/4.3's "10 heartbeats" at the 1 Hz minimum rate), logs a
  warning.

## Console

```
obc> help
```

lists the available commands (`grant`, `revoke`, `timesync`, `run <id>`,
`abort`, `halt`, `sendtable <file>`, `sendalgo <file>`, `adcs <on|off>`,
`status`, `quit`). All inbound traffic (telemetry, command responses, ADCS
requests, bulk receives) is handled automatically regardless of what you type
- the console is only for things a real busOBC would initiate. If stdin is
closed or redirected from `/dev/null` (e.g. running under a service
manager), the console is simply disabled and it runs headless as a
background responder/logger - that's the "daemon" mode.

## Known gaps (left for whoever picks this up)

- No retransmission of our *own* outbound bulk sends on NACK - `sendtable`/
  `sendalgo` fire-and-forget all chunks once; if the payload NACKs, you have
  to re-run the command. The receive side (0x030/0x031/0x032) does implement
  NACK-with-missing-chunks.
- No persistence across restarts - the ICD's "resume file transfer from the
  last confirmed byte offset" behavior (Section 4.2) isn't implemented; a
  restart starts cold.
- The ADCS pointing-request approval policy is "always ACK." Real safety
  logic (torque limits, mode checks, DCE authority bookkeeping beyond the
  single `g_dce_held_by_pay` flag) belongs in flight software, not here.
- Single in-flight session per bulk APID (one calc, one force-torque, one
  video transfer at a time) - a new transfer on the same APID abandons an
  incomplete one rather than queuing it.
