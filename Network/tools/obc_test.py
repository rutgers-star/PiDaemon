#!/usr/bin/env python3
"""Acts as the OBC: sends payload-link framed test requests over a serial
port and prints back whatever bytes come in response. Mirrors the exact
wire format implemented in Network/src/main.c + PayloadLink:

    SYNC 0x1ACFFC1D (4B) | LENGTH (2B BE, body len) | BODY | CRC-16 (2B BE)

BODY (Network/src/main.c's own convention on top of the opaque payload-link
body):
    APID (2B BE) | LEN (2B BE) | DATA (LEN bytes) | zero pad to >= 7 bytes

No pyserial dependency - raw termios/os, matching Network/src/main.c's
open_serial_port() (115200 8N1, no flow control, raw mode).

Usage: wire this machine's serial port to the Pi's RS-422 link (e.g. a
USB-serial adapter's ttyUSB0 to the Pi's ttyS0), have daemond + network
(+ optionally payload_app, for a full round trip) running on the Pi, then:

    python3 obc_test.py [/dev/ttyUSB0]
"""
import os
import sys
import termios
import time

SYNC = bytes([0x1A, 0xCF, 0xFC, 0x1D])
MIN_BODY_LEN = 7
MAX_BODY_LEN = 518

# APIDs (star/payload_apids.h)
STAR_APID_PAYLOAD_COMMAND = 0x010
STAR_APID_ADCS_TELEMETRY_PASSTHROUGH = 0x020
STAR_APID_ADCS_REQUEST_RESPONSE = 0x022


def crc16_ccitt_false(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def build_body(apid: int, data: bytes) -> bytes:
    body = apid.to_bytes(2, "big") + len(data).to_bytes(2, "big") + data
    if len(body) < MIN_BODY_LEN:
        body += b"\x00" * (MIN_BODY_LEN - len(body))
    return body


def encode_frame(body: bytes) -> bytes:
    assert MIN_BODY_LEN <= len(body) <= MAX_BODY_LEN
    length = len(body).to_bytes(2, "big")
    crc = crc16_ccitt_false(body).to_bytes(2, "big")
    return SYNC + length + body + crc


def open_serial(path: str) -> int:
    fd = os.open(path, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    attrs = termios.tcgetattr(fd)
    iflag, oflag, cflag, lflag, ispeed, ospeed, cc = attrs

    iflag &= ~(termios.IXON | termios.IXOFF | termios.IXANY |
               termios.IGNBRK | termios.BRKINT | termios.PARMRK |
               termios.ISTRIP | termios.INLCR | termios.IGNCR | termios.ICRNL)
    oflag &= ~(termios.OPOST | termios.ONLCR)
    cflag &= ~(termios.PARENB | termios.CSTOPB | termios.CSIZE | termios.CRTSCTS)
    cflag |= termios.CS8 | termios.CREAD | termios.CLOCAL
    lflag &= ~(termios.ICANON | termios.ECHO | termios.ECHOE |
               termios.ECHONL | termios.ISIG)
    cc[termios.VTIME] = 0
    cc[termios.VMIN] = 0
    ispeed = ospeed = termios.B115200

    termios.tcsetattr(fd, termios.TCSANOW,
                       [iflag, oflag, cflag, lflag, ispeed, ospeed, cc])
    return fd


def send_frame(fd: int, apid: int, data: bytes, label: str):
    frame = encode_frame(build_body(apid, data))
    print(f"--> {label}: apid=0x{apid:03X} data={data.hex()} "
          f"frame={frame.hex()} ({len(frame)} bytes)")
    os.write(fd, frame)


def drain_response(fd: int, timeout_s: float = 1.5):
    end = time.time() + timeout_s
    chunks = []
    while time.time() < end:
        try:
            chunk = os.read(fd, 256)
        except BlockingIOError:
            chunk = b""
        if chunk:
            chunks.append(chunk)
        else:
            time.sleep(0.05)
    total = b"".join(chunks)
    if total:
        print(f"<-- received {len(total)} bytes: {total.hex()}")
    else:
        print("<-- no response bytes received")
    return total


def main():
    port = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
    fd = open_serial(port)
    print(f"[obc_test] opened {port} @ 115200 8N1\n")

    try:
        # 1. Legitimate command frame: opcode 0x03 = CMD_TIME_SYNC_PING
        #    (see Daemon/src/payload_app.c). Exercises STAR_APID_PAYLOAD_COMMAND.
        send_frame(fd, STAR_APID_PAYLOAD_COMMAND, bytes([0x03]), "CMD_TIME_SYNC_PING")
        drain_response(fd)
        print()

        # 2. Dummy ADCS telemetry passthrough frame - exercises a different APID.
        send_frame(fd, STAR_APID_ADCS_TELEMETRY_PASSTHROUGH, bytes(range(16)),
                   "ADCS telemetry passthrough (dummy payload)")
        drain_response(fd)
        print()

        # 3. Deliberately corrupt CRC - the decoder should log a bad-CRC
        #    resync and recover cleanly for the next real frame.
        good = encode_frame(build_body(STAR_APID_ADCS_REQUEST_RESPONSE, bytes([0xAA])))
        corrupted = good[:-1] + bytes([good[-1] ^ 0xFF])
        print(f"--> corrupted-CRC frame: {corrupted.hex()}")
        os.write(fd, corrupted)
        drain_response(fd)
        print()

        # 4. Follow-up good frame to confirm the decoder resynced properly
        #    after the corrupted one above.
        send_frame(fd, STAR_APID_PAYLOAD_COMMAND, bytes([0x03]),
                   "CMD_TIME_SYNC_PING (post-resync check)")
        drain_response(fd)

    finally:
        os.close(fd)


if __name__ == "__main__":
    main()
