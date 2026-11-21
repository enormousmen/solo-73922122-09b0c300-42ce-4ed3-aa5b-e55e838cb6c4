#!/usr/bin/env python3
"""收包示例：模拟播出端，在单播地址 + 端口上接收网关转发来的数据报。

用法：
    python3 examples/receive_unicast.py --ip 127.0.0.1 --port 9000

然后用订阅 API 把该地址登记到某个频道，即可看到转发过来的数据报：
    curl -X POST http://127.0.0.1:8000/api/v1/subscriptions \
         -H 'Content-Type: application/json' \
         -d '{"subscriber": {"ip": "127.0.0.1", "port": 9000},
              "channel": {"group": "239.10.0.1", "port": 5000}}'
"""

from __future__ import annotations

import argparse
import socket
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--ip", default="127.0.0.1", help="监听的单播地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=9000, help="监听的 UDP 端口，默认 9000")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.ip, args.port))
    print(f"listening on {args.ip}:{args.port} (unicast) ...", flush=True)
    try:
        while True:
            data, addr = sock.recvfrom(65535)
            print(f"[{time.strftime('%H:%M:%S')}] from {addr[0]}:{addr[1]}: {data!r}", flush=True)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
