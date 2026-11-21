#!/usr/bin/env python3
"""发包示例：向指定组播频道（组播地址 + 端口）发送 IPv4 UDP 数据报。

用法：
    python3 examples/send_multicast.py --group 239.10.0.1 --port 5000
    python3 examples/send_multicast.py --group 239.10.0.2 --port 5000 --count 0 --interval 0.5
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
    parser.add_argument("--group", default="239.10.0.1", help="组播地址（频道），默认 239.10.0.1")
    parser.add_argument("--port", type=int, default=5000, help="组播端口（频道），默认 5000")
    parser.add_argument("--message", default=None, help="自定义报文；默认自动生成带序号的内容")
    parser.add_argument("--count", type=int, default=10, help="发送个数，默认 10；<=0 表示持续发送")
    parser.add_argument("--interval", type=float, default=1.0, help="发送间隔秒数，默认 1.0")
    parser.add_argument("--ttl", type=int, default=1, help="IP_MULTICAST_TTL，默认 1")
    parser.add_argument("--interface", default=None, help="发送网卡 IP（IP_MULTICAST_IF），默认由内核选择")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, args.ttl)
    if args.interface:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(args.interface))

    seq = 0
    try:
        while args.count <= 0 or seq < args.count:
            payload = args.message or f"{args.group}:{args.port} seq={seq} ts={time.time():.3f}"
            sock.sendto(payload.encode(), (args.group, args.port))
            print(f"sent -> {args.group}:{args.port}: {payload}", flush=True)
            seq += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
