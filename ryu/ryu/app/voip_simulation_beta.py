#!/usr/bin/python3
"""
LEAF–SPINE 3x3 VOIP SIMULATION (D-ITG FINAL)
- True VoIP traffic
- Stable watchdog
- Mininet-native stats
"""

from mininet.net import Mininet
from mininet.node import OVSController
from mininet.link import TCLink
from mininet.log import setLogLevel, info

import time
import threading
import subprocess
import re
import signal
import sys
import os

# ================= CONFIG =================
COLLECT_INTERVAL = 1.0
TX_THRESHOLD = 300
UDP_PORT = 5060

SENDER_HOSTS = ['user1']

HOST_INFO = {
    'user1': {'ip': '10.0.0.1', 'dst': '10.0.0.2'},
    'user2': {'ip': '10.0.0.2', 'dst': None},
}

stop_event = threading.Event()
traffic_proc = None

# =========================================


def build_topology():
    net = Mininet(controller=OVSController, link=TCLink)
    net.addController('c0')

    spines = [net.addSwitch(f'spine{i}') for i in range(1, 4)]
    leaves = [net.addSwitch(f'leaf{i}') for i in range(1, 4)]

    user1 = net.addHost('user1', ip='10.0.0.1/24')
    user2 = net.addHost('user2', ip='10.0.0.2/24')

    net.addLink(user1, leaves[0], bw=1000)
    net.addLink(user2, leaves[1], bw=1000)

    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000)

    net.start()
    return net


# ============== TRAFFIC ===================

def start_traffic(net):
    global traffic_proc

    info("*** [Traffic] Starting D-ITG VoIP stream\n")

    sender = net.get('user1')
    dst = HOST_INFO['user1']['dst']

    cmd = (
        f"ITGSend -T UDP "
        f"-a {dst} "
        f"-rp {UDP_PORT} "
        f"-C 100 "
        f"-c 160 "
        f"-t 0 "
        f"-x recv.log"
    )

    traffic_proc = sender.popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


def stop_traffic():
    global traffic_proc
    if traffic_proc:
        try:
            traffic_proc.send_signal(signal.SIGINT)
        except Exception:
            pass
        traffic_proc = None


# ============== STATS =====================

def get_stats(net, host):
    h = net.get(host)
    out = h.cmd(f"ip -s link show {host}-eth0")

    if not out:
        return None

    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out)

    if rx and tx:
        return {
            'rx_bytes': int(rx.group(1)),
            'tx_bytes': int(tx.group(1))
        }
    return None


# ============== COLLECTOR =================

def collector(net):
    info("*** [Collector] STARTED\n")

    last_tx = {}

    while not stop_event.is_set():
        active_flows = 0

        for h in SENDER_HOSTS:
            stat = get_stats(net, h)
            if not stat:
                continue

            prev = last_tx.get(h, stat['tx_bytes'])
            delta = stat['tx_bytes'] - prev
            last_tx[h] = stat['tx_bytes']

            if delta > TX_THRESHOLD:
                active_flows += 1

        if active_flows == 0:
            info("*** [Watchdog] Traffic dead – restarting\n")
            stop_traffic()
            time.sleep(1)
            start_traffic(net)

        time.sleep(COLLECT_INTERVAL)


# ============== MAIN ======================

def main():
    setLogLevel('info')
    net = build_topology()

    # Receiver
    net.get('user2').cmd(f"ITGRecv &")

    start_traffic(net)

    t = threading.Thread(target=collector, args=(net,), daemon=True)
    t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        info("\n*** Stopping simulation\n")
        stop_event.set()
        stop_traffic()
        net.stop()
        sys.exit(0)


if __name__ == '__main__':
    main()
