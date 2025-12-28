#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (PATTERNED TIME-SERIES EDITION)

✔ Continuous Stream (Fire & Forget)
✔ Random Walk Pattern (13k–19.8k)
✔ Temporal Correlation (naik–turun alami)
✔ Watchdog Auto-Restart
"""

import threading
import time
import random
import os
import subprocess
import re
import shutil
import psycopg2
from datetime import datetime
import signal
import sys

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= 1. KONFIGURASI =================
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1.0

TARGET_CENTER = 16500

BYTES_MIN = 13000
BYTES_MAX = 19800
DELTA_MAX = 1500

last_pattern_bytes = TARGET_CENTER

HOST_INFO = {
    'user1': {'ip': '10.0.1.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.0.2.1'},
    'user2': {'ip': '10.0.1.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.0.3.1'},
    'web1':  {'ip': '10.0.2.1', 'mac': '00:00:00:00:02:01', 'dst_ip': '10.0.1.1'},
    'web2':  {'ip': '10.0.2.2', 'mac': '00:00:00:00:02:02', 'dst_ip': '10.0.3.2'},
    'app1':  {'ip': '10.0.3.1', 'mac': '00:00:00:00:03:01', 'dst_ip': '10.0.2.2'},
    'db1':   {'ip': '10.0.3.2', 'mac': '00:00:00:00:03:02', 'dst_ip': '10.0.1.2'},
}

IP_TO_MAC = {v['ip']: v['mac'] for v in HOST_INFO.values()}
stop_event = threading.Event()

ITG_SEND_BIN = shutil.which("ITGSend") or "/usr/local/bin/ITGSend"
ITG_RECV_BIN = shutil.which("ITGRecv") or "/usr/local/bin/ITGRecv"

# ================= 2. TOPOLOGI =================
class LeafSpineTopo(Topo):
    def build(self):
        spines = [self.addSwitch(f'spine{i}', dpid=f'10{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}', dpid=f'20{i}') for i in range(1, 4)]

        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000)

        self.addLink(self.addHost('user1', ip='10.0.1.1/8', mac='00:00:00:00:01:01'), leaves[0], bw=100)
        self.addLink(self.addHost('user2', ip='10.0.1.2/8', mac='00:00:00:00:01:02'), leaves[0], bw=100)
        self.addLink(self.addHost('web1',  ip='10.0.2.1/8', mac='00:00:00:00:02:01'), leaves[1], bw=100)
        self.addLink(self.addHost('web2',  ip='10.0.2.2/8', mac='00:00:00:00:02:02'), leaves[1], bw=100)
        self.addLink(self.addHost('app1',  ip='10.0.3.1/8', mac='00:00:00:00:03:01'), leaves[2], bw=100)
        self.addLink(self.addHost('db1',   ip='10.0.3.2/8', mac='00:00:00:00:03:02'), leaves[2], bw=100)

# ================= 3. RANDOM PATTERN =================
def patterned_bytes(prev):
    delta = random.randint(-DELTA_MAX, DELTA_MAX)
    val = prev + delta

    if val < BYTES_MIN:
        val = BYTES_MIN + random.randint(0, 500)
    elif val > BYTES_MAX:
        val = BYTES_MAX - random.randint(0, 500)

    return val

# ================= 4. COLLECTOR =================
def run_ns_cmd(host, cmd):
    if not os.path.exists(f"/var/run/netns/{host}"):
        return None
    try:
        return subprocess.run(
            ['sudo', 'ip', 'netns', 'exec', host] + cmd,
            capture_output=True, text=True, timeout=0.2
        ).stdout
    except:
        return None

def get_stats(host):
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', f"{host}-eth0"])
    if not out:
        return None

    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out)

    if rx and tx:
        return {
            'tx_bytes': int(tx.group(1)),
            'rx_bytes': int(rx.group(1)),
            'tx_pkts': int(tx.group(2)),
            'rx_pkts': int(rx.group(2))
        }
    return None

def collector_thread(net):
    global last_pattern_bytes
    info("*** [Collector] STARTED\n")

    try:
        conn = psycopg2.connect(DB_CONN)
    except:
        conn = None

    last_stats = {}

    while not stop_event.is_set():
        time.sleep(COLLECT_INTERVAL)
        now = datetime.now()
        rows = []
        traffic_alive = False

        for h, meta in HOST_INFO.items():
            curr = get_stats(h)
            if not curr:
                continue

            if h not in last_stats:
                last_stats[h] = curr
                continue

            prev = last_stats[h]
            dtx = curr['tx_bytes'] - prev['tx_bytes']
            last_stats[h] = curr

            if dtx > 500:
                traffic_alive = True

                synthetic = patterned_bytes(last_pattern_bytes)
                last_pattern_bytes = synthetic

                rows.append((
                    now, 0,
                    meta['ip'], meta['dst_ip'],
                    meta['mac'], IP_TO_MAC.get(meta['dst_ip']),
                    17, 5060, 5060,
                    synthetic,
                    int(synthetic * 0.5),
                    max(1, synthetic // 200),
                    max(1, synthetic // 400),
                    COLLECT_INTERVAL,
                    "normal"
                ))

        if rows and conn:
            try:
                cur = conn.cursor()
                cur.executemany("""
                    INSERT INTO traffic.flow_stats_
                    (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                     ip_proto, tp_src, tp_dst,
                     bytes_tx, bytes_rx, pkts_tx, pkts_rx,
                     duration_sec, traffic_label)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, rows)
                conn.commit()
            except:
                conn.rollback()

        if not traffic_alive:
            info("*** [Watchdog] Traffic dead – restarting\n")
            start_traffic_stream(net)

# ================= 5. TRAFFIC GENERATOR =================
def start_traffic_stream(net):
    info("*** [Traffic] Starting Continuous Stream\n")

    for h in net.hosts:
        h.cmd("killall -9 ITGRecv")
        h.cmd(f"{ITG_RECV_BIN} > /dev/null 2>&1 &")

    time.sleep(2)

    payload = 160
    wire = 202
    pps = int(TARGET_CENTER / wire)

    for h, meta in HOST_INFO.items():
        net.get(h).cmd(
            f"{ITG_SEND_BIN} -T UDP -a {meta['dst_ip']} "
            f"-c {payload} -C {pps} -t 3600000 "
            f"> /dev/null 2>&1 &"
        )

# ================= 6. MAIN =================
def force_cleanup():
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True)
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True)

if __name__ == "__main__":
    force_cleanup()
    setLogLevel('info')

    net = Mininet(
        topo=LeafSpineTopo(),
        controller=RemoteController,
        switch=OVSKernelSwitch,
        link=TCLink
    )

    try:
        net.start()
        subprocess.run("sudo mkdir -p /var/run/netns", shell=True)

        for h in net.hosts:
            subprocess.run(
                f"sudo ln -s /proc/{h.pid}/ns/net /var/run/netns/{h.name}",
                shell=True
            )

        start_traffic_stream(net)
        collector_thread(net)

    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        force_cleanup()
        net.stop()
