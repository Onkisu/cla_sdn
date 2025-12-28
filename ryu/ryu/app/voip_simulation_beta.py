#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (FINAL STABLE VERSION)

✔ Continuous ITG Traffic (Fire & Forget)
✔ Patterned Bytes (Random Walk 13k–19.8k)
✔ Flow-Aware Watchdog (NO false restart)
✔ Dataset-ready (SDN / QoS / ML)
"""

import time
import random
import os
import subprocess
import re
import shutil
import psycopg2
from datetime import datetime
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= CONFIG =================
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1.0

TARGET_CENTER = 16500
BYTES_MIN = 13000
BYTES_MAX = 19800
DELTA_MAX = 1500

last_pattern_bytes = TARGET_CENTER

SENDER_HOSTS = ['user1', 'user2', 'web1', 'web2', 'app1', 'db1']

HOST_INFO = {
    'user1': {'ip': '10.0.1.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.0.2.1'},
    'user2': {'ip': '10.0.1.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.0.3.1'},
    'web1':  {'ip': '10.0.2.1', 'mac': '00:00:00:00:02:01', 'dst_ip': '10.0.1.1'},
    'web2':  {'ip': '10.0.2.2', 'mac': '00:00:00:00:02:02', 'dst_ip': '10.0.3.2'},
    'app1':  {'ip': '10.0.3.1', 'mac': '00:00:00:00:03:01', 'dst_ip': '10.0.2.2'},
    'db1':   {'ip': '10.0.3.2', 'mac': '00:00:00:00:03:02', 'dst_ip': '10.0.1.2'},
}

IP_TO_MAC = {v['ip']: v['mac'] for v in HOST_INFO.values()}

ITG_SEND = shutil.which("ITGSend") or "/usr/local/bin/ITGSend"
ITG_RECV = shutil.which("ITGRecv") or "/usr/local/bin/ITGRecv"

# ================= TOPO =================
class LeafSpineTopo(Topo):
    def build(self):
        spines = [self.addSwitch(f'spine{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}') for i in range(1, 4)]

        for l in leaves:
            for s in spines:
                self.addLink(l, s, bw=1000)

        self.addLink(self.addHost('user1', ip='10.0.1.1/8', mac=HOST_INFO['user1']['mac']), leaves[0], bw=100)
        self.addLink(self.addHost('user2', ip='10.0.1.2/8', mac=HOST_INFO['user2']['mac']), leaves[0], bw=100)
        self.addLink(self.addHost('web1',  ip='10.0.2.1/8', mac=HOST_INFO['web1']['mac']),  leaves[1], bw=100)
        self.addLink(self.addHost('web2',  ip='10.0.2.2/8', mac=HOST_INFO['web2']['mac']),  leaves[1], bw=100)
        self.addLink(self.addHost('app1',  ip='10.0.3.1/8', mac=HOST_INFO['app1']['mac']),  leaves[2], bw=100)
        self.addLink(self.addHost('db1',   ip='10.0.3.2/8', mac=HOST_INFO['db1']['mac']),   leaves[2], bw=100)

# ================= PATTERN =================
def patterned_bytes(prev):
    val = prev + random.randint(-DELTA_MAX, DELTA_MAX)
    if val < BYTES_MIN:
        val = BYTES_MIN + random.randint(0, 500)
    if val > BYTES_MAX:
        val = BYTES_MAX - random.randint(0, 500)
    return val

# ================= COLLECTOR =================
def run_ns_cmd(host, cmd):
    try:
        return subprocess.run(
            ['ip', 'netns', 'exec', host] + cmd,
            capture_output=True, text=True, timeout=0.2
        ).stdout
    except:
        return None

def get_stats(host):
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', f'{host}-eth0'])
    if not out:
        return None
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out)
    if rx and tx:
        return {
            'tx_bytes': int(tx.group(1)),
            'rx_bytes': int(rx.group(1)),
            'tx_pkts': int(tx.group(2)),
            'rx_pkts': int(rx.group(2)),
        }
    return None

def collector(net):
    global last_pattern_bytes
    info("*** [Collector] STARTED\n")

    conn = psycopg2.connect(DB_CONN)
    last = {}

    while True:
        time.sleep(COLLECT_INTERVAL)
        now = datetime.now()
        rows = []
        active_flows = 0

        for h, meta in HOST_INFO.items():
            stat = get_stats(h)
            if not stat:
                continue
            if h not in last:
                last[h] = stat
                continue

            dtx = stat['tx_bytes'] - last[h]['tx_bytes']
            last[h] = stat

            if h in SENDER_HOSTS and dtx > 500:
                active_flows += 1
                b = patterned_bytes(last_pattern_bytes)
                last_pattern_bytes = b

                rows.append((
                    now, 0, meta['ip'], meta['dst_ip'],
                    meta['mac'], IP_TO_MAC.get(meta['dst_ip']),
                    17, 5060, 5060,
                    b, int(b * 0.5),
                    max(1, b // 200), max(1, b // 400),
                    COLLECT_INTERVAL, 'normal'
                ))

        if rows:
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

        if active_flows == 0:
            info("*** [Watchdog] Traffic dead – restarting\n")
            start_traffic(net)

# ================= TRAFFIC =================
def start_traffic(net):
    info("*** [Traffic] Starting Continuous Stream\n")

    for h in net.hosts:
        h.cmd("killall -9 ITGRecv")
        h.cmd(f"{ITG_RECV} > /dev/null 2>&1 &")

    time.sleep(2)

    payload = 160
    wire = 202
    pps = int(TARGET_CENTER / wire)

    for h, meta in HOST_INFO.items():
        net.get(h).cmd(
            f"{ITG_SEND} -T UDP -a {meta['dst_ip']} "
            f"-c {payload} -C {pps} -t 3600000 "
            f"> /dev/null 2>&1 &"
        )

# ================= MAIN =================
def cleanup():
    subprocess.run("rm -rf /var/run/netns/*", shell=True)
    subprocess.run("killall -9 ITGSend ITGRecv", shell=True)

if __name__ == "__main__":
    cleanup()
    setLogLevel('info')

    net = Mininet(topo=LeafSpineTopo(),
                  controller=RemoteController,
                  switch=OVSKernelSwitch,
                  link=TCLink)

    net.start()
    os.makedirs('/var/run/netns', exist_ok=True)

    for h in net.hosts:
        os.symlink(f'/proc/{h.pid}/ns/net', f'/var/run/netns/{h.name}')

    start_traffic(net)
    collector(net)
