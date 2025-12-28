#!/usr/bin/python3
"""
FIXED VOIP SIMULATION (BYTE-TREND EDITION)

Target:
1. bytes_tx per interval: 13.000 – 19.800
2. Pola naik–turun smooth (sinusoidal)
3. Perioda ±1 jam
4. Collector tetap hitung BPS dari delta bytes
"""

import threading
import time
import random
import os
import sys
import subprocess
import re
import math
import psycopg2
from datetime import datetime

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= 1. KONFIGURASI =================
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1

HOST_INFO = {
    'user1': {'ip': '192.168.100.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.10.1.1', 'label': 'normal'},
    'user2': {'ip': '192.168.100.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.10.2.1', 'label': 'normal'},
    'user3': {'ip': '192.168.100.3', 'mac': '00:00:00:00:01:03', 'dst_ip': '10.10.1.2', 'label': 'normal'},
    'web1':  {'ip': '10.10.1.1',      'mac': '00:00:00:00:0A:01', 'dst_ip': '192.168.100.1', 'label': 'normal'},
    'web2':  {'ip': '10.10.1.2',      'mac': '00:00:00:00:0A:02', 'dst_ip': '192.168.100.3', 'label': 'normal'},
    'cache1':{'ip': '10.10.1.3',      'mac': '00:00:00:00:0A:03', 'dst_ip': '10.10.2.2',     'label': 'normal'},
    'app1':  {'ip': '10.10.2.1',      'mac': '00:00:00:00:0B:01', 'dst_ip': '10.10.2.2',     'label': 'normal'},
    'db1':   {'ip': '10.10.2.2',      'mac': '00:00:00:00:0B:02', 'dst_ip': '10.10.2.1',     'label': 'normal'},
}

stop_event = threading.Event()
cmd_lock = threading.Lock()

# ================= 2. TOPOLOGI =================
class LinuxRouter(Node):
    def config(self, **params):
        super().config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl -w net.ipv4.ip_forward=0')
        super().terminate()

class DataCenterTopo(Topo):
    def build(self):
        info("*** Building Data Center Topology\n")

        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        ext_sw = self.addSwitch('ext_sw', dpid='1')
        tor1 = self.addSwitch('tor1', dpid='2')
        tor2 = self.addSwitch('tor2', dpid='3')

        user1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        user2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        user3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')

        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')

        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        self.addLink(user1, ext_sw, bw=100)
        self.addLink(user2, ext_sw, bw=100)
        self.addLink(user3, ext_sw, bw=100)

        self.addLink(web1, tor1, bw=100)
        self.addLink(web2, tor1, bw=100)
        self.addLink(cache1, tor1, bw=100)

        self.addLink(app1, tor2, bw=100)
        self.addLink(db1, tor2, bw=100)

        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ================= 3. COLLECTOR =================
def run_ns_cmd(host, cmd):
    try:
        return subprocess.run(
            ['sudo', 'ip', 'netns', 'exec', host] + cmd,
            capture_output=True, text=True, timeout=1
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
            'rx_bytes': int(rx.group(1)),
            'tx_bytes': int(tx.group(1)),
            'time': time.time()
        }
    return None

def collector_thread():
    info("*** [Collector] Started\n")
    conn = psycopg2.connect(DB_CONN)
    last = {}

    while not stop_event.is_set():
        time.sleep(COLLECT_INTERVAL)
        rows = []
        now = datetime.now()

        for h, meta in HOST_INFO.items():
            curr = get_stats(h)
            if not curr:
                continue

            if h not in last:
                last[h] = curr
                continue

            prev = last[h]
            dtx = curr['tx_bytes'] - prev['tx_bytes']
            drx = curr['rx_bytes'] - prev['rx_bytes']
            dt = curr['time'] - prev['time']
            if dt <= 0:
                dt = 1

            bps = (dtx * 8) / dt

            label = "idle"
            if 13000 <= dtx <= 19800:
                label = "normal"
            elif dtx > 19800:
                label = "congestion"
            elif dtx > 0:
                label = "background"

            if dtx > 0 or drx > 0:
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'],
                             'FF:FF:FF:FF:FF:FF', 17, 5060, 5060,
                             dtx, drx, 0, 0, COLLECT_INTERVAL, label))

            last[h] = curr

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

# ================= 4. TRAFFIC GENERATOR (BYTE-TREND) =================
def safe_cmd(node, cmd):
    if stop_event.is_set():
        return
    with cmd_lock:
        node.cmd(cmd)

def run_voip_traffic(net):
    info("*** [Traffic] BYTE-TREND 13k–19.8k (1 hour wave)\n")

    receivers = set(m['dst_ip'] for m in HOST_INFO.values())
    for h in net.hosts:
        if h.IP() in receivers:
            safe_cmd(h, "killall ITGRecv")
            safe_cmd(h, "ITGRecv &")

    time.sleep(3)

    BYTES_MIN = 13000
    BYTES_MAX = 19800
    MID = (BYTES_MIN + BYTES_MAX) / 2
    AMP = (BYTES_MAX - BYTES_MIN) / 2
    PERIOD = 3600
    NOISE = 400

    start = time.time()

    while not stop_event.is_set():
        t = time.time() - start
        base = MID + AMP * math.sin(2 * math.pi * t / PERIOD)

        for h, meta in HOST_INFO.items():
            node = net.get(h)
            target_bytes = int(max(BYTES_MIN,
                               min(BYTES_MAX, base + random.randint(-NOISE, NOISE))))
            pkt_size = random.randint(200, 400)
            rate = max(1, int(target_bytes / pkt_size))

            cmd = (
                f"ITGSend -T UDP -a {meta['dst_ip']} "
                f"-c {pkt_size} -C {rate} -t 1000 "
                f"> /dev/null 2>&1 &"
            )
            safe_cmd(node, cmd)

        time.sleep(1)

# ================= 5. MAIN =================
def setup_namespaces(net):
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'])
    for h in net.hosts:
        subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)])

def cleanup():
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True)
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True)

if __name__ == "__main__":
    setLogLevel('info')
    topo = DataCenterTopo()
    net = Mininet(topo=topo, controller=RemoteController,
                  switch=OVSKernelSwitch, link=TCLink)

    try:
        net.start()
        setup_namespaces(net)
        time.sleep(5)

        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        run_voip_traffic(net)

    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        cleanup()
        net.stop()
