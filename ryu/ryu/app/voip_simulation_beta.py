#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (DEBUGGED)
Topology: 3 Spines, 3 Leaves (Full Mesh L2)
Fixes: Static ARP, Correct DB Logging, ITG Debugging
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
from mininet.node import RemoteController, OVSKernelSwitch, Host
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI

# ================= 1. KONFIGURASI =================
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1
TARGET_BYTES_MIN = 13000 
TARGET_BYTES_MAX = 19800

# Helper untuk mencari MAC berdasarkan IP
IP_TO_MAC = {} # Akan diisi otomatis

HOST_INFO = {
    'user1': {'ip': '10.0.1.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.0.2.1', 'label': 'normal'},
    'user2': {'ip': '10.0.1.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.0.3.1', 'label': 'normal'},
    'web1':  {'ip': '10.0.2.1', 'mac': '00:00:00:00:02:01', 'dst_ip': '10.0.1.1', 'label': 'normal'},
    'web2':  {'ip': '10.0.2.2', 'mac': '00:00:00:00:02:02', 'dst_ip': '10.0.3.2', 'label': 'normal'},
    'app1':  {'ip': '10.0.3.1', 'mac': '00:00:00:00:03:01', 'dst_ip': '10.0.2.2', 'label': 'normal'},
    'db1':   {'ip': '10.0.3.2', 'mac': '00:00:00:00:03:02', 'dst_ip': '10.0.1.2', 'label': 'normal'},
}

# Isi IP_TO_MAC map
for h, data in HOST_INFO.items():
    IP_TO_MAC[data['ip']] = data['mac']

stop_event = threading.Event()
cmd_lock = threading.Lock()

# ================= 2. TOPOLOGI =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Building Topo: 3-Leaf 3-Spine (Full Mesh)...\n")
        spines = [self.addSwitch(f'spine{i}', dpid=f'10{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}', dpid=f'20{i}') for i in range(1, 4)]

        # Interconnect (1Gbps)
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000) 

        # Hosts (100Mbps)
        # Leaf 1
        self.addLink(self.addHost('user1', ip='10.0.1.1/8', mac='00:00:00:00:01:01'), leaves[0], bw=100)
        self.addLink(self.addHost('user2', ip='10.0.1.2/8', mac='00:00:00:00:01:02'), leaves[0], bw=100)
        # Leaf 2
        self.addLink(self.addHost('web1', ip='10.0.2.1/8', mac='00:00:00:00:02:01'), leaves[1], bw=100)
        self.addLink(self.addHost('web2', ip='10.0.2.2/8', mac='00:00:00:00:02:02'), leaves[1], bw=100)
        # Leaf 3
        self.addLink(self.addHost('app1', ip='10.0.3.1/8', mac='00:00:00:00:03:01'), leaves[2], bw=100)
        self.addLink(self.addHost('db1', ip='10.0.3.2/8', mac='00:00:00:00:03:02'), leaves[2], bw=100)

# ================= 3. COLLECTOR (FIXED MAC LOGGING) =================
def run_ns_cmd(host_name, cmd):
    if not os.path.exists(f"/var/run/netns/{host_name}"): return None
    try:
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host_name] + cmd, 
                            capture_output=True, text=True, timeout=1.0).stdout
    except: return None

def get_stats(host_name):
    intf = f"{host_name}-eth0"
    out = run_ns_cmd(host_name, ['ip', '-s', 'link', 'show', intf])
    if not out: return None
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    if rx and tx:
        return {
            'rx_bytes': int(rx.group(1)), 'rx_pkts': int(rx.group(2)),
            'tx_bytes': int(tx.group(1)), 'tx_pkts': int(tx.group(2)),
            'time': time.time()
        }
    return None

def collector_thread():
    info("*** [Collector] Monitoring Started.\n")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONN)
    except Exception as e:
        info(f"!!! [Collector] DB Error: {e}\n")

    last_stats = {}
    
    while not stop_event.is_set():
        time.sleep(COLLECT_INTERVAL)
        rows = []
        now = datetime.now()

        for h_name, meta in HOST_INFO.items():
            curr = get_stats(h_name)
            if not curr: continue

            if h_name not in last_stats:
                last_stats[h_name] = curr
                continue

            prev = last_stats[h_name]
            dtx_bytes = curr['tx_bytes'] - prev['tx_bytes']
            drx_bytes = curr['rx_bytes'] - prev['rx_bytes']
            dtx_pkts = curr['tx_pkts'] - prev['tx_pkts']
            drx_pkts = curr['rx_pkts'] - prev['rx_pkts']
            
            t_diff = curr['time'] - prev['time']
            if t_diff <= 0: t_diff = 1.0

            bytes_sec_tx = dtx_bytes / t_diff
            
            # --- LABELING ---
            label = "idle"
            if bytes_sec_tx > 25000: label = "burst"
            elif (TARGET_BYTES_MIN - 2000) <= bytes_sec_tx <= (TARGET_BYTES_MAX + 2000): label = "normal"
            elif bytes_sec_tx > 500: label = "background"

            last_stats[h_name] = curr

            if dtx_bytes > 0 or drx_bytes > 0:
                # FIX: Ambil MAC tujuan yang benar dari map IP_TO_MAC
                real_dst_mac = IP_TO_MAC.get(meta['dst_ip'], '00:00:00:00:00:00')
                
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], real_dst_mac,
                             17, 5060, 5060, dtx_bytes, drx_bytes, dtx_pkts, drx_pkts, 
                             COLLECT_INTERVAL, label))

        if rows and conn and not conn.closed:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec , traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except Exception as e: 
                conn.rollback()
    
    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR (DEBUG MODE) =================
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    with cmd_lock:
        try: node.cmd(cmd)
        except: pass

def run_voip_traffic(net):
    info(f"*** [Traffic] Generating Strict {TARGET_BYTES_MIN} - {TARGET_BYTES_MAX} BYTES/sec...\n")
    
    # Start Receivers
    receivers = set()
    for meta in HOST_INFO.values(): receivers.add(meta['dst_ip'])
    
    for h in net.hosts:
        if h.IP() in receivers:
            h.cmd("killall ITGRecv")
            h.cmd("ITGRecv &")
    
    time.sleep(2) # Beri waktu Recv untuk siap

    while not stop_event.is_set():
        for h_name, meta in HOST_INFO.items():
            node = net.get(h_name)
            
            # Perhitungan Bytes
            target_bytes_sec = random.randint(TARGET_BYTES_MIN, TARGET_BYTES_MAX)
            pkt_size_bytes = random.randint(100, 300)
            rate_pps = int(target_bytes_sec / pkt_size_bytes)
            if rate_pps < 1: rate_pps = 1
            
            # --- FIX: LOGGING ERROR D-ITG ---
            # Kita arahkan output ke /tmp/itg_<hostname>.log untuk debug
            # Hapus '> /dev/null' agar kita bisa lihat jika ada error
            cmd = (f"ITGSend -T UDP -a {meta['dst_ip']} -c {pkt_size_bytes} "
                   f"-C {rate_pps} -t 1000 > /tmp/itg_{h_name}.log 2>&1 &")
            
            safe_cmd(node, cmd)
        
        time.sleep(1.0)

# ================= 5. SETUP & MAIN =================
def setup_network_config(net):
    info("*** Configuring Hosts (IP /8 & Static ARP)...\n")
    # Flush IP & Set /8
    for h_name, meta in HOST_INFO.items():
        h = net.get(h_name)
        h.cmd(f"ip addr flush dev {h.name}-eth0")
        h.cmd(f"ip addr add {meta['ip']}/8 brd 10.255.255.255 dev {h.name}-eth0")
        h.cmd(f"ip link set dev {h.name}-eth0 up")
    
    # Static ARP Manual
    for src_name, src_meta in HOST_INFO.items():
        src_node = net.get(src_name)
        for dst_name, dst_meta in HOST_INFO.items():
            if src_name != dst_name:
                src_node.cmd(f"arp -s {dst_meta['ip']} {dst_meta['mac']}")

    # Verifikasi
    info("*** Verifying ARP on user1:\n")
    print(net.get('user1').cmd("arp -n"))

def cleanup():
    info("*** Cleaning up...\n")
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    setLogLevel('info')
    topo = LeafSpineTopo()
    # Pastikan controller Ryu sudah jalan sebelum ini
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        
        # Setup Namespace symlink
        subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
        for h in net.hosts:
            if not os.path.exists(f"/var/run/netns/{h.name}"):
                 subprocess.run(['sudo', 'ln', '-s', f'/proc/{h.pid}/ns/net', f'/var/run/netns/{h.name}'], check=False)

        setup_network_config(net)

        info("*** Testing Connectivity (Ping All)...\n")
        # --- FIX: WAJIB PingAll ---
        # Kalau PingAll gagal (X), jangan harap Traffic jalan.
        # Ini juga memancing Controller untuk belajar path (KSP)
        drop = net.pingAll()
        if drop > 0:
            info("!!! WARNING: Some pings failed. Controller might be struggling.\n")
        else:
            info("*** Connectivity Excellent.\n")
        
        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        run_voip_traffic(net)

    except KeyboardInterrupt:
        info("\n*** Stopped.\n")
    finally:
        stop_event.set()
        cleanup()
        net.stop()