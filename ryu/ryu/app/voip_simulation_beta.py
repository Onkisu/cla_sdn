#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (BYTES EDITION)
Topology: 3 Spines, 3 Leaves (Full Mesh L2/L3)
Target: 
 1. Throughput target dalam BYTES per Second.
 2. Range: 13,000 - 19,800 Bytes/sec.
 3. Label: 'normal' jika masuk range tersebut.
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

# Range Target (BYTES per Second)
# Sesuai request: 13000 - 19800 Bytes
TARGET_BYTES_MIN = 13000 
TARGET_BYTES_MAX = 19800

# Mapping Host agar tersebar di Leaf yang berbeda
# L1: user1, user2
# L2: web1, web2
# L3: app1, db1
HOST_INFO = {
    # Leaf 1 Clients
    'user1': {'ip': '10.0.1.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.0.2.1', 'label': 'normal'},
    'user2': {'ip': '10.0.1.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.0.3.1', 'label': 'normal'},
    
    # Leaf 2 Servers
    'web1':  {'ip': '10.0.2.1', 'mac': '00:00:00:00:02:01', 'dst_ip': '10.0.1.1', 'label': 'normal'},
    'web2':  {'ip': '10.0.2.2', 'mac': '00:00:00:00:02:02', 'dst_ip': '10.0.3.2', 'label': 'normal'},
    
    # Leaf 3 DB/App
    'app1':  {'ip': '10.0.3.1', 'mac': '00:00:00:00:03:01', 'dst_ip': '10.0.2.2', 'label': 'normal'},
    'db1':   {'ip': '10.0.3.2', 'mac': '00:00:00:00:03:02', 'dst_ip': '10.0.1.2', 'label': 'normal'},
}

stop_event = threading.Event()
cmd_lock = threading.Lock()

# ================= 2. TOPOLOGI LEAF-SPINE =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Membangun Topologi 3-Leaf 3-Spine...\n")
        
        # --- Create Spines ---
        spines = []
        for i in range(1, 4):
            spines.append(self.addSwitch(f'spine{i}', dpid=f'10{i}'))

        # --- Create Leaves ---
        leaves = []
        for i in range(1, 4):
            leaves.append(self.addSwitch(f'leaf{i}', dpid=f'20{i}'))

        # --- Interconnect Spines and Leaves (Full Mesh) ---
        # Setiap Leaf connect ke SEMUA Spine
        for leaf in leaves:
            for spine in spines:
                # BW besar di backbone biar tidak bottleneck
                self.addLink(leaf, spine, bw=1000) 

        # --- Connect Hosts to Leaves ---
        # Leaf 1: Users
        h_user1 = self.addHost('user1', ip='10.0.1.1/24', mac='00:00:00:00:01:01')
        h_user2 = self.addHost('user2', ip='10.0.1.2/24', mac='00:00:00:00:01:02')
        self.addLink(h_user1, leaves[0], bw=100)
        self.addLink(h_user2, leaves[0], bw=100)

        # Leaf 2: Web
        h_web1 = self.addHost('web1', ip='10.0.2.1/24', mac='00:00:00:00:02:01')
        h_web2 = self.addHost('web2', ip='10.0.2.2/24', mac='00:00:00:00:02:02')
        self.addLink(h_web1, leaves[1], bw=100)
        self.addLink(h_web2, leaves[1], bw=100)

        # Leaf 3: App/DB
        h_app1 = self.addHost('app1', ip='10.0.3.1/24', mac='00:00:00:00:03:01')
        h_db1 = self.addHost('db1', ip='10.0.3.2/24', mac='00:00:00:00:03:02')
        self.addLink(h_app1, leaves[2], bw=100)
        self.addLink(h_db1, leaves[2], bw=100)

# ================= 3. COLLECTOR (DATABASE BYTES) =================
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
    info("*** [Collector] Monitoring Started. Storing as BYTES/SEC.\n")
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

            # --- PERHITUNGAN BYTES PER SECOND ---
            # Kita menggunakan total bytes (TX + RX) atau salah satu tergantung perspektif.
            # Di sini kita hitung Bytes rate murni dari TX (yang dikirim host ini).
            bytes_sec_tx = dtx_bytes / t_diff
            
            last_stats[h_name] = curr
            
            # --- LABELING LOGIC (BYTES) ---
            # Range Normal: 13,000 - 19,800 Bytes/sec
            # Toleransi jitter +/- 1000 bytes
            
            label = "idle"
            
            if bytes_sec_tx > 25000:
                label = "burst" # Terlalu tinggi
            elif (TARGET_BYTES_MIN - 2000) <= bytes_sec_tx <= (TARGET_BYTES_MAX + 2000):
                label = "normal"     # Sesuai Target Dataset
            elif bytes_sec_tx > 500:
                label = "background"
            else:
                label = "idle"

            # Insert ke DB jika ada aktivitas
            if dtx_bytes > 0 or drx_bytes > 0:
                # Kolom 'bytes_tx' adalah representasi Length dalam dataset (Total Length)
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], 'FF:FF:FF:FF:FF:FF',
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
                # info(f"DB Error: {e}\n")
    
    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR (BYTES TARGET) =================
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
            safe_cmd(h, "killall ITGRecv")
            safe_cmd(h, "ITGRecv &")
    
    time.sleep(3)

    while not stop_event.is_set():
        
        for h_name, meta in HOST_INFO.items():
            node = net.get(h_name)
            
            # --- FORMULA BYTES ---
            # 1. Tentukan Target Bytes acak di range 13000 - 19800
            target_bytes_sec = random.randint(TARGET_BYTES_MIN, TARGET_BYTES_MAX)
            
            # 2. Random Packet Size (Payload Bytes)
            # Agar terlihat natural, packet size antara 100 - 300 bytes
            pkt_size_bytes = random.randint(100, 300)
            
            # 3. Hitung Rate (Packets Per Second) agar mencapai Target Bytes
            # Total Bytes = Rate (pkt/s) * Size (bytes)
            # Rate = Total Bytes / Size
            rate_pps = int(target_bytes_sec / pkt_size_bytes)
            
            # Safety check
            if rate_pps < 1: rate_pps = 1
            
            # 4. Kirim Command
            # -c : packet payload size (bytes)
            # -C : constant rate (packets per second)
            # -t : duration (ms) -> 1000ms = 1 detik refresh
            cmd = f"ITGSend -T UDP -a {meta['dst_ip']} -c {pkt_size_bytes} -C {rate_pps} -t 1000 > /dev/null 2>&1 &"
            safe_cmd(node, cmd)
        
        # Loop setiap 1 detik agar data bytes per detik akurat
        time.sleep(1.0)

# ================= 5. MAIN =================
# ================= 5. MAIN (UPDATED LOGIC) =================
def setup_network_config(net):
    info("*** Configuring Hosts (IP /8 & Static ARP)...\n")
    
    # 1. SETUP IP ADDRESS (/8 FLATTENED NETWORK)
    # Kita flush IP bawaan Mininet, dan pasang IP manual dengan netmask /8
    for h_name, meta in HOST_INFO.items():
        h = net.get(h_name)
        # Flush konfigurasi lama
        h.cmd(f"ip addr flush dev {h.name}-eth0")
        # Set IP baru dengan /8
        h.cmd(f"ip addr add {meta['ip']}/8 brd 10.255.255.255 dev {h.name}-eth0")
        h.cmd(f"ip link set dev {h.name}-eth0 up")
    
    # 2. SETUP STATIC ARP MANUAL
    # Karena 'net.staticArp()' bisa hilang saat flush IP, kita isi manual satu per satu.
    # Setiap host akan dipaksa "kenal" dengan host lain.
    for src_name, src_meta in HOST_INFO.items():
        src_node = net.get(src_name)
        for dst_name, dst_meta in HOST_INFO.items():
            if src_name != dst_name:
                # Command: arp -s <IP_TUJUAN> <MAC_TUJUAN>
                src_node.cmd(f"arp -s {dst_meta['ip']} {dst_meta['mac']}")

    # Verifikasi satu host untuk memastikan ARP masuk
    info("*** Verifying ARP on user1:\n")
    print(net.get('user1').cmd("arp -n"))

def cleanup():
    info("*** Cleaning up...\n")
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)
    # Hapus namespace sisa jika ada
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    setLogLevel('info')
    topo = LeafSpineTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        
        # Setup Namespaces untuk monitoring (wajib untuk collector)
        # Ini membuat symlink agar 'ip netns exec' bisa jalan
        subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
        for h in net.hosts:
            pid = h.pid
            h_name = h.name
            if not os.path.exists(f"/var/run/netns/{h_name}"):
                 subprocess.run(['sudo', 'ln', '-s', f'/proc/{pid}/ns/net', f'/var/run/netns/{h_name}'], check=False)

        # Konfigurasi IP dan ARP
        setup_network_config(net)

        info("*** Network Ready. Waiting 5s for stability...\n")
        time.sleep(5)
        
        # Jalankan Collector
        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        # Jalankan Traffic
        run_voip_traffic(net)

    except KeyboardInterrupt:
        info("\n*** Stopped.\n")
    finally:
        stop_event.set()
        cleanup()
        net.stop()