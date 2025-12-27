#!/usr/bin/python3
"""
ALL-IN-ONE SIMULATION: 3-Spine, 3-Leaf, 3-Host (VoIP Traffic)
Fitur:
1. Topologi Leaf-Spine (dengan STP aktif untuk mencegah loop).
2. Traffic Generator D-ITG khusus VoIP (UDP, packet size 60-220 bytes).
3. Data Collector ke PostgreSQL.
"""

import threading
import time
import random
import os
import sys
import subprocess
import re
import psycopg2
from collections import defaultdict
from datetime import datetime

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI

# ================= CONFIGURATION =================
# Database Config
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1  # Detik

# Traffic Config
VOIP_MIN_BYTES = 60
VOIP_MAX_BYTES = 220
TRAFFIC_DURATION = 300 # Durasi simulasi dalam detik (misal 5 menit)

# Host Definitions (3 Host sesuai request)
# Kita gunakan flat network 10.0.0.x agar switch L2 mudah bekerja
HOST_CONF = {
    'h1': {'ip': '10.0.0.1', 'mac': '00:00:00:00:00:01', 'leaf': 'leaf1', 'dst': '10.0.0.2'}, # VoIP User A
    'h2': {'ip': '10.0.0.2', 'mac': '00:00:00:00:00:02', 'leaf': 'leaf2', 'dst': '10.0.0.3'}, # VoIP User B
    'h3': {'ip': '10.0.0.3', 'mac': '00:00:00:00:00:03', 'leaf': 'leaf3', 'dst': '10.0.0.1'}  # Server/User C
}

stop_event = threading.Event()

# ================= TOPOLOGY: 3 SPINE, 3 LEAF =================
class LeafSpineTopo(Topo):
    """3 Spines, 3 Leaves, 3 Hosts Topology"""
    def build(self):
        spines = []
        leaves = []

        # Create 3 Spines
        for i in range(1, 4):
            spines.append(self.addSwitch(f'spine{i}', dpid=f'10{i}'))

        # Create 3 Leaves
        for i in range(1, 4):
            leaves.append(self.addSwitch(f'leaf{i}', dpid=f'20{i}'))

        # Create Links: Full Mesh antara Leaf dan Spine
        # Bandwidth Spine-Leaf besar (1Gbps), Latency rendah
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=100, delay='1ms')

        # Create Hosts and connect to specific Leaves
        # Bandwidth Host-Leaf standar (100Mbps)
        for h_name, conf in HOST_CONF.items():
            host = self.addHost(h_name, ip=f"{conf['ip']}/24", mac=conf['mac'])
            self.addLink(host, conf['leaf'], bw=100, delay='5ms')

# ================= UTILS & COLLECTOR =================
def run_ns_cmd(host, cmd):
    """Menjalankan perintah di dalam namespace host"""
    if not os.path.exists(f"/var/run/netns/{host}"): return None
    try:
        # Timeout agresif agar collector tidak lag
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host] + cmd, 
                            capture_output=True, text=True, timeout=0.5).stdout
    except: return None

def get_stats(host_name):
    """Mengambil statistik RX/TX dari interface host"""
    # Coba interface standar 'h1-eth0'
    intf = f"{host_name}-eth0"
    out = run_ns_cmd(host_name, ['ip', '-s', 'link', 'show', intf])
    
    if not out: return None
    
    # Regex untuk parse output 'ip -s link'
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
    info(f"*** [Collector] Started. Interval: {COLLECT_INTERVAL}s\n")
    
    # Cache statistik sebelumnya untuk menghitung delta
    last_stats = defaultdict(lambda: {'tx_bytes': 0, 'rx_bytes': 0, 'time': 0})
    
    # Init Baseline
    for h in HOST_CONF.keys():
        s = get_stats(h)
        if s: last_stats[h] = s

    conn = None
    try:
        conn = psycopg2.connect(DB_CONN)
    except Exception as e:
        info(f"!!! [Collector] DB Connection Failed: {e}\n")
        # Kita lanjut jalan tapi tidak simpan ke DB (print only) agar simulasi tidak crash

    while not stop_event.is_set():
        time.sleep(COLLECT_INTERVAL)
        rows = []
        now = datetime.now()

        for h_name, conf in HOST_CONF.items():
            curr = get_stats(h_name)
            if not curr: continue
            
            prev = last_stats[h_name]
            
            # Hitung Delta
            dtx_bytes = curr['tx_bytes'] - prev['tx_bytes']
            drx_bytes = curr['rx_bytes'] - prev['rx_bytes']
            dtx_pkts = curr['tx_pkts'] - prev['tx_pkts'] # Jika perlu
            
            time_diff = curr['time'] - prev['time']
            if time_diff <= 0: time_diff = 1.0 # Hindari divide by zero

            # Hitung Throughput (Mbps)
            # (Bytes * 8) / (seconds * 1_000_000)
            throughput = (dtx_bytes * 8) / (time_diff * 1_000_000.0)
            
            # Update cache
            last_stats[h_name] = curr

            # Logic Labeling Sederhana
            label = 'voip_idle'
            if throughput > 0.01: # Jika ada traffic
                label = 'voip_active'
            if throughput > 5.0:  # Threshold anomali/burst
                label = 'voip_congestion'

            # Siapkan data row
            # Mapping: timestamp, dpid(fake), src, dst, mac_src, mac_dst, proto(17=UDP), tp_src, tp_dst
            # Untuk demo ini, dpid kita set 0 atau ambil dari topology jika mau kompleks
            if dtx_bytes > 0 or drx_bytes > 0:
                rows.append((
                    now, 0, conf['ip'], conf['dst'], 
                    conf['mac'], 'FF:FF:FF:FF:FF:FF', # Destination MAC idealnya dynamic
                    17, 5060, 5060, # UDP SIP ports
                    dtx_bytes, drx_bytes, 
                    curr['tx_pkts'], curr['rx_pkts'], 
                    COLLECT_INTERVAL, round(throughput, 4), label
                ))

        # Insert ke DB
        if rows and conn and not conn.closed:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (
                    timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                    ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                    duration_sec, throughput_mbps, traffic_label
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
                cur.close()
            except Exception as e:
                print(f"[DB Error] {e}")
                conn.rollback()

    if conn: conn.close()
    info("*** [Collector] Stopped.\n")

# ================= TRAFFIC GENERATOR =================
def traffic_generator(net):
    info("*** [Traffic] Starting VoIP Traffic Generation...\n")
    
    # 1. Start ITGRecv di semua host target
    for h_name in HOST_CONF:
        h = net.get(h_name)
        # Kill previous instances
        h.cmd("killall ITGRecv")
        h.cmd("ITGRecv &") # Run in background
        info(f"   -> {h_name} listening...\n")
    
    time.sleep(2) # Tunggu receiver siap

    # 2. Kirim Traffic Loop
    # Simulasi berjalan selama TRAFFIC_DURATION
    end_time = time.time() + TRAFFIC_DURATION
    
    while time.time() < end_time and not stop_event.is_set():
        for h_name, conf in HOST_CONF.items():
            src_node = net.get(h_name)
            dst_ip = conf['dst']
            
            # Random Packet Size untuk VoIP (60 - 220 bytes)
            pkt_size = random.randint(VOIP_MIN_BYTES, VOIP_MAX_BYTES)
            
            # Rate packet: VoIP G.711 ~ 50 pps (packets per second)
            # Duration: kirim burst kecil selama 2 detik, lalu sleep (simulasi talk spurts)
            
            cmd = (f"ITGSend -T UDP -a {dst_ip} -c {pkt_size} -C 50 -t 2000 "
                   f"-x {h_name}_log.log > /dev/null 2>&1 &")
            
            src_node.cmd(cmd)
            info(f"   [Flow] {h_name} -> {dst_ip} | Size: {pkt_size}B | Rate: 50pps\n")
        
        # Jeda antar talk-spurt (simulasi percakapan manusia ada jeda)
        time.sleep(3)

# ================= MAIN =================
def setup_namespaces(net):
    """Link Mininet NS ke /var/run/netns agar 'ip netns' command works"""
    info("*** Setup Namespaces for Collector...\n")
    # Buat folder jika belum ada
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    
    for h in net.hosts:
        ns_path = f"/var/run/netns/{h.name}"
        # 1. Hapus symlink lama jika ada (ini solusi error "File exists")
        if os.path.exists(ns_path):
            subprocess.run(['sudo', 'rm', '-f', ns_path], check=False)
            
        # 2. Buat symlink baru
        subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

def cleanup():
    info("*** Cleaning up processes...\n")
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    setLogLevel('info')
    
    # PENTING: stp=True pada switch OVS sangat krusial untuk Leaf-Spine
    # Jika False, paket akan loop selamanya karena ada multiple path
    topo = LeafSpineTopo()
    net = Mininet(topo=topo, 
                  controller=RemoteController, 
                  switch=lambda name, **opts: OVSKernelSwitch(name, stp=True, **opts),
                  link=TCLink)

    try:
        net.start()
        setup_namespaces(net)

        # Tunggu STP convergence (Spanning Tree butuh waktu ~30s untuk block port loop)
        info("*** Waiting 30s for STP Convergence (Important!)...\n")
        time.sleep(30)
        
        # Test Ping
        info("*** Testing Connectivity (Ping All)...\n")
        net.pingAll()

        # Start Collector Thread
        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        # Start Traffic
        traffic_generator(net)

        info("*** Simulation Completed. Stopping...\n")

    except KeyboardInterrupt:
        info("\n*** Interrupted by User.\n")
    except Exception as e:
        info(f"*** Error: {e}\n")
    finally:
        stop_event.set()
        cleanup()
        net.stop()