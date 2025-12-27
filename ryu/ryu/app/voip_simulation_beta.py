#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (SAPU JAGAT EDITION)
Fitur:
1. Auto-Clean Namespace (Anti Error 'File Exists').
2. D-ITG Auto-Path Detection.
3. Full Logging ke /tmp/ jika error.
4. Target Bytes: 13.000 - 19.800 Bytes/sec.
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
COLLECT_INTERVAL = 1

# Target Throughput (Bytes/sec)
TARGET_BYTES_MIN = 13000 
TARGET_BYTES_MAX = 19800

# Host Definition
HOST_INFO = {
    'user1': {'ip': '10.0.1.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.0.2.1'},
    'user2': {'ip': '10.0.1.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.0.3.1'},
    'web1':  {'ip': '10.0.2.1', 'mac': '00:00:00:00:02:01', 'dst_ip': '10.0.1.1'},
    'web2':  {'ip': '10.0.2.2', 'mac': '00:00:00:00:02:02', 'dst_ip': '10.0.3.2'},
    'app1':  {'ip': '10.0.3.1', 'mac': '00:00:00:00:03:01', 'dst_ip': '10.0.2.2'},
    'db1':   {'ip': '10.0.3.2', 'mac': '00:00:00:00:03:02', 'dst_ip': '10.0.1.2'},
}

# Helper Map untuk Collector
IP_TO_MAC = {v['ip']: v['mac'] for k, v in HOST_INFO.items()}

stop_event = threading.Event()

# Auto-Detect ITG Binary
ITG_SEND_BIN = shutil.which("ITGSend") or "/usr/local/bin/ITGSend"
ITG_RECV_BIN = shutil.which("ITGRecv") or "/usr/local/bin/ITGRecv"

# ================= 2. TOPOLOGI =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Building Topo: 3-Leaf 3-Spine (Full Mesh)...\n")
        spines = [self.addSwitch(f'spine{i}', dpid=f'10{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}', dpid=f'20{i}') for i in range(1, 4)]

        # Backbone Links (1Gbps)
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000) 

        # Host Links (100Mbps)
        # Leaf 1
        self.addLink(self.addHost('user1', ip='10.0.1.1/8', mac='00:00:00:00:01:01'), leaves[0], bw=100)
        self.addLink(self.addHost('user2', ip='10.0.1.2/8', mac='00:00:00:00:01:02'), leaves[0], bw=100)
        # Leaf 2
        self.addLink(self.addHost('web1', ip='10.0.2.1/8', mac='00:00:00:00:02:01'), leaves[1], bw=100)
        self.addLink(self.addHost('web2', ip='10.0.2.2/8', mac='00:00:00:00:02:02'), leaves[1], bw=100)
        # Leaf 3
        self.addLink(self.addHost('app1', ip='10.0.3.1/8', mac='00:00:00:00:03:01'), leaves[2], bw=100)
        self.addLink(self.addHost('db1', ip='10.0.3.2/8', mac='00:00:00:00:03:02'), leaves[2], bw=100)

# ================= 3. COLLECTOR =================
def run_ns_cmd(host_name, cmd):
    # Cek apakah namespace file ada
    if not os.path.exists(f"/var/run/netns/{host_name}"): return None
    try:
        # Timeout cepat agar tidak memblokir loop
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host_name] + cmd, 
                            capture_output=True, text=True, timeout=0.2).stdout
    except: return None

def get_stats(host_name):
    # Ambil statistik TX/RX dari interface host
    out = run_ns_cmd(host_name, ['ip', '-s', 'link', 'show', f"{host_name}-eth0"])
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
    info("*** [Collector] Monitoring Started...\n")
    conn = None
    try: 
        conn = psycopg2.connect(DB_CONN)
    except Exception as e:
        info(f"!!! [Collector] DB Connection Failed: {e}\n")
        # Kita tidak return, agar simulasi tetap jalan walau DB mati (untuk debug)
    
    last_stats = {}
    while not stop_event.is_set():
        time.sleep(COLLECT_INTERVAL)
        rows = []
        now = datetime.now()

        for h_name, meta in HOST_INFO.items():
            curr = get_stats(h_name)
            if not curr: continue # Skip jika gagal ambil stats

            if h_name not in last_stats:
                last_stats[h_name] = curr
                continue

            prev = last_stats[h_name]
            # Hitung Delta (Perubahan)
            dtx_bytes = curr['tx_bytes'] - prev['tx_bytes']
            drx_bytes = curr['rx_bytes'] - prev['rx_bytes']
            dtx_pkts = curr['tx_pkts'] - prev['tx_pkts']
            drx_pkts = curr['rx_pkts'] - prev['rx_pkts']
            
            # Labeling Logic (Simple)
            bytes_sec = dtx_bytes
            label = "idle"
            if bytes_sec > 25000: label = "burst"
            elif bytes_sec > 10000: label = "normal" # Toleransi bawah agak longgar
            elif bytes_sec > 500: label = "background"

            last_stats[h_name] = curr
            
            # Hanya simpan ke DB jika ada aktivitas traffic
            if dtx_bytes > 0 or drx_bytes > 0:
                dst_mac = IP_TO_MAC.get(meta['dst_ip'], '00:00:00:00:00:00')
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], dst_mac,
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
                # Uncomment di bawah ini jika ingin lihat error DB di terminal
                # info(f"DB Insert Error: {e}\n")

# ================= 4. TRAFFIC GENERATOR =================
def run_voip_traffic(net):
    info(f"*** [Traffic] Generating Traffic via D-ITG...\n")
    info(f"*** Binary Path: {ITG_SEND_BIN}\n")
    
    # 1. Start Receivers
    info("*** Starting Receivers (Logs at /tmp/itg_recv_*.log)...\n")
    for h in net.hosts:
        h.cmd("killall -9 ITGRecv")
        # Jalankan Receiver di background
        h.cmd(f"{ITG_RECV_BIN} > /tmp/itg_recv_{h.name}.log 2>&1 &")
    
    time.sleep(3) # Tunggu receiver siap

    while not stop_event.is_set():
        for h_name, meta in HOST_INFO.items():
            src_node = net.get(h_name)
            
            # --- FORMULA RATE ---
            # Kita ingin Bytes yang tercatat di interface (Wire Speed) = 13.000 - 19.800
            # Packet Size D-ITG (-c) adalah Payload.
            # Wire Size = Payload + 14(Eth) + 20(IP) + 8(UDP) = Payload + 42 bytes
            
            payload_size = 160 
            wire_packet_size = payload_size + 42
            
            target_bytes = random.randint(TARGET_BYTES_MIN, TARGET_BYTES_MAX)
            
            # Berapa paket per detik untuk mencapai target bytes?
            pps = int(target_bytes / wire_packet_size)
            
            # Boosting: Virtual environment sering loss atau delay,
            # Kita naikkan rate kirim 20% agar yang SAMPAI sesuai target.
            boosted_pps = int(pps * 1.2)

            # Command D-ITG
            # Output error dibuang ke file log spesifik agar bisa didebug
            cmd = (f"{ITG_SEND_BIN} -T UDP -a {meta['dst_ip']} "
                   f"-c {payload_size} "
                   f"-C {boosted_pps} "
                   f"-t 1000 > /tmp/itg_send_{h_name}.log 2>&1 &")
            
            try:
                src_node.cmd(cmd)
            except:
                pass
        
        # Loop setiap 1.1 detik
        time.sleep(1.1)

# ================= 5. MAIN SETUP =================
def force_cleanup():
    # Fungsi bersih-bersih brutal sebelum start
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)

def setup_network_config(net):
    # Set IP Flat /8 dan Static ARP
    info("*** Configuring Flat Network (IP /8 & Static ARP)...\n")
    for h in net.hosts:
        h.cmd(f"ip addr flush dev {h.name}-eth0")
        h.cmd(f"ip addr add {HOST_INFO[h.name]['ip']}/8 brd 10.255.255.255 dev {h.name}-eth0")
        h.cmd(f"ip link set dev {h.name}-eth0 up")
    
    for src in net.hosts:
        src_meta = HOST_INFO[src.name]
        for dst_name, dst_meta in HOST_INFO.items():
            if src.name != dst_name:
                src.cmd(f"arp -s {dst_meta['ip']} {dst_meta['mac']}")

def wait_for_convergence(net):
    info("*** WARMUP: Pinging to ensure Topology Discovery...\n")
    # PingAll berkali-kali sampai loss 0%
    for i in range(1, 6):
        loss = net.pingAll()
        info(f"   Run {i}: {loss}% Dropped\n")
        if loss == 0.0:
            info("   >>> Network READY! <<<\n")
            return True
        time.sleep(2)
    return False

if __name__ == "__main__":
    force_cleanup() # BERSIHKAN SEBELUM MULAI
    
    setLogLevel('info')
    topo = LeafSpineTopo()
    # Pastikan Remote Controller aktif
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        
        # Buat Namespace Symlink (Wajib untuk Collector)
        subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
        for h in net.hosts:
            if not os.path.exists(f"/var/run/netns/{h.name}"):
                 subprocess.run(['sudo', 'ln', '-s', f'/proc/{h.pid}/ns/net', f'/var/run/netns/{h.name}'], check=False)

        setup_network_config(net)
        
        info("*** Waiting 5s for Controller (LLDP)...\n")
        time.sleep(5)
        
        # Wajib Converge dulu baru jalan traffic
        if wait_for_convergence(net):
            t_col = threading.Thread(target=collector_thread)
            t_col.start()
            run_voip_traffic(net)
        else:
            info("!!! CRITICAL: Network Failed to Converge. Check Controller/Ryu.\n")
            info("!!! Hint: Pastikan 'voip_controller_ksp.py' jalan dengan '--observe-links'\n")

    except KeyboardInterrupt:
        info("\n*** Stopped by User.\n")
    except Exception as e:
        info(f"\n!!! Crash: {e}\n")
    finally:
        stop_event.set()
        force_cleanup()
        net.stop()