#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (SINE WAVE PATTERN)
Fitur:
1. SINE WAVE: Traffic naik turun otomatis (13k - 19.8k) setiap 60 detik.
2. NO GAPS: Menggunakan teknik Overlap (Jalan dulu, baru matiin).
3. RANDOM NOISE: Ada variasi acak kecil biar terlihat natural.
"""

import threading
import time
import random
import os
import subprocess
import re
import shutil
import psycopg2
import math
from datetime import datetime
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= 1. KONFIGURASI =================
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1.0

# CONFIG SINE WAVE
MIN_BYTES = 13000
MAX_BYTES = 19800
WAVE_PERIOD = 60  # Satu gelombang penuh (naik-puncak-turun-lembah) = 60 detik

HOST_INFO = {
    'user1': {'ip': '10.0.1.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.0.2.1'},
    'user2': {'ip': '10.0.1.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.0.3.1'},
    'web1':  {'ip': '10.0.2.1', 'mac': '00:00:00:00:02:01', 'dst_ip': '10.0.1.1'},
    'web2':  {'ip': '10.0.2.2', 'mac': '00:00:00:00:02:02', 'dst_ip': '10.0.3.2'},
    'app1':  {'ip': '10.0.3.1', 'mac': '00:00:00:00:03:01', 'dst_ip': '10.0.2.2'},
    'db1':   {'ip': '10.0.3.2', 'mac': '00:00:00:00:03:02', 'dst_ip': '10.0.1.2'},
}

IP_TO_MAC = {v['ip']: v['mac'] for k, v in HOST_INFO.items()}
stop_event = threading.Event()

ITG_SEND_BIN = shutil.which("ITGSend") or "/usr/local/bin/ITGSend"
ITG_RECV_BIN = shutil.which("ITGRecv") or "/usr/local/bin/ITGRecv"

# ================= 2. TOPOLOGI =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Building Topo...\n")
        spines = [self.addSwitch(f'spine{i}', dpid=f'10{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}', dpid=f'20{i}') for i in range(1, 4)]

        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000) 

        self.addLink(self.addHost('user1', ip='10.0.1.1/8', mac='00:00:00:00:01:01'), leaves[0], bw=100)
        self.addLink(self.addHost('user2', ip='10.0.1.2/8', mac='00:00:00:00:01:02'), leaves[0], bw=100)
        self.addLink(self.addHost('web1', ip='10.0.2.1/8', mac='00:00:00:00:02:01'), leaves[1], bw=100)
        self.addLink(self.addHost('web2', ip='10.0.2.2/8', mac='00:00:00:00:02:02'), leaves[1], bw=100)
        self.addLink(self.addHost('app1', ip='10.0.3.1/8', mac='00:00:00:00:03:01'), leaves[2], bw=100)
        self.addLink(self.addHost('db1', ip='10.0.3.2/8', mac='00:00:00:00:03:02'), leaves[2], bw=100)

# ================= 3. COLLECTOR (SIMPLE & ROBUST) =================
def run_ns_cmd(host_name, cmd):
    if not os.path.exists(f"/var/run/netns/{host_name}"): return None
    try:
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host_name] + cmd, 
                            capture_output=True, text=True, timeout=0.2).stdout
    except: return None

def get_stats(host_name):
    out = run_ns_cmd(host_name, ['ip', '-s', 'link', 'show', f"{host_name}-eth0"])
    if not out: return None
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    if rx and tx:
        return {'tx_bytes': int(tx.group(1)), 'rx_bytes': int(rx.group(1)), 
                'tx_pkts': int(tx.group(2)), 'rx_pkts': int(rx.group(2))}
    return None

def collector_thread():
    info("*** [Collector] STARTED. Saving Data...\n")
    conn = None
    try: conn = psycopg2.connect(DB_CONN)
    except: pass
    
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
            
            last_stats[h_name] = curr
            
            label = "normal"
            dst_mac = IP_TO_MAC.get(meta['dst_ip'], '00:00:00:00:00:00')
            
            # Filter noise kecil (< 500 bytes)
            if dtx_bytes > 500: 
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], dst_mac,
                                17, 5060, 5060, dtx_bytes, drx_bytes, dtx_pkts, drx_pkts, 
                                COLLECT_INTERVAL, label))

        if rows and conn:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec , traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except: conn.rollback()

# ================= 4. TRAFFIC GENERATOR (SINE WAVE) =================
def run_sine_wave_traffic(net):
    info(f"*** [Traffic] Starting SINE WAVE Pattern (Period: {WAVE_PERIOD}s)...\n")
    
    # 1. Start Receivers
    for h in net.hosts:
        h.cmd("killall -9 ITGRecv")
        h.cmd(f"{ITG_RECV_BIN} > /dev/null 2>&1 &")
    
    time.sleep(2)
    
    # Variables for Sine Wave
    center = (MAX_BYTES + MIN_BYTES) / 2
    amplitude = (MAX_BYTES - MIN_BYTES) / 2
    step_time = 0
    
    # UPDATE INTERVAL (Semakin kecil, semakin mulus gelombangnya)
    UPDATE_SEC = 2.0 
    
    while not stop_event.is_set():
        # A. HITUNG TARGET BYTES (Matematika Sinus)
        # Rumus: Center + Amp * sin(waktu) + Noise
        sine_val = math.sin(2 * math.pi * step_time / WAVE_PERIOD)
        noise = random.randint(-500, 500) # Biar gak kaku banget
        
        target_bytes = int(center + (amplitude * sine_val) + noise)
        
        # Clip (Jaga batas aman)
        target_bytes = max(MIN_BYTES, min(target_bytes, MAX_BYTES))
        
        # Hitung PPS
        wire_size = 202
        pps = int(target_bytes / wire_size)
        
        # Print info ke layar biar Anda bisa pantau polanya
        phase = "RISING" if sine_val > 0 else "FALLING"
        # info(f"   >>> Traffic Phase: {phase} | Target: {target_bytes} Bps ({pps} pps)\n")

        # B. EKSEKUSI (Teknik Overlap)
        # Kita set durasi ITG sedikit lebih lama dari loop sleep
        # Durasi ITG = 2500ms, Sleep Loop = 2000ms.
        # Ada overlap 500ms. Ini menjamin TIDAK ADA GAP.
        
        for h_name, meta in HOST_INFO.items():
            src_node = net.get(h_name)
            cmd = (f"{ITG_SEND_BIN} -T UDP -a {meta['dst_ip']} "
                   f"-c 160 "
                   f"-C {pps} "
                   f"-t {int(UPDATE_SEC * 1000) + 500} " # Overlap 500ms
                   f"> /dev/null 2>&1 &")
            try: src_node.cmd(cmd)
            except: pass
        
        time.sleep(UPDATE_SEC)
        step_time += UPDATE_SEC

# ================= 5. MAIN SETUP =================
def force_cleanup():
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)

def setup_network_config(net):
    info("*** Configuring Network...\n")
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
    info("*** WARMUP (Ping Check)...\n")
    for i in range(1, 6):
        loss = net.pingAll()
        if loss == 0.0:
            info("   >>> Network READY! <<<\n")
            return True
        time.sleep(2)
    return False

if __name__ == "__main__":
    force_cleanup()
    setLogLevel('info')
    topo = LeafSpineTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
        for h in net.hosts:
            if not os.path.exists(f"/var/run/netns/{h.name}"):
                 subprocess.run(['sudo', 'ln', '-s', f'/proc/{h.pid}/ns/net', f'/var/run/netns/{h.name}'], check=False)

        setup_network_config(net)
        time.sleep(5)
        
        if wait_for_convergence(net):
            # Jalanin Collector
            t_col = threading.Thread(target=collector_thread)
            t_col.start()
            
            # Jalanin Traffic Sine Wave
            run_sine_wave_traffic(net)
        else:
            info("!!! CRITICAL: Network Failed to Converge.\n")

    except KeyboardInterrupt:
        info("\n*** Stopped.\n")
    finally:
        stop_event.set()
        force_cleanup()
        net.stop()