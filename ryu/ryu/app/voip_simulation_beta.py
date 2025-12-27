#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (CONTINUOUS STREAM EDITION)
Strategi:
1. Fire & Forget: Jalankan Traffic Generator SEKALI saja dengan durasi sangat lama (1 jam).
2. Rate Fixed: Set rate di angka tengah (16.500 Bytes/sec).
   Variasi alami jaringan (jitter/delay) akan membuat angkanya naik turun cantik di 13k-19k.
3. Watchdog: Collector akan memantau, jika traffic mati, dia akan menyalakan ulang otomatis.
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

# Target Range: 13.000 - 19.800
# Kita tembak di tengah-tengah: 16.500 Bytes/sec
TARGET_CENTER = 16500 

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

# ================= 3. COLLECTOR =================
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

def collector_thread(net):
    info("*** [Collector] STARTED.\n")
    conn = None
    try: conn = psycopg2.connect(DB_CONN)
    except: pass
    
    last_stats = {}
    
    while not stop_event.is_set():
        time.sleep(COLLECT_INTERVAL)
        rows = []
        now = datetime.now()
        
        # WATCHDOG LOGIC
        # Kita cek apakah traffic drop ke 0. Jika ya, kita kick lagi traffic generatornya
        traffic_alive = False

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
            
            # Labeling & Storing
            label = "normal"
            dst_mac = IP_TO_MAC.get(meta['dst_ip'], '00:00:00:00:00:00')
            
            # Hanya simpan ke DB kalau bukan data sampah (128 bytes)
            # Karena ini Continuous Stream, harusnya isinya belasan ribu.
            if dtx_bytes > 500: 
                traffic_alive = True
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
        
        # WATCHDOG: Kalau semua host idle (traffic_alive = False), kickstart lagi!
        if not traffic_alive:
            info("... (Watchdog) Traffic seems dead. Kicking generator again ...\n")
            start_traffic_stream(net)

# ================= 4. TRAFFIC GENERATOR (CONTINUOUS) =================
def start_traffic_stream(net):
    """
    Menyalakan traffic SEKALI SAJA dengan durasi 1 jam (3600000 ms).
    Tidak ada loop. Tidak ada start-stop. Mengalir terus.
    """
    info(f"*** [Traffic] Starting Continuous Stream ({TARGET_CENTER} Bps)...\n")
    
    # 1. Pastikan Receiver Jalan
    for h in net.hosts:
        h.cmd(f"killall -9 ITGRecv") # Bersihkan dulu
        h.cmd(f"{ITG_RECV_BIN} > /dev/null 2>&1 &")
    
    time.sleep(2)
    
    # 2. Start Sender (Long Duration)
    for h_name, meta in HOST_INFO.items():
        src_node = net.get(h_name)
        
        # Hitung PPS untuk target 16.500 Bytes/sec
        payload_size = 160 
        wire_size = 202 
        
        # Target PPS = 16500 / 202 = ~81 PPS
        pps = int(TARGET_CENTER / wire_size)
        
        # -t 3600000 = 1 Jam.
        cmd = (f"{ITG_SEND_BIN} -T UDP -a {meta['dst_ip']} "
               f"-c {payload_size} "
               f"-C {pps} "
               f"-t 3600000 > /dev/null 2>&1 &")
        
        src_node.cmd(cmd)

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
            # 1. Jalankan Traffic DULUAN biar ngalir
            start_traffic_stream(net)
            
            # 2. Baru jalankan Collector untuk mencatat
            # (Collector punya watchdog untuk restart traffic kalau mati)
            collector_thread(net) 
        else:
            info("!!! CRITICAL: Network Failed to Converge.\n")

    except KeyboardInterrupt:
        info("\n*** Stopped.\n")
    finally:
        stop_event.set()
        force_cleanup()
        net.stop()