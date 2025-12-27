#!/usr/bin/python3
"""
LEAF-SPINE VOIP SIMULATION (DATASET GENERATOR EDITION)
Fitur:
1. Strict Traffic: 13.000 - 19.800 Bytes/sec.
2. Single Label: 'normal' only.
3. No Idle: Data kosong/gap dibuang, tidak masuk database.
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

        # Backbone Links
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000) 

        # Host Links
        self.addLink(self.addHost('user1', ip='10.0.1.1/8', mac='00:00:00:00:01:01'), leaves[0], bw=100)
        self.addLink(self.addHost('user2', ip='10.0.1.2/8', mac='00:00:00:00:01:02'), leaves[0], bw=100)
        self.addLink(self.addHost('web1', ip='10.0.2.1/8', mac='00:00:00:00:02:01'), leaves[1], bw=100)
        self.addLink(self.addHost('web2', ip='10.0.2.2/8', mac='00:00:00:00:02:02'), leaves[1], bw=100)
        self.addLink(self.addHost('app1', ip='10.0.3.1/8', mac='00:00:00:00:03:01'), leaves[2], bw=100)
        self.addLink(self.addHost('db1', ip='10.0.3.2/8', mac='00:00:00:00:03:02'), leaves[2], bw=100)

# ================= 3. COLLECTOR (FILTERED) =================
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
        return {
            'rx_bytes': int(rx.group(1)), 'rx_pkts': int(rx.group(2)),
            'tx_bytes': int(tx.group(1)), 'tx_pkts': int(tx.group(2)),
            'time': time.time()
        }
    return None

def collector_thread():
    info("*** [Collector] Monitoring Started (Only Saving 'Normal' Traffic)...\n")
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
            
            # --- LOGIC FILTERING DATASET ---
            # 1. Hapus Idle: Jika bytes < 5000 (gap), jangan simpan ke DB.
            # 2. Label: Semuanya 'normal'.
            
            if dtx_bytes > 5000:
                traffic_label = "normal"
                
                dst_mac = IP_TO_MAC.get(meta['dst_ip'], '00:00:00:00:00:00')
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], dst_mac,
                             17, 5060, 5060, dtx_bytes, drx_bytes, dtx_pkts, drx_pkts, 
                             COLLECT_INTERVAL, traffic_label))

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

# ================= 4. TRAFFIC GENERATOR (CONSISTENT) =================
def run_voip_traffic(net):
    info(f"*** [Traffic] Generating Consistent 13k-19k Traffic...\n")
    
    # Start Receivers
    for h in net.hosts:
        h.cmd("killall -9 ITGRecv")
        h.cmd(f"{ITG_RECV_BIN} > /dev/null 2>&1 &")
    
    time.sleep(2) 

    while not stop_event.is_set():
        for h_name, meta in HOST_INFO.items():
            src_node = net.get(h_name)
            
            payload_size = 160 
            wire_packet_size = payload_size + 42
            
            # Random target murni 13.000 - 19.800
            target_bytes = random.randint(TARGET_BYTES_MIN, TARGET_BYTES_MAX)
            pps = int(target_bytes / wire_packet_size)
            
            # Boosting yang pas agar tidak terlalu overshoot atau drop
            boosted_pps = int(pps * 1.25)

            cmd = (f"{ITG_SEND_BIN} -T UDP -a {meta['dst_ip']} "
                   f"-c {payload_size} "
                   f"-C {boosted_pps} "
                   f"-t 1000 > /dev/null 2>&1 &")
            
            try: src_node.cmd(cmd)
            except: pass
        
        # Loop 1.0 pas agar overlap sedikit dengan processing time
        # Ini meminimalisir gap (idle)
        time.sleep(1.0)

# ================= 5. MAIN SETUP =================
def force_cleanup():
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)

def setup_network_config(net):
    info("*** Configuring Flat Network...\n")
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
    info("*** WARMUP: Pinging...\n")
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
            t_col = threading.Thread(target=collector_thread)
            t_col.start()
            run_voip_traffic(net)
        else:
            info("!!! CRITICAL: Network Failed to Converge.\n")

    except KeyboardInterrupt:
        info("\n*** Stopped.\n")
    finally:
        stop_event.set()
        force_cleanup()
        net.stop()