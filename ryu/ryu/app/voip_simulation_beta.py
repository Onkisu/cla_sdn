#!/usr/bin/python3
"""
FIXED VOIP SIMULATION (Dataset Clone Edition)
Target: Meniru pola dataset User (Size 100-160B, Interval ~10ms)
"""

import threading
import time
import random
import os
import sys
import subprocess
import re
import math
import signal
import psycopg2
from collections import defaultdict
from datetime import datetime

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI

# ================= 1. KONFIGURASI =================
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1

HOST_INFO = {
    # Subnet Client
    'user1': {'ip': '192.168.100.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.10.1.1', 'label': 'normal'},
    'user2': {'ip': '192.168.100.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.10.2.1', 'label': 'normal'},
    'user3': {'ip': '192.168.100.3', 'mac': '00:00:00:00:01:03', 'dst_ip': '10.10.1.2', 'label': 'normal'},
    # Subnet Server
    'web1':  {'ip': '10.10.1.1', 'mac': '00:00:00:00:0A:01', 'dst_ip': '192.168.100.1', 'label': 'normal'},
    'web2':  {'ip': '10.10.1.2', 'mac': '00:00:00:00:0A:02', 'dst_ip': '192.168.100.3', 'label': 'normal'},
    'cache1':{'ip': '10.10.1.3', 'mac': '00:00:00:00:0A:03', 'dst_ip': '10.10.2.2', 'label': 'normal'},
    # Subnet App/DB
    'app1':  {'ip': '10.10.2.1', 'mac': '00:00:00:00:0B:01', 'dst_ip': '10.10.2.2', 'label': 'normal'},
    'db1':   {'ip': '10.10.2.2', 'mac': '00:00:00:00:0B:02', 'dst_ip': '10.10.2.1', 'label': 'normal'},
}

stop_event = threading.Event()
cmd_lock = threading.Lock()

# ================= 2. TOPOLOGI (ROUTER + SWITCH) =================
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl -w net.ipv4.ip_forward=0')
        super(LinuxRouter, self).terminate()

class DataCenterTopo(Topo):
    def build(self):
        info("*** Membangun Topologi Data Center L3...\n")
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

        self.addLink(user1, ext_sw, bw=20); self.addLink(user2, ext_sw, bw=20); self.addLink(user3, ext_sw, bw=20)
        self.addLink(web1, tor1, bw=50); self.addLink(web2, tor1, bw=50); self.addLink(cache1, tor1, bw=50)
        self.addLink(app1, tor2, bw=50); self.addLink(db1, tor2, bw=50)

        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ================= 3. COLLECTOR (DATABASE) =================
def run_ns_cmd(host_name, cmd):
    if not os.path.exists(f"/var/run/netns/{host_name}"): return None
    try:
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host_name] + cmd, 
                            capture_output=True, text=True, timeout=0.5).stdout
    except: return None

def get_stats(host_name):
    intf = f"{host_name}-eth0"
    out = run_ns_cmd(host_name, ['ip', '-s', 'link', 'show', intf])
    if not out: return None
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    if rx and tx:
        return {'rx_bytes': int(rx.group(1)), 'tx_bytes': int(tx.group(1)), 'time': time.time()}
    return None

def collector_thread():
    info("*** [Collector] Database Collector Started.\n")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONN)
    except Exception as e:
        info(f"!!! [Collector] DB Connection Failed: {e}\n")

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
            dtx = curr['tx_bytes'] - prev['tx_bytes']
            drx = curr['rx_bytes'] - prev['rx_bytes']
            t_diff = curr['time'] - prev['time']
            if t_diff <= 0: t_diff = 1.0

            throughput = (dtx * 8) / (t_diff * 1_000_000.0) # Mbps
            last_stats[h_name] = curr
            
            # --- LABELING UNTUK TRAINING MODEL ---
            # Normal Traffic: range 0.05 - 0.2 Mbps (Sesuai dataset anda)
            # Congestion/Attack: > 0.8 Mbps
            
            label = "normal" # Default label
            
            if throughput > 0.8: 
                label = "congestion" # Burst traffic
            elif throughput > 0.01:
                label = "normal"     # Traffic dataset anda
            else:
                label = "idle"

            if dtx > 0 or drx > 0:
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], 'FF:FF:FF:FF:FF:FF',
                             17, 5060, 5060, dtx, drx, 0, 0, COLLECT_INTERVAL, throughput, label))

        if rows and conn and not conn.closed:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec, throughput_mbps, traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except: conn.rollback()
    
    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR (DATASET MIMIC) =================
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    with cmd_lock:
        try: node.cmd(cmd)
        except: pass

def run_voip_traffic(net):
    info("*** [Traffic] Starting Traffic Generation (Pattern: Dataset Clone)...\n")
    
    # Start Receivers
    receivers = set()
    for meta in HOST_INFO.values(): receivers.add(meta['dst_ip'])
    
    for h in net.hosts:
        if h.IP() in receivers:
            safe_cmd(h, "killall ITGRecv")
            safe_cmd(h, "ITGRecv &")
    
    time.sleep(2)

    while not stop_event.is_set():
        # --- PHASE A: NORMAL TRAFFIC (Mirip Dataset) (25 Detik) ---
        # Dataset Specs:
        # Size: 103 - 157 bytes
        # Interval: 0.006s - 0.015s (Rate: ~66 - 160 pps)
        
        info("--- [Phase] Normal (Dataset Pattern) ---\n")
        start_t = time.time()
        
        while time.time() - start_t < 25 and not stop_event.is_set():
            for h_name, meta in HOST_INFO.items():
                node = net.get(h_name)
                
                # 1. Packet Size (Persis Dataset: 100 - 160)
                pkt_size = random.randint(100, 160) 
                
                # 2. Rate (Persis Dataset: Interval ~10ms -> Rate ~100pps)
                # Kita variasi antara 60 pps sampai 120 pps
                rate_pps = random.randint(60, 120)
                
                # Command D-ITG
                # Kita set durasi pendek (2000ms) lalu loop agar ukuran paket terus berubah
                cmd = f"ITGSend -T UDP -a {meta['dst_ip']} -c {pkt_size} -C {rate_pps} -t 2000 > /dev/null 2>&1 &"
                safe_cmd(node, cmd)
            
            time.sleep(2.05) 

        # --- PHASE B: BURST / CONGESTION (5 Detik) ---
        # Ini untuk data "anomali" supaya model Anda bisa belajar bedanya
        # Kita buat beda JAUH: Paket besar (500-1000) dan Rate GILA (3000 pps)
        
        info("!!! [Phase] Congestion Burst (Anomaly Data) !!!\n")
        burst_victims = ['user1', 'user2'] 
        
        for v in burst_victims:
            node = net.get(v)
            dst = HOST_INFO[v]['dst_ip']
            
            b_size = random.randint(500, 1000) # Ukuran paket besar
            b_rate = 3000 # Rate 3000 pps -> ~15-20 Mbps
            
            cmd = f"ITGSend -T UDP -a {dst} -c {b_size} -C {b_rate} -t 5000 > /dev/null 2>&1 &"
            safe_cmd(node, cmd)
        
        time.sleep(5)

# ================= 5. SETUP & CLEANUP =================
def setup_namespaces(net):
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    for h in net.hosts:
        ns_path = f"/var/run/netns/{h.name}"
        if os.path.exists(ns_path):
            subprocess.run(['sudo', 'umount', ns_path], check=False, stderr=subprocess.DEVNULL)
            subprocess.run(['sudo', 'umount', ns_path], check=False, stderr=subprocess.DEVNULL)
            subprocess.run(['sudo', 'rm', '-f', ns_path], check=False)
        subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

def cleanup():
    info("*** Cleaning up processes...\n")
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo umount /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    setLogLevel('info')
    topo = DataCenterTopo()
    # Gunakan Controller Ryu yang simple
    net = Mininet(topo=topo, 
                  controller=RemoteController, 
                  switch=OVSKernelSwitch,
                  link=TCLink)
    
    try:
        net.start()
        setup_namespaces(net)
        info("*** Waiting for Network Convergence (5s)...\n")
        time.sleep(5)
        
        if net.pingAll() > 0:
            info("!!! WARNING: Ping failed. Check Controller.\n")
        
        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        run_voip_traffic(net)

    except KeyboardInterrupt:
        info("\n*** Interrupted.\n")
    finally:
        stop_event.set()
        cleanup()
        net.stop()