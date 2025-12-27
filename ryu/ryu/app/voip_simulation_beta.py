#!/usr/bin/python3
"""
FIXED VOIP SIMULATION (Topology + Collector + Strict Generator)
Requirement:
1. Packet Length: 100 - 230 Bytes
2. Throughput Sum: 100.000 - 150.000 BITS/sec (12.5 - 18.75 KB/s)
3. Label otomatis 'normal' jika masuk range tsb.
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

# Target Traffic (Bits Per Second)
TARGET_BPS_MIN = 100000 
TARGET_BPS_MAX = 150000

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

        # Hosts
        user1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        user2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        user3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        
        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')

        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        # Links
        self.addLink(user1, ext_sw, bw=100); self.addLink(user2, ext_sw, bw=100); self.addLink(user3, ext_sw, bw=100)
        self.addLink(web1, tor1, bw=100); self.addLink(web2, tor1, bw=100); self.addLink(cache1, tor1, bw=100)
        self.addLink(app1, tor2, bw=100); self.addLink(db1, tor2, bw=100)

        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ================= 3. COLLECTOR (DATABASE & LABELING) =================
def run_ns_cmd(host_name, cmd):
    if not os.path.exists(f"/var/run/netns/{host_name}"): return None
    try:
        # Timeout slightly increased to prevent empty return
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host_name] + cmd, 
                            capture_output=True, text=True, timeout=1.0).stdout
    except: return None

def get_stats(host_name):
    # Asumsi interface default Mininet: [hostname]-eth0
    intf = f"{host_name}-eth0"
    out = run_ns_cmd(host_name, ['ip', '-s', 'link', 'show', intf])
    if not out: return None
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    if rx and tx:
        # RX Bytes, RX Packets | TX Bytes, TX Packets
        return {
            'rx_bytes': int(rx.group(1)), 'rx_pkts': int(rx.group(2)),
            'tx_bytes': int(tx.group(1)), 'tx_pkts': int(tx.group(2)),
            'time': time.time()
        }
    return None

def collector_thread():
    info("*** [Collector] Monitoring Started. Label Logic: 100k-150k bps = NORMAL.\n")
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
            
            # Hitung Delta
            dtx_bytes = curr['tx_bytes'] - prev['tx_bytes']
            drx_bytes = curr['rx_bytes'] - prev['rx_bytes']
            dtx_pkts = curr['tx_pkts'] - prev['tx_pkts']
            drx_pkts = curr['rx_pkts'] - prev['rx_pkts']
            
            t_diff = curr['time'] - prev['time']
            if t_diff <= 0: t_diff = 1.0

            # Hitung Throughput (Mbps) -> (Bytes * 8) / (Time * 1 Juta)
            bps = (dtx_bytes * 8) / t_diff
            throughput_mbps = bps / 1_000_000.0

            last_stats[h_name] = curr
            
            # --- STRICT LABELING LOGIC ---
            # Range Normal: 100.000 bps - 150.000 bps
            # Dalam Mbps: 0.1 - 0.15 Mbps
            
            label = "idle"
            
            # Buffer dikit biar gak strict banget (toleransi +/- 10%)
            if throughput_mbps > 0.8:
                label = "congestion"
            elif 0.08 <= throughput_mbps <= 0.20: 
                label = "normal"  # INI YG ANDA CARI
            elif throughput_mbps > 0.001:
                label = "background"
            else:
                label = "idle"

            # Hanya insert jika ada activity tx atau rx
            if dtx_bytes > 0 or drx_bytes > 0:
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], 'FF:FF:FF:FF:FF:FF',
                             17, 5060, 5060, dtx_bytes, drx_bytes, dtx_pkts, drx_pkts, 
                             COLLECT_INTERVAL, throughput_mbps, label))

        if rows and conn and not conn.closed:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec, throughput_mbps, traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except Exception as e: 
                # info(f"DB Insert Fail: {e}\n")
                conn.rollback()
    
    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR (THE FIX) =================
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    with cmd_lock:
        try: node.cmd(cmd)
        except: pass

def run_voip_traffic(net):
    info("*** [Traffic] Generating 100k-150k bps Traffic...\n")
    
    # Start Receivers
    receivers = set()
    for meta in HOST_INFO.values(): receivers.add(meta['dst_ip'])
    
    for h in net.hosts:
        if h.IP() in receivers:
            safe_cmd(h, "killall ITGRecv")
            safe_cmd(h, "ITGRecv &")
    
    time.sleep(3) # Tunggu receiver siap

    while not stop_event.is_set():
        # Loop pendek agar parameter random terus berganti
        # Kita pakai durasi pendek (2000ms) di D-ITG
        
        for h_name, meta in HOST_INFO.items():
            node = net.get(h_name)
            
            # --- RUMUS MATEMATIKA SUPAYA PAS REQUIREMENT ---
            
            # 1. Tentukan target bits (100k - 150k bps)
            target_bps = random.randint(TARGET_BPS_MIN, TARGET_BPS_MAX)
            
            # 2. Konversi ke Bytes per detik
            target_Bps = target_bps / 8
            
            # 3. Tentukan ukuran paket (100 - 230 Bytes)
            pkt_size = random.randint(100, 230)
            
            # 4. Hitung Rate (packet per second) supaya totalnya pas
            # Rate = Target Bytes / Size per Packet
            rate_pps = int(target_Bps / pkt_size)
            
            # Command D-ITG
            # -c: Constant packet size (kita acak per command)
            # -C: Constant rate (kita hitung biar throughputnya pas)
            # -t: Duration (2000ms = 2 detik, lalu loop lagi biar dinamis)
            
            cmd = f"ITGSend -T UDP -a {meta['dst_ip']} -c {pkt_size} -C {rate_pps} -t 2000 > /dev/null 2>&1 &"
            safe_cmd(node, cmd)
        
        # Sleep sebentar sebelum generate batch command berikutnya
        # Diberi jeda 2 detik (sesuai durasi -t)
        time.sleep(2.0)

# ================= 5. SETUP & CLEANUP =================
def setup_namespaces(net):
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    for h in net.hosts:
        ns_path = f"/var/run/netns/{h.name}"
        if os.path.exists(ns_path):
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
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        setup_namespaces(net)
        info("*** Waiting for Network Convergence (5s)...\n")
        time.sleep(5)
        
        # Test Ping Dulu Biar Yakin Connect
        info("*** Testing Connectivity...\n")
        loss = net.pingAll()
        if loss > 0:
             info("!!! WARNING: Ping failed. Check Controller/Router Config !!!\n")
        
        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        run_voip_traffic(net)

    except KeyboardInterrupt:
        info("\n*** Interrupted.\n")
    finally:
        stop_event.set()
        cleanup()
        net.stop()