#!/usr/bin/python3
"""
FIXED VOIP SIMULATION (RAW BYTES EDITION)
Target: 
1. Database hanya menyimpan raw bytes/packets.
2. Tidak menyimpan kolom throughput_bps.
3. Traffic fisik dijaga agar saat di-query (bytes * 8) hasilnya 100k - 150k bps.
"""

import threading
import time
import random
import os
import sys
import subprocess
import re
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

# Range Target (BITS per Second)
# Ini digunakan untuk mengontrol seberapa cepat paket dikirim
TARGET_BPS_MIN = 100000 
TARGET_BPS_MAX = 150000

HOST_INFO = {
    # Subnet Client
    'user1': {'ip': '192.168.100.1', 'mac': '00:00:00:00:01:01', 'dst_ip': '10.10.1.1'},
    'user2': {'ip': '192.168.100.2', 'mac': '00:00:00:00:01:02', 'dst_ip': '10.10.2.1'},
    'user3': {'ip': '192.168.100.3', 'mac': '00:00:00:00:01:03', 'dst_ip': '10.10.1.2'},
    # Subnet Server
    'web1':  {'ip': '10.10.1.1', 'mac': '00:00:00:00:0A:01', 'dst_ip': '192.168.100.1'},
    'web2':  {'ip': '10.10.1.2', 'mac': '00:00:00:00:0A:02', 'dst_ip': '192.168.100.3'},
    'cache1':{'ip': '10.10.1.3', 'mac': '00:00:00:00:0A:03', 'dst_ip': '10.10.2.2'},
    # Subnet App/DB
    'app1':  {'ip': '10.10.2.1', 'mac': '00:00:00:00:0B:01', 'dst_ip': '10.10.2.2'},
    'db1':   {'ip': '10.10.2.2', 'mac': '00:00:00:00:0B:02', 'dst_ip': '10.10.2.1'},
}

stop_event = threading.Event()
cmd_lock = threading.Lock()

# ================= 2. TOPOLOGI =================
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

# ================= 3. COLLECTOR (RAW BYTES ONLY) =================
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
    info("*** [Collector] Monitoring Started (Raw Bytes Only).\n")
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

            # --- Kalkulasi Sementara untuk LABEL ---
            # Kita hitung sebentar cuma buat nentuin Label 'normal'/'congestion'.
            # Nilai ini TIDAK dimasukkan ke DB.
            current_bps = (dtx_bytes * 8) / t_diff
            
            label = "idle"
            if current_bps > 180000:
                label = "congestion"
            elif 90000 <= current_bps <= 160000:
                label = "normal"
            elif current_bps > 1000:
                label = "background"

            last_stats[h_name] = curr

            # Insert hanya jika ada traffic
            if dtx_bytes > 0 or drx_bytes > 0:
                # Perhatikan: throughput_bps SUDAH DIHAPUS dari tuple ini
                rows.append((now, 0, meta['ip'], meta['dst_ip'], meta['mac'], 'FF:FF:FF:FF:FF:FF',
                             17, 5060, 5060, dtx_bytes, drx_bytes, dtx_pkts, drx_pkts, 
                             COLLECT_INTERVAL, label))

        if rows and conn and not conn.closed:
            try:
                cur = conn.cursor()
                # --- QUERY TANPA THROUGHPUT_BPS ---
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec, traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except Exception as e: 
                conn.rollback()
    
    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR =================
# Logika ini TETAP diperlukan agar fisik traffic yang lewat kabel
# jumlah bytes-nya sesuai range target saat nanti dihitung di SQL.
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    with cmd_lock:
        try: node.cmd(cmd)
        except: pass

# GANTI BAGIAN INI DI SCRIPT PYTHON ANDA

def run_voip_traffic(net):
    info(f"*** [Traffic] Starting Continuous Stream (Target: {TARGET_BPS_MIN}-{TARGET_BPS_MAX} bps)...\n")
    
    # 1. Start Receivers
    receivers = set()
    for meta in HOST_INFO.values(): receivers.add(meta['dst_ip'])
    
    print("-> Starting Receivers...")
    for h in net.hosts:
        if h.IP() in receivers:
            safe_cmd(h, "killall ITGRecv")
            safe_cmd(h, "ITGRecv &")
    
    time.sleep(3) # Tunggu receiver siap
    
    # 2. Start Senders (Sekali jalan durasi panjang)
    print("-> Starting Continuous Senders...")
    
    # Durasi stream dilebihkan sedikit dari durasi simulasi agar tidak putus di tengah
    # Misal kita ingin collect data 60 detik, kita set traffic 70 detik.
    stream_duration = 70000 # ms
    
    for h_name, meta in HOST_INFO.items():
        node = net.get(h_name)
        
        # --- KALKULASI TARGET STABIL ---
        # Kita ambil nilai tengah/random di range target
        target_bps = random.randint(TARGET_BPS_MIN, TARGET_BPS_MAX) 
        
        # Packet Size statis (mirip dataset VoIP: ~120-160 bytes)
        pkt_size_bytes = random.randint(120, 160)
        
        # Hitung Rate (PPS) supaya pas target
        # Rumus: Rate = Target_BPS / (Size_Bytes * 8)
        rate_pps = int(target_bps / (pkt_size_bytes * 8))
        
        # Command ITGSend
        # -t: Durasi (ms). Kita set panjang (70 detik) jadi dia ngirim TERUS menerus.
        # Tidak ada loop start-stop.
        cmd = f"ITGSend -T UDP -a {meta['dst_ip']} -c {pkt_size_bytes} -C {rate_pps} -t {stream_duration} > /dev/null 2>&1 &"
        
        print(f"   + {h_name} -> {meta['dst_ip']} | {target_bps} bps | {rate_pps} pps (Continuous)")
        safe_cmd(node, cmd)
        
        # Beri jeda dikit antar host biar start-nya gak barengan banget (opsional)
        time.sleep(0.2)
        
    print("*** Traffic sedang berjalan stabil. Silakan tunggu collector... ***")
    
    # Block thread ini selama simulasi agar script tidak exit
    # Collector akan tetap berjalan di background
    while not stop_event.is_set():
        time.sleep(1)

# ================= 5. MAIN =================
def setup_namespaces(net):
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    for h in net.hosts:
        ns_path = f"/var/run/netns/{h.name}"
        if os.path.exists(ns_path):
            subprocess.run(['sudo', 'rm', '-f', ns_path], check=False)
        subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

def cleanup():
    info("*** Cleaning up...\n")
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True, stderr=subprocess.DEVNULL)
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    setLogLevel('info')
    topo = DataCenterTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        setup_namespaces(net)
        info("*** Network Ready. Waiting 5s...\n")
        time.sleep(5)
        
        if net.pingAll() > 0:
             info("!!! PING FAILED. Check Controller !!!\n")
        
        t_col = threading.Thread(target=collector_thread)
        t_col.start()

        run_voip_traffic(net)

    except KeyboardInterrupt:
        info("\n*** Stopped.\n")
    finally:
        stop_event.set()
        cleanup()
        net.stop()