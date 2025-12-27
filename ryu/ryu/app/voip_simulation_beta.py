#!/usr/bin/python3
"""
FINAL STABLE VOIP SIMULATION
Target: 100.000 - 150.000 bps (STABIL)
Strategi: Continuous Stream (Tanpa restart loop)
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
SIMULATION_DURATION = 60 # Detik

HOST_INFO = {
    'user1': {'ip': '192.168.100.1', 'dst_ip': '10.10.1.1'},
    'user2': {'ip': '192.168.100.2', 'dst_ip': '10.10.2.1'},
    'user3': {'ip': '192.168.100.3', 'dst_ip': '10.10.1.2'},
    'web1':  {'ip': '10.10.1.1', 'dst_ip': '192.168.100.1'},
    'web2':  {'ip': '10.10.1.2', 'dst_ip': '192.168.100.3'},
    'cache1':{'ip': '10.10.1.3', 'dst_ip': '10.10.2.2'},
    'app1':  {'ip': '10.10.2.1', 'dst_ip': '10.10.2.2'},
    'db1':   {'ip': '10.10.2.2', 'dst_ip': '10.10.2.1'},
}

stop_event = threading.Event()

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
        info("*** Topologi Data Center L3...\n")
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        ext_sw = self.addSwitch('ext_sw', dpid='1')
        tor1 = self.addSwitch('tor1', dpid='2')
        tor2 = self.addSwitch('tor2', dpid='3')

        # Hosts
        h1=self.addHost('user1', ip='192.168.100.1/24', defaultRoute='via 192.168.100.254')
        h2=self.addHost('user2', ip='192.168.100.2/24', defaultRoute='via 192.168.100.254')
        h3=self.addHost('user3', ip='192.168.100.3/24', defaultRoute='via 192.168.100.254')
        
        w1=self.addHost('web1', ip='10.10.1.1/24', defaultRoute='via 10.10.1.254')
        w2=self.addHost('web2', ip='10.10.1.2/24', defaultRoute='via 10.10.1.254')
        c1=self.addHost('cache1', ip='10.10.1.3/24', defaultRoute='via 10.10.1.254')

        a1=self.addHost('app1', ip='10.10.2.1/24', defaultRoute='via 10.10.2.254')
        d1=self.addHost('db1', ip='10.10.2.2/24', defaultRoute='via 10.10.2.254')

        # Links (High Bandwidth Internal)
        self.addLink(h1, ext_sw); self.addLink(h2, ext_sw); self.addLink(h3, ext_sw)
        self.addLink(w1, tor1); self.addLink(w2, tor1); self.addLink(c1, tor1)
        self.addLink(a1, tor2); self.addLink(d1, tor2)

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
        return {
            'tx_bytes': int(tx.group(1)), 'rx_bytes': int(rx.group(1)),
            'tx_pkts': int(tx.group(2)), 'rx_pkts': int(rx.group(2)),
            'time': time.time()
        }
    return None

def collector_thread():
    info("*** [Collector] STARTED. Target: 100k-150k bps.\n")
    conn = psycopg2.connect(DB_CONN)
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

            # Hitung BPS
            bps = (dtx * 8) / t_diff
            
            # Update history
            last_stats[h_name] = curr
            
            # === LABELING ===
            # Normal: 90.000 - 160.000 bps (Buffer dikit)
            label = "idle"
            if 90000 <= bps <= 160000:
                label = "normal"
            elif bps > 160000:
                label = "congestion"
            elif bps > 1000:
                label = "background"

            # Print debug ke layar biar kelihatan jalan atau nggak
            if dtx > 100:
                print(f"DEBUG: {h_name} -> {bps:.0f} bps [{label}]")

            if dtx > 0 or drx > 0:
                rows.append((now, 0, meta['ip'], meta['dst_ip'], '00:00:00:00:00:00', '00:00:00:00:00:00',
                             17, 5060, 5060, dtx, drx, 0, 0, COLLECT_INTERVAL, bps, label))

        if rows:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec, throughput_bps, traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except: conn.rollback()
    conn.close()

# ================= 4. GENERATOR STABIL =================
# ================= 4. GENERATOR (DENGAN WARM-UP) =================
def run_traffic(net):
    info("*** [Traffic] Starting Traffic (Staggered Mode)...\n")
    
    # 1. Start Receivers
    print("-> Starting Receivers...")
    for h in net.hosts:
        h.cmd("killall ITGRecv")
        # Tambahkan log file agar jika error bisa dibaca
        h.cmd(f"ITGRecv -l /tmp/{h.name}_recv.log &") 
    
    time.sleep(3) 
    
    # 2. Start Senders
    print("-> Starting Senders (with ARP Warm-up)...")
    
    for h_name, meta in HOST_INFO.items():
        if stop_event.is_set(): break
        
        sender = net.get(h_name)
        dst_ip = meta['dst_ip']
        
        # PING DULU (Penting!)
        sender.cmd(f"ping -c 1 {dst_ip}")
        
        target_bps = random.randint(110000, 140000) 
        pkt_size = random.randint(120, 180)
        
        # Rumus Rate
        rate_pps = int(target_bps / (pkt_size * 8))
        if rate_pps < 1: rate_pps = 1
        
        duration_ms = SIMULATION_DURATION * 1000
        
        print(f"   + {h_name} -> {dst_ip} | Target: {target_bps} bps | Rate: {rate_pps} pps")
        
        # PERBAIKAN DI SINI:
        # 1. Gunakan full path jika perlu (misal /usr/local/bin/ITGSend)
        # 2. Tambahkan logging (-l) untuk debugging
        # 3. Hapus argumen yang mungkin bikin crash jika folder log tidak ada
        
        # Opsi A: Command Standar (Coba ini dulu)
        cmd = f"ITGSend -T UDP -a {dst_ip} -c {pkt_size} -C {rate_pps} -t {duration_ms} -l /tmp/{h_name}_send.log &"
        
        # Eksekusi
        sender.cmd(cmd)
        
        time.sleep(0.5)

    print("*** Traffic sedang berjalan... Monitor database sekarang! ***")
    
    # Cek apakah process benar-benar jalan di salah satu host
    check_proc = net.get('user1').cmd("pgrep ITGSend")
    if not check_proc.strip():
        print("\n[CRITICAL ERROR] ITGSend tidak berjalan! Cek instalasi D-ITG.")
        print("Coba jalankan: 'ITGSend -h' di terminal untuk memastikan terinstall.\n")
    
   
    # Biarkan jalan selama durasi simulasi
    time.sleep(SIMULATION_DURATION)

# ================= 5. MAIN =================
def setup_ns(net):
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    for h in net.hosts:
        path = f"/var/run/netns/{h.name}"
        if os.path.exists(path): subprocess.run(f"sudo rm -f {path}", shell=True)
        subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

if __name__ == "__main__":
    setLogLevel('info')
    topo = DataCenterTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        setup_ns(net)
        info("*** Menunggu Network Convergence (5s)...\n")
        time.sleep(5)
        
        # WAJIB PING DULU
        if net.pingAll() > 0:
            print("!!! PING GAGAL. Traffic tidak akan jalan !!!")
            print("Pastikan Controller (Ryu/ONOS) sudah jalan.")
            sys.exit(1)
            
        t = threading.Thread(target=collector_thread)
        t.start()
        
        run_traffic(net)
        
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True)
        net.stop()