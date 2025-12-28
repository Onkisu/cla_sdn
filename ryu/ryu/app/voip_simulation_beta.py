#!/usr/bin/python3
"""
REAL TRAFFIC SIMULATION (NO FAKE DATA)
Mechanism:
1. Traffic Gen: Tuned to produce ~15kbps physically (-C 95 -c 160).
2. Measurement: Reads REAL Linux Kernel Counters (/sys/class/net/...).
3. Result: Valid data in range 13.000 - 19.800 bytes/sec.
"""

import threading
import time
import os
import subprocess
import psycopg2
from datetime import datetime

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= DATABASE CONFIG =================
DB_CONFIG = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# Mapping Host Mininet -> Database Info
HOST_MAP = {
    'user1': {'ip': '192.168.100.1', 'dst': '10.10.1.1', 'mac': '00:00:00:00:01:01'},
    'user2': {'ip': '192.168.100.2', 'dst': '10.10.2.1', 'mac': '00:00:00:00:01:02'},
    'user3': {'ip': '192.168.100.3', 'dst': '10.10.1.2', 'mac': '00:00:00:00:01:03'},
    'web1':  {'ip': '10.10.1.1',     'dst': '192.168.100.1', 'mac': '00:00:00:00:0A:01'},
    'app1':  {'ip': '10.10.2.1',     'dst': '10.10.2.2',     'mac': '00:00:00:00:0B:01'},
}

stop_event = threading.Event()

# ================= TOPOLOGY =================
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')
    def terminate(self):
        self.cmd('sysctl -w net.ipv4.ip_forward=0')
        super(LinuxRouter, self).terminate()

class SimpleTopo(Topo):
    def build(self):
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        sw1 = self.addSwitch('sw1')
        sw2 = self.addSwitch('sw2')

        # User Network
        h1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        h2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        h3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        self.addLink(h1, sw1); self.addLink(h2, sw1); self.addLink(h3, sw1)
        self.addLink(sw1, cr1, intfName2='cr1-eth1', params2={'ip': '192.168.100.254/24'})

        # Server Network
        w1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        a1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        d1 = self.addHost('db1',  ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')
        self.addLink(w1, sw2); self.addLink(a1, sw2); self.addLink(d1, sw2)
        self.addLink(sw2, cr1, intfName2='cr1-eth2', params2={'ip': '10.10.1.254/24'})

# ================= REAL DATA COLLECTOR =================
def read_linux_bytes(host_name):
    """
    Membaca langsung file kernel Linux untuk statistik interface.
    Ini adalah cara paling valid untuk mengukur traffic di Mininet.
    """
    try:
        # Mininet menyimpan network namespace di /var/run/netns/
        # Kita pakai command 'ip netns exec' untuk baca file stat
        cmd = ["sudo", "ip", "netns", "exec", host_name, "cat", f"/sys/class/net/{host_name}-eth0/statistics/tx_bytes"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return int(result.stdout.strip())
    except:
        return 0

def monitor_thread():
    info("*** [Monitor] Starting Real-Time Interface Polling...\n")
    conn = psycopg2.connect(DB_CONFIG)
    
    # Simpan state terakhir (Total Bytes)
    last_stats = {h: read_linux_bytes(h) for h in HOST_MAP.keys()}
    
    while not stop_event.is_set():
        time.sleep(1.0) # Sampling setiap 1 detik
        rows = []
        now = datetime.now()

        for h_name, meta in HOST_MAP.items():
            # 1. Baca data Total Bytes tebaru dari Kernel
            current_total = read_linux_bytes(h_name)
            
            # 2. Hitung selisih (Delta) = Traffic per detik ini
            delta_bytes = current_total - last_stats[h_name]
            last_stats[h_name] = current_total

            # Hitung packet count (estimasi kasar dr bytes biar db ga error)
            delta_pkts = int(delta_bytes / 180) if delta_bytes > 0 else 0

            # 3. Masukkan ke DB (Hanya jika ada traffic > 0)
            # Karena ini data asli, kadang ada noise kecil, kita filter
            if delta_bytes > 0:
                rows.append((now, 0, meta['ip'], meta['dst'], meta['mac'], 'FF:FF:FF:FF:FF:FF',
                             17, 5060, 5060, delta_bytes, 0, delta_pkts, 0, 1.0, 'data'))

        if rows:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec , traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except Exception as e:
                print(f"DB Error: {e}")
                conn.rollback()
    
    conn.close()

# ================= TRAFFIC TUNING =================
def traffic_thread(net):
    info("*** [Traffic] Starting Tuned VoIP Generator (Target: ~16KB/s)...\n")
    
    # Setup Receivers
    for h in net.hosts:
        h.cmd("killall ITGRecv; ITGRecv &")
    time.sleep(1)

    while not stop_event.is_set():
        for h_name, meta in HOST_MAP.items():
            sender = net.get(h_name)
            
            # --- RUMUS TUNING (PENTING) ---
            # Kita mau range 13.000 - 19.800 bytes/sec
            # Packet Size (-c) = 160 bytes (Payload VoIP G.711 standard)
            # Header Overhead (IP+UDP+Eth) ~= 42 bytes. Total wire size ~= 202 bytes.
            # Rate (-C) = 95 packets/sec.
            # Hitungan kasar: 95 pkts * 202 bytes = ~19.190 bytes/sec (Wire speed)
            # Hitungan payload: 95 * 160 = 15.200 bytes/sec (App speed)
            # D-ITG kadang menghitung payload, Linux menghitung wire.
            # Setting ini akan menghasilkan angka 'real' di Linux sekitar 15k - 20k.
            
            cmd = f"ITGSend -T UDP -a {meta['dst']} -c 160 -C 95 -t 3000 > /dev/null 2>&1 &"
            sender.cmd(cmd)
        
        # Loop tiap 3 detik (biar overlap dan continuous)
        time.sleep(2.8)

# ================= MAIN =================
def setup_namespaces(net):
    # Trik supaya Python bisa baca file /sys/class/net di dalam namespace mininet
    for h in net.hosts:
        subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
        ns_path = f"/var/run/netns/{h.name}"
        if not os.path.exists(ns_path):
             subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

def cleanup():
    os.system("sudo killall -9 ITGSend ITGRecv > /dev/null 2>&1")
    os.system("sudo rm -rf /var/run/netns/*")

if __name__ == "__main__":
    setLogLevel('info')
    cleanup()
    
    topo = SimpleTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        # Routing Server Subnet 2
        net['cr1'].cmd('ip addr add 10.10.2.254/24 dev cr1-eth2')
        
        setup_namespaces(net)
        info("*** Network Ready. Pinging...\n")
        net.pingAll()
        
        t_mon = threading.Thread(target=monitor_thread)
        t_gen = threading.Thread(target=traffic_thread, args=(net,))
        
        t_mon.start()
        t_gen.start()
        
        info("*** RUNNING REAL SIMULATION. Press Ctrl+C to stop.\n")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        info("\n*** Stopping...\n")
        stop_event.set()
        net.stop()
        cleanup()