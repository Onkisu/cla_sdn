#!/usr/bin/python3
"""
LEAF-SPINE 3x3 VOIP SIMULATION (FIXED STP & ARP)
"""

import threading
import time
import random
import os
import sys
import signal
import psycopg2
from datetime import datetime
import itertools

# --- IMPORT MININET & SCAPY ---
from scapy.all import sniff, IP, UDP
from mininet.net import Mininet
from mininet.node import Controller, OVSKernelSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= 1. KONFIGURASI =================
DB_CONFIG = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
TARGET_BPS_MIN = 100000 
TARGET_BPS_MAX = 150000

HOST_INFO = {
    'user1': {'ip': '10.0.0.1', 'dst_ip': '10.0.0.2'},
    'user2': {'ip': '10.0.0.2', 'dst_ip': '10.0.0.1'}
}

# Global Variables
stop_event = threading.Event()
pkt_queue = []
queue_lock = threading.Lock()
packet_counter = itertools.count(1)
start_time_ref = time.time()

# ================= 2. TOPOLOGI LEAF-SPINE 3x3 =================
class LeafSpineTopo(Topo):
    def build(self):
        #         info("*** Membangun Topologi Leaf-Spine 3x3...\n")
        
        spines = [self.addSwitch(f'spine{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}') for i in range(1, 4)]
        
        # Link Backbone
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000)
        
        # Hosts
        user1 = self.addHost('user1', ip='10.0.0.1/24', mac='00:00:00:00:00:01')
        self.addLink(user1, leaves[0], bw=100)
        
        web1 = self.addHost('web1', ip='10.0.0.3/24', mac='00:00:00:00:00:03')
        self.addLink(web1, leaves[1], bw=100)
        
        user2 = self.addHost('user2', ip='10.0.0.2/24', mac='00:00:00:00:00:02')
        self.addLink(user2, leaves[2], bw=100)

# ================= 3. CAPTURE & DB =================
def process_packet(pkt):
    if IP in pkt and UDP in pkt:
        src_ip = pkt[IP].src
        dst_ip = pkt[IP].dst
        target_ips = {'10.0.0.1', '10.0.0.2'}
        
        if src_ip not in target_ips or dst_ip not in target_ips:
            return

        arrival_time = datetime.now()
        rel_time = time.time() - start_time_ref
        length = len(pkt)
        sport = pkt[UDP].sport
        dport = pkt[UDP].dport
        info_str = f"{sport} > {dport} Len={length}"
        no = next(packet_counter)
        
        with queue_lock:
            pkt_queue.append({
                "time": rel_time, "source": src_ip, "protocol": "UDP",
                "length": length, "Arrival Time": arrival_time,
                "info": info_str, "No.": no, "destination": dst_ip
            })

def sniffer_thread(net):
    info("*** [Sniffer] Starting Capture...\n")
    ifaces = []
    for sw in net.switches:
        if 'leaf' in sw.name:
            for intf in sw.intfList():
                if intf.name != 'lo': ifaces.append(intf.name)
    sniff(iface=ifaces, prn=process_packet, filter="udp", store=0, stop_filter=lambda x: stop_event.is_set())

def db_writer_thread():
    info("*** [DB Writer] Connecting...\n")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONFIG)
    except Exception as e:
        info(f"!!! DB Error: {e}\n")
        return

    while not stop_event.is_set():
        time.sleep(1.0)
        batch = []
        with queue_lock:
            if pkt_queue:
                batch = list(pkt_queue)
                pkt_queue.clear()
        
        if batch and conn:
            try:
                cur = conn.cursor()
                q = """INSERT INTO pcap_logs ("time", "source", "protocol", "length", "Arrival Time", "info", "No.", "destination") VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                data_tuple = [(r["time"], r["source"], r["protocol"], r["length"], r["Arrival Time"], r["info"], r["No."], r["destination"]) for r in batch]
                cur.executemany(q, data_tuple)
                conn.commit()
            except Exception as e:
                print(f"SQL Error: {e}")
                conn.rollback()
    if conn: conn.close()

# ================= 4. TRAFFIC =================
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    try: node.cmd(cmd)
    except: pass

def run_voip_traffic(net):
    info("*** [Traffic] Starting VoIP Stream...\n")
    u1, u2 = net.get('user1'), net.get('user2')
    safe_cmd(u2, "killall ITGRecv"); safe_cmd(u2, "ITGRecv &")
    time.sleep(2)
    
    while not stop_event.is_set():
        target_bps = random.randint(TARGET_BPS_MIN, TARGET_BPS_MAX)
        pkt_size = random.randint(100, 160)
        rate_pps = max(1, int(target_bps / (pkt_size * 8)))
        cmd = f"ITGSend -T UDP -a 10.0.0.2 -c {pkt_size} -C {rate_pps} -t 2000 > /dev/null 2>&1"
        safe_cmd(u1, cmd)
        time.sleep(0.5)

# ================= 5. MAIN =================
def cleanup():
    os.system("sudo mn -c > /dev/null 2>&1")
    os.system("sudo killall -9 ITGSend ITGRecv > /dev/null 2>&1")

def signal_handler(sig, frame):
    info("\n*** Stopping...\n")
    stop_event.set()

if __name__ == "__main__":
    cleanup()
    setLogLevel('info')
    
    topo = LeafSpineTopo()
    
    # PERUBAHAN PENTING 1: controller=None (Standalone Mode)
    # Ini mencegah controller Mininet mengganggu STP OVS
    net = Mininet(topo=topo, controller=None, switch=OVSKernelSwitch, link=TCLink)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        net.start()
        
        info("*** Setting OVS to Standalone & Enabling STP...\n")
        for sw in net.switches:
            # Set mode standalone agar switch belajar MAC address sendiri (L2 learning)
            sw.cmd(f'ovs-vsctl set-controller {sw.name} ptcp:') 
            sw.cmd(f'ovs-vsctl set-fail-mode {sw.name} standalone')
            sw.cmd(f'ovs-vsctl set Bridge {sw.name} stp_enable=true')

        # PERUBAHAN PENTING 2: Waktu tunggu lebih lama (40 detik)
        info("*** Menunggu 40 detik untuk STP Convergence (Wajib!)...\n")
        time.sleep(40) 
        
        info("*** Ping All untuk Cek Koneksi...\n")
        drop = net.pingAll()
        
        if drop > 0:
            info("!!! PERINGATAN: Masih ada packet loss saat Ping. Traffic mungkin tidak stabil.\n")
        else:
            info("*** Koneksi Sempurna. Memulai Simulasi...\n")

        t_db = threading.Thread(target=db_writer_thread); t_db.daemon = True; t_db.start()
        t_sniff = threading.Thread(target=sniffer_thread, args=(net,)); t_sniff.daemon = True; t_sniff.start()
        
        run_voip_traffic(net)
        
    except Exception as e:
        info(f"!!! Error: {e}\n")
    finally:
        stop_event.set()
        time.sleep(1)
        net.stop()
        cleanup()