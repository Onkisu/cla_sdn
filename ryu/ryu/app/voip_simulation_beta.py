#!/usr/bin/python3
"""
============================================================
FINAL SCRIPT: LEAF-SPINE 3x3 VOIP SIMULATION
============================================================
Fitur Utama:
1. Topologi Leaf-Spine (3 Spine, 3 Leaf) - Full Mesh.
2. Cross-Leaf Traffic: User1 (Leaf1) -> User2 (Leaf3).
   -> Memaksa traffic melewati Spine (Valid untuk K-Shortest Path).
3. Sniffer Mode: Menangkap setiap paket & Length-nya.
4. Database: Menyimpan data per paket ke PostgreSQL.
============================================================
"""

import threading
import time
import random
import os
import sys
import signal
import psycopg2
from datetime import datetime

# --- IMPORT LIBRARY JARINGAN ---
from scapy.all import sniff, IP, UDP
from mininet.net import Mininet
from mininet.node import Controller, OVSKernelSwitch, RemoteController
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI

# ================= 1. KONFIGURASI DATABASE & TARGET =================
DB_CONFIG = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# Target Throughput VoIP (Bits Per Second)
TARGET_BPS_MIN = 100000 
TARGET_BPS_MAX = 150000

# Konfigurasi Host & IP (Flat Network 10.0.0.x agar Routing L2 otomatis)
HOST_INFO = {
    # --- SKENARIO UTAMA (VOIP) ---
    # User 1 mengirim suara ke User 2
    # Lokasi fisik mereka dipisah (Leaf 1 vs Leaf 3)
    'user1': {'ip': '10.0.0.1', 'dst_ip': '10.0.0.2', 'mac': '00:00:00:00:00:01'}, 
    'user2': {'ip': '10.0.0.2', 'dst_ip': '10.0.0.1', 'mac': '00:00:00:00:00:02'},
}

# Global Control Variables
stop_event = threading.Event()
pkt_queue = []
queue_lock = threading.Lock()
cmd_lock = threading.Lock()

# ================= 2. CLASS TOPOLOGI LEAF-SPINE =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Membangun Topologi Leaf-Spine 3x3 (Cross-Leaf Setup)...\n")
        
        # 1. Buat 3 Spine Switches
        spine1 = self.addSwitch('spine1', dpid='1001')
        spine2 = self.addSwitch('spine2', dpid='1002')
        spine3 = self.addSwitch('spine3', dpid='1003')
        spines = [spine1, spine2, spine3]
            
        # 2. Buat 3 Leaf Switches
        leaf1 = self.addSwitch('leaf1', dpid='2001')
        leaf2 = self.addSwitch('leaf2', dpid='2002')
        leaf3 = self.addSwitch('leaf3', dpid='2003')
        leaves = [leaf1, leaf2, leaf3]
            
        # 3. Link Full Mesh (Setiap Leaf connect ke SEMUA Spine)
        # Bandwidth antar switch dibuat besar (1Gbps)
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000) 
        
        # 4. PENEMPATAN HOST (CRITICAL FOR THESIS)
        
        # --- LEAF 1 (Zona A) ---
        # User 1 (Penelpon) ditaruh disini
        user1 = self.addHost('user1', ip='10.0.0.1/24', mac='00:00:00:00:00:01')
        self.addLink(user1, leaf1, bw=100) # 100Mbps link to switch
        
        # --- LEAF 2 (Zona B - Background/Dummy) ---
        # Kita taruh Web Server dummy agar topologi terlihat hidup
        web1 = self.addHost('web1', ip='10.0.0.10/24', mac='00:00:00:00:00:10')
        self.addLink(web1, leaf2, bw=100)
        
        # --- LEAF 3 (Zona C - Seberang) ---
        # User 2 (Penerima) ditaruh disini.
        # Akibatnya: Paket dari User1 HARUS nyebrang lewat Spine untuk sampai kesini.
        user2 = self.addHost('user2', ip='10.0.0.2/24', mac='00:00:00:00:00:02')
        self.addLink(user2, leaf3, bw=100)

# ================= 3. SNIFFER (DATASET MAKER) =================
def packet_callback(pkt):
    """Fungsi ini dipanggil setiap ada paket lewat di interface yang dipantau"""
    if IP in pkt and UDP in pkt:
        src_ip = pkt[IP].src
        dst_ip = pkt[IP].dst
        length = len(pkt) # INI YANG PENTING (Packet Size)
        
        # Filter: Hanya catat paket milik User1 atau User2
        target_ips = {'10.0.0.1', '10.0.0.2'}
        
        if src_ip in target_ips or dst_ip in target_ips:
            with queue_lock:
                pkt_queue.append((
                    datetime.now(), 
                    src_ip, 
                    dst_ip, 
                    length, # bytes_tx diisi LENGTH paket
                    1,      # pkts_tx diisi 1
                    "normal" # Label
                ))

def sniffer_thread(net):
    info("*** [Sniffer] Starting Packet Capture on LEAF Switches...\n")
    
    # Kita sniff di interface switch LEAF yang mengarah ke Host
    # Tujuannya: Menangkap paket tepat saat masuk/keluar jaringan
    target_interfaces = []
    
    for sw in net.switches:
        # Cari switch yang namanya mengandung 'leaf'
        if 'leaf' in sw.name:
            for intf in sw.intfList():
                # Jangan sniff loopback
                if intf.name != 'lo':
                    target_interfaces.append(intf.name)
    
    info(f"*** Monitoring Interfaces: {target_interfaces}\n")
    
    # Menjalankan Scapy Sniff (Store=0 agar RAM tidak jebol)
    sniff(iface=target_interfaces, prn=packet_callback, filter="udp", store=0, 
          stop_filter=lambda x: stop_event.is_set())

def db_writer_thread():
    info("*** [DB Writer] Connected to Database.\n")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONFIG)
    except Exception as e:
        info(f"!!! DB Connection Failed: {e}\n")
        return

    while not stop_event.is_set():
        # Insert data setiap 1 detik (Batch Processing)
        time.sleep(1.0)
        
        rows_to_insert = []
        with queue_lock:
            if pkt_queue:
                rows_to_insert = list(pkt_queue)
                pkt_queue.clear()
        
        if rows_to_insert:
            try:
                cur = conn.cursor()
                # Query Insert
                q = """INSERT INTO traffic.flow_stats_ 
                       (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec, traffic_label) 
                       VALUES (%s, 0, %s, %s, '00:00', '00:00', 17, 0, 0, %s, 0, %s, 0, 0, %s)"""
                
                # Mapping data queue ke query
                final_data = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows_to_insert]
                
                cur.executemany(q, final_data)
                conn.commit()
                # print(f"-> DB: Saved {len(rows_to_insert)} packets.")
            except Exception as e:
                print(f"!!! SQL Error: {e}")
                conn.rollback()

    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR (VoIP) =================
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    with cmd_lock:
        try: node.cmd(cmd)
        except: pass

def run_voip_traffic(net):
    info("*** [Traffic] Starting VoIP Stream (User1 -> User2)...\n")
    
    # 1. Pastikan Receiver (User 2) Siap
    u2 = net.get('user2')
    safe_cmd(u2, "killall ITGRecv") # Bersihkan sisa lama
    safe_cmd(u2, "ITGRecv -l /tmp/user2_recv.log &") # Start Receiver log ke file
    
    time.sleep(2)
    
    u1 = net.get('user1')
    dst_ip = HOST_INFO['user1']['dst_ip'] # 10.0.0.2
    
    # Loop terus menerus sampai script di-stop
    while not stop_event.is_set():
        # --- PARAMETER DINAMIS (Agar mirip real traffic) ---
        
        # Target BPS acak antara 100k - 150k
        target_bps = random.randint(TARGET_BPS_MIN, TARGET_BPS_MAX)
        
        # Ukuran Paket acak (100 - 160 bytes) -> Length di DB akan bervariasi
        pkt_size = random.randint(100, 160)
        
        # Hitung Rate (Packet Per Second)
        # Rumus: Rate = Target_Bits / (Size_Bytes * 8)
        rate_pps = int(target_bps / (pkt_size * 8))
        if rate_pps < 1: rate_pps = 1
        
        # Durasi burst pendek (2 detik) lalu loop lagi
        # Ini membuat traffic lebih responsif
        duration = 2000 # ms
        
        # Log file untuk debug sender
        log_file = "/tmp/user1_sender.log"
        
        # Command D-ITG
        cmd = f"ITGSend -T UDP -a {dst_ip} -c {pkt_size} -C {rate_pps} -t {duration} > {log_file} 2>&1"
        
        # print(f"   + Sending: Size={pkt_size}B | Rate={rate_pps}pps | Target={target_bps/1000}kbps")
        safe_cmd(u1, cmd)
        
        # Tunggu sedikit sebelum inject lagi (sedikit jitter is okay)
        time.sleep(0.5)

# ================= 5. MAIN PROGRAM =================
def cleanup():
    info("*** Cleaning up processes...\n")
    os.system("killall -9 ITGSend ITGRecv > /dev/null 2>&1")
    os.system("mn -c > /dev/null 2>&1")

def signal_handler(sig, frame):
    info("\n*** Interrupted by User. Stopping...\n")
    stop_event.set()

if __name__ == "__main__":
    # Bersihkan sisa mininet sebelumnya
    cleanup()
    
    setLogLevel('info')
    
    # Init Topologi
    topo = LeafSpineTopo()
    
    # Gunakan Controller default (Learning Switch) agar koneksi L2 otomatis jalan
    # Switch menggunakan OVS agar bisa di-sniff
    net = Mininet(topo=topo, controller=Controller, switch=OVSKernelSwitch, link=TCLink)
    
    # Tangkap CTRL+C
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        net.start()
        
        info("*** Menunggu 5 detik untuk Network Convergence (STP)...\n")
        time.sleep(5)
        
        # Test Ping Dulu (Optional)
        # info("*** Testing Ping All...\n")
        # net.pingAll()
        
        # 1. Start Database Writer
        t_db = threading.Thread(target=db_writer_thread)
        t_db.start()
        
        # 2. Start Sniffer
        t_sniff = threading.Thread(target=sniffer_thread, args=(net,))
        t_sniff.start()

        # 3. Start Traffic
        run_voip_traffic(net)

    except Exception as e:
        info(f"!!! Error Fatal: {e}\n")
    finally:
        stop_event.set()
        time.sleep(2) # Beri waktu thread berhenti
        net.stop()
        cleanup()
        info("*** Simulation Finished.\n")
        sys.exit(0)