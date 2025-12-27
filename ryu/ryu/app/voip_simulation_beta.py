#!/usr/bin/python3
"""
LEAF-SPINE 3x3 VOIP SIMULATION (PCAP STYLE OUTPUT)
Topology: 3 Spines, 3 Leaves (Full Mesh).
Traffic: VoIP (UDP) User1 -> User2.
Output: Mimics Wireshark/PCAP CSV Export.
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
from mininet.node import Controller, OVSKernelSwitch, RemoteController
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= 1. KONFIGURASI =================
DB_CONFIG = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# Target Traffic (Bits Per Second)
TARGET_BPS_MIN = 100000 
TARGET_BPS_MAX = 150000

# Host Configuration (Flat Subnet 10.0.0.x for L2 Fabric)
HOST_INFO = {
    'user1': {'ip': '10.0.0.1', 'dst_ip': '10.0.0.2', 'mac': '00:00:00:00:00:01'},
    'user2': {'ip': '10.0.0.2', 'dst_ip': '10.0.0.1', 'mac': '00:00:00:00:00:02'},
    # Background Host (Optional)
    'web1':  {'ip': '10.0.0.3', 'dst_ip': '10.0.0.1', 'mac': '00:00:00:00:00:03'}
}

# Global Variables
stop_event = threading.Event()
pkt_queue = []
queue_lock = threading.Lock()
cmd_lock = threading.Lock()

# Global Counter untuk kolom "No."
packet_counter = itertools.count(1)
start_time_ref = time.time() # Untuk menghitung relatif "time"

# ================= 2. TOPOLOGI LEAF-SPINE 3x3 =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Membangun Topologi Leaf-Spine 3x3...\n")
        
        # 1. Create Spines (Backbone)
        spines = [self.addSwitch(f'spine{i}') for i in range(1, 4)]
        
        # 2. Create Leaves (Access)
        leaves = [self.addSwitch(f'leaf{i}') for i in range(1, 4)]
        
        # 3. Full Mesh Links (Every Leaf connects to Every Spine)
        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000) # 1Gbps Backbone
        
        # 4. Host Placement (Cross-Fabric Traffic)
        # User 1 di Leaf 1
        user1 = self.addHost('user1', ip='10.0.0.1/24', mac='00:00:00:00:00:01')
        self.addLink(user1, leaves[0], bw=100)
        
        # Web 1 di Leaf 2 (Dummy)
        web1 = self.addHost('web1', ip='10.0.0.3/24', mac='00:00:00:00:00:03')
        self.addLink(web1, leaves[1], bw=100)
        
        # User 2 di Leaf 3 (Seberang)
        user2 = self.addHost('user2', ip='10.0.0.2/24', mac='00:00:00:00:00:02')
        self.addLink(user2, leaves[2], bw=100)

# ================= 3. CAPTURE ENGINE (PCAP STYLE) =================
def process_packet(pkt):
    """
    Fungsi ini dipanggil Scapy setiap ada paket.
    Mengekstrak data persis seperti format Wireshark.
    """
    if IP in pkt and UDP in pkt:
        src_ip = pkt[IP].src
        dst_ip = pkt[IP].dst
        
        # Filter: Hanya traffic User1 <-> User2
        target_ips = {'10.0.0.1', '10.0.0.2'}
        if src_ip not in target_ips or dst_ip not in target_ips:
            return

        # 1. Arrival Time (Absolute)
        arrival_time = datetime.now()
        
        # 2. Time (Relative float)
        rel_time = time.time() - start_time_ref
        
        # 3. Length (Bytes)
        length = len(pkt)
        
        # 4. Info String (Source Port > Dest Port Len=X)
        sport = pkt[UDP].sport
        dport = pkt[UDP].dport
        info_str = f"{sport} > {dport} Len={length}"
        
        # 5. No (Counter)
        no = next(packet_counter)
        
        # Masukkan ke Queue
        with queue_lock:
            pkt_queue.append({
                "time": rel_time,           # float8
                "source": src_ip,           # text
                "protocol": "UDP",          # text
                "length": length,           # int8
                "Arrival Time": arrival_time, # timestamp
                "info": info_str,           # text
                "No.": no,                  # int4
                "destination": dst_ip       # varchar(50)
            })

def sniffer_thread(net):
    info("*** [Sniffer] Starting Capture (Leaf Interfaces)...\n")
    # Monitor interface yang mengarah ke host di Leaf
    ifaces = []
    for sw in net.switches:
        if 'leaf' in sw.name:
            for intf in sw.intfList():
                if intf.name != 'lo':
                    ifaces.append(intf.name)
    
    # Scapy Sniff
    sniff(iface=ifaces, prn=process_packet, filter="udp", store=0, 
          stop_filter=lambda x: stop_event.is_set())

def db_writer_thread():
    info("*** [DB Writer] Connected.\n")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONFIG)
    except Exception as e:
        info(f"!!! DB Error: {e}\n")
        return

    while not stop_event.is_set():
        time.sleep(1.0) # Batch insert setiap 1 detik
        
        batch = []
        with queue_lock:
            if pkt_queue:
                batch = list(pkt_queue)
                pkt_queue.clear()
        
        if batch and conn:
            try:
                cur = conn.cursor()
                # Query Insert sesuai kolom permintaan
                q = """INSERT INTO pcap_logs 
                       ("time", "source", "protocol", "length", "Arrival Time", "info", "No.", "destination") 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                
                data_tuple = [(
                    r["time"], 
                    r["source"], 
                    r["protocol"], 
                    r["length"], 
                    r["Arrival Time"], 
                    r["info"], 
                    r["No."], 
                    r["destination"]
                ) for r in batch]
                
                cur.executemany(q, data_tuple)
                conn.commit()
                # print(f"-> Saved {len(batch)} packets.")
            except Exception as e:
                print(f"SQL Error: {e}")
                conn.rollback()
    
    if conn: conn.close()

# ================= 4. TRAFFIC GENERATOR (VoIP BPS) =================
def safe_cmd(node, cmd):
    if stop_event.is_set(): return
    with cmd_lock:
        try: node.cmd(cmd)
        except: pass

def run_voip_traffic(net):
    info("*** [Traffic] Starting VoIP Stream (100k - 150k bps)...\n")
    
    u1 = net.get('user1')
    u2 = net.get('user2')
    
    # Start Receiver di User 2
    safe_cmd(u2, "killall ITGRecv")
    safe_cmd(u2, "ITGRecv &")
    time.sleep(2)
    
    dst_ip = HOST_INFO['user1']['dst_ip']
    
    while not stop_event.is_set():
        # Randomize Parameters untuk VoIP
        # BPS Target: 100kbps - 150kbps
        target_bps = random.randint(TARGET_BPS_MIN, TARGET_BPS_MAX)
        
        # Packet Size: 100 - 160 Bytes (Typical VoIP G.711)
        pkt_size = random.randint(100, 160)
        
        # Hitung Packet Rate (PPS)
        # Rate = Target_Bits / (Size_Bytes * 8)
        rate_pps = int(target_bps / (pkt_size * 8))
        if rate_pps < 1: rate_pps = 1
        
        # Kirim traffic selama 2 detik
        duration = 2000 
        cmd = f"ITGSend -T UDP -a {dst_ip} -c {pkt_size} -C {rate_pps} -t {duration} > /dev/null 2>&1"
        
        safe_cmd(u1, cmd)
        time.sleep(0.5) # Jeda kecil

# ================= 5. MAIN & CLEANUP =================
def cleanup():
    os.system("sudo killall -9 ITGSend ITGRecv python3 > /dev/null 2>&1")
    os.system("sudo mn -c > /dev/null 2>&1")

def signal_handler(sig, frame):
    if stop_event.is_set():
        info("\n!!! FORCE EXIT !!!\n")
        os._exit(1)
    info("\n*** Stopping... (Press Ctrl+C again to Force Kill)\n")
    stop_event.set()

    
if __name__ == "__main__":
    # 1. BERSIHKAN SISA PROSES LAMA (PENTING)
    cleanup()
    
    setLogLevel('info')
    
    topo = LeafSpineTopo()
    
    # Kita gunakan OVSKernelSwitch
    net = Mininet(topo=topo, controller=Controller, switch=OVSKernelSwitch, link=TCLink)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        net.start()
        
        # ==========================================
        # PERBAIKAN: AKTIFKAN STP (PENTING!)
        # Agar RAM tidak meledak karena looping
        # ==========================================
        info("*** Enabling STP to prevent Loops/Broadcast Storms...\n")
        for sw in net.switches:
            # Command manual ke OVS untuk nyalakan Spanning Tree
            sw.cmd(f'ovs-vsctl set Bridge {sw.name} stp_enable=true')
        
        info("*** Menunggu 15 detik untuk STP Convergence (Wajib Lama)...\n")
        # STP butuh waktu 15-30 detik untuk memblokir path yang loop
        time.sleep(15) 
        
        info("*** Ping All untuk mengisi ARP Table...\n")
        # Jika STP belum kelar, ini bisa gagal sebagian, tapi tidak akan bikin crash
        net.pingAll()
        
        # Start Threads
        t_db = threading.Thread(target=db_writer_thread)
        t_db.daemon = True 
        t_db.start()
        
        t_sniff = threading.Thread(target=sniffer_thread, args=(net,))
        t_sniff.daemon = True
        t_sniff.start()
        
        run_voip_traffic(net)
        
    except Exception as e:
        info(f"!!! Error: {e}\n")
    finally:
        stop_event.set()
        time.sleep(1)
        net.stop()
        cleanup()