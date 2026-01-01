#!/usr/bin/env python3
"""
FIXED Mininet Spine-Leaf Topology with DYNAMIC TRAFFIC GENERATION
- Implements 'Rising and Falling' traffic pattern (30 mins up, 30 mins down).
- Controls D-ITG dynamically from Python to achieve specific byte rates.
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import math
import random
import threading
import subprocess

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

# --- LOGIKA TRAFFIC GENERATOR ---
def traffic_pattern_runner(net, hosts):
    """
    Fungsi ini berjalan di background thread.
    Mengupdate rate pengiriman D-ITG setiap 'interval' detik
    agar sesuai dengan pola gelombang.
    """
    start_time = time.time()
    
    # Konfigurasi Pola
    min_bytes = 13000
    max_bytes = 19800
    period = 3600 # 1 jam
    packet_payload = 160 # Bytes per packet (VoIP G.711 approx)
    update_interval = 5   # Update rate tiap 5 detik (agar transisi halus)

    info(f"*** Traffic Generator Started: {min_bytes}-{max_bytes} Bytes/sec cycle\n")

    while True:
        elapsed = time.time() - start_time
        
        # 1. Hitung Target Bytes (Matematika Cosine)
        # Menit 0-30 Naik, 30-60 Turun
        cycle_pos = (elapsed % period) / period
        phase = cycle_pos * 2 * math.pi
        
        base = (max_bytes + min_bytes) / 2
        amplitude = (max_bytes - min_bytes) / 2
        
        # Pola dasar kurva (-cos)
        curve_value = base - (amplitude * math.cos(phase))
        
        # 2. Tambah Noise (Supaya tidak mulus/grafik kasar)
        # Random +/- 10%
        noise = random.uniform(-0.10, 0.10)
        target_bytes = int(curve_value + (curve_value * noise))
        
        # Safety limit
        target_bytes = max(10000, target_bytes)
        
        # 3. Konversi Bytes ke Packets per Second (PPS) untuk D-ITG
        # D-ITG option: -C (packets per second), -c (packet size)
        # Overhead Ethernet+IP+UDP approx 42 bytes. Total wire size approx 200 bytes.
        # Tapi controller menghitung payload+header. Kita setting Packet Rate (PPS).
        pps = int(target_bytes / (packet_payload + 42)) 
        
        # 4. Eksekusi Command ke Host
        # Kita jalankan burst selama update_interval detik
        # command: ITGSend -T UDP -a DST -c SIZE -C RATE -t DURATION
        
        # Kirim traffic dari host ganjil ke genap (h1->h2, h3->h4, dst)
        # Hanya ambil sample host pertama (h1->h2) agar tidak overload CPU laptop
        # Atau bisa semua pasang. Kita coba 1 pasang utama dulu biar grafik jelas.
        
        sender = hosts[0] # h1
        receiver = hosts[1] # h2
        
        # Jalankan D-ITG di background (&)
        # -t time dalam milisecond (5 detik = 5000)
        cmd = f"ITGSend -T UDP -a {receiver.IP()} -c {packet_payload} -C {pps} -t {update_interval*1000} &"
        sender.cmd(cmd)
        
        # Debug Log (Optional, biar tau rate sekarang)
        # info(f"[Traffic] Time: {int(elapsed)}s | Target: {target_bytes}B | Rate: {pps} pps\n")
        
        time.sleep(update_interval)

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (Real Traffic Gen)\n")
    
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)
    
    spines = []
    leaves = []
    
    # Create Spines (DPID 1-3)
    for i in range(1, 4):
        s = net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13')
        spines.append(s)
        
    # Create Leaves (DPID 4-6)
    for i in range(1, 4):
        l = net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13')
        leaves.append(l)
        
    info("*** Creating Links\n")
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    info("*** Adding Hosts\n")
    hosts = []
    host_id = 1
    for leaf in leaves:
        for j in range(2): 
            h = net.addHost(f'h{host_id}', ip=f'10.0.0.{host_id}/24')
            net.addLink(h, leaf, bw=100, delay='1ms')
            hosts.append(h)
            host_id += 1

    info("*** Starting Network\n")
    net.start()
    time.sleep(3)
    
    net.pingAll()
    
    if check_ditg():
        info("*** Starting Traffic Receiver\n")
        # Start Receiver di H2
        hosts[1].cmd('ITGRecv -l /tmp/recv.log &')
        
        info("*** Starting Dynamic Traffic Generator Thread\n")
        # Jalankan thread terpisah agar CLI Mininet tetap jalan
        t = threading.Thread(target=traffic_pattern_runner, args=(net, hosts))
        t.daemon = True # Thread mati kalau script utama mati
        t.start()
            
    info("*** Running CLI\n")
    CLI(net)
    
    info("*** Stopping Network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()