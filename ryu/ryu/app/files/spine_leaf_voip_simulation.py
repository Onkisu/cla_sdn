#!/usr/bin/env python3
"""
FIXED MOUNTAIN BURST (ISOLATED PORTS)
- h1 -> h2 (Port 9000): AMAN, tidak akan putus.
- h3 -> h2 (Port 9001): GUNUNG, naik turun tiap detik tanpa ganggu h1.
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading 
import random 

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (Fixed Version)\n")
    
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    net.addController('c0', ip='127.0.0.1', port=6653)
    
    spines = []
    leaves = []
    
    # Topology Setup
    for i in range(1, 4):
        s = net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13')
        spines.append(s)
    for i in range(1, 4):
        l = net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13')
        leaves.append(l)
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    # Hosts
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    info("*** Starting Network & PingAll\n")
    net.start()
    time.sleep(3)
    net.pingAll()
    
    if check_ditg():
        info("*** Setup Traffic Receiver (ISOLATED PORTS)\n")
        
        # --- KUNCI PERBAIKAN DI SINI ---
        # 1. Receiver Utama (Untuk h1) - Port Default (9000)
        h2.cmd('ITGRecv -l /tmp/recv_h1_voip.log &')
        # 2. Receiver Burst (Untuk h3) - Port 9001 (WAJIB ADA LAGI)
        h2.cmd('ITGRecv -Sp 9001 -l /tmp/recv_h1_voip.log &')
    
        
        time.sleep(1)
        
        # -----------------------------------------------------------
        # TRAFIK 1: h1 -> h2 (Steady VoIP)
        # -----------------------------------------------------------
        dst_ip = h2.IP()
        info(f"    [STEADY] h1 -> h2 running on Port 9000 (Safe)\n")
        # Menggunakan durasi 24 jam (86400 detik) agar tidak overflow
        h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 86400000 -l /dev/null &')
        
        # -----------------------------------------------------------
        # TRAFIK 2: h3 -> h2 (Mountain Burst)
        # -----------------------------------------------------------
        def burst_loop():
            # Interval waktu antar serangan (menit)
            intervals_minutes = [2, 3, 4]
            
            # Settingan Gunung
            duration_sec = 30
            peak_rate = 1000
            base_rate = 500
            noise_factor = 0.3
            
            # Rumus Parabola
            midpoint = duration_sec / 2
            curvature = (peak_rate - base_rate) / (midpoint ** 2)

            info(f"    [BURST] h3  Pattern: Mountain.\n")
            
            while True:
                for mins in intervals_minutes:
                    # Ganti jadi (mins * 60) untuk real time, atau angka kecil untuk test
                    seconds_to_wait = mins * 60 
                    
                    info(f"\n[SCHEDULER] Waiting {mins} minutes...\n")
                    time.sleep(seconds_to_wait) 
                    
                    info(f"\n*** [ATTACK START] h3 sending Mountain Burst to h2:9001 ***\n")
                    
                    # Looping Detik-per-Detik untuk membentuk gunung
                    for t in range(duration_sec + 1):
                        # 1. Hitung Rate
                        dist = t - midpoint
                        clean = peak_rate - (curvature * (dist ** 2))
                        noise = random.uniform(-noise_factor, noise_factor)
                        final = int(clean * (1 + noise))
                        if final < base_rate: final = base_rate
                        
                        # 2. Visualisasi
                        bar = "#" * int(final / 10)
                        info(f"\r t={t:02d} | Rate={final:03d} | {bar}")
                        
                        # 3. TEMBAK D-ITG
        
                        # -t 1000   : Durasi 1 detik
                        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -Sdp 9001 -c 160 -C {final} -t 1000 -l /dev/null &')
                        time.sleep(1)
                        
                    
                    print("")
                    info("*** [ATTACK END] Cycle finished.\n")

        t = threading.Thread(target=burst_loop)
        t.daemon = True 
        t.start()
            
    info("*** Running CLI\n")
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()

