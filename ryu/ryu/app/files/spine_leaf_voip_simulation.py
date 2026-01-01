#!/usr/bin/env python3
"""
SIMPLIFIED Spine-Leaf VoIP Simulation + BURST ATTACK
- 3 Hosts only (h1, h2, h3) attached to separate Leaves
- Traffic h1 -> h2 (Steady VoIP)
- Traffic h3 -> h2 (BURSTY every 30s to create spikes)
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading # Added for background burst task

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (3 Hosts Version)\n")
    
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    
    info("*** Adding Controller\n")
    # Arahkan ke IP lokal dimana script python Ryu jalan
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
        
    info("*** Creating Links (Spine-Leaf)\n")
    # Connect Leaves to Spines FIRST (Port 1-3 on Leaf -> Spines)
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    info("*** Adding Hosts (Manual Assignment)\n")
    # Kita sebar host ke leaf yang berbeda supaya komunikasi harus lewat spine
    hosts = []
    
    # Host h1 -> Leaf l1
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    net.addLink(h1, leaves[0], bw=100, delay='1ms') # leaves[0] adalah l1
    hosts.append(h1)
    
    # Host h2 -> Leaf l2
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    net.addLink(h2, leaves[1], bw=100, delay='1ms') # leaves[1] adalah l2
    hosts.append(h2)
    
    # Host h3 -> Leaf l3
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    net.addLink(h3, leaves[2], bw=100, delay='1ms') # leaves[2] adalah l3
    hosts.append(h3)

    info("*** Starting Network\n")
    net.start()
    
    # Tunggu sebentar agar Switch connect ke Controller
    time.sleep(3)
    
    info("*** Testing Connectivity (PingAll)\n")
    net.pingAll()
    
    if check_ditg():
        info("*** Starting D-ITG Traffic Setup\n")
        
        # 1. Setup Receiver di h2 (Menerima dari h1 dan h3)
        h2.cmd('ITGRecv -l /tmp/recv.log &')
        time.sleep(1)
        
        # 2. Main Traffic: h1 -> h2 (Steady VoIP - 1 Jam)
        dst_ip = h2.IP()
        info(f"    [STEADY] h1 -> h2 (UDP VoIP started for 1 hour)\n")
        h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 3600000 -l /tmp/send_h1.log &')
        
        # 3. BURST Traffic Logic: h3 -> h2 (Setiap 30 menit)
        def burst_loop():
            info(f"    [BURST] h3 armed to attack h2 every 30 seconds...\n")
            while True:
                time.sleep(1800) # Tunggu 30 menit
                info("\n*** [BURST] h3 SENDING SPIKE TRAFFIC TO h2 (5 seconds) ***\n")
                # Command Burst: Packet Size 1KB, Rate 8000 pkt/s (~65Mbps), Duration 5s
                # Ini cukup besar untuk membebani link h2 (bw=100Mbps) jika digabung traffic h1
                h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 1024 -C 8000 -t 5000 -l /tmp/send_h3_burst.log')
                info("*** [BURST] Done. Waiting next cycle...\n")

        # Jalankan loop burst di thread terpisah agar tidak memblokir CLI
        t = threading.Thread(target=burst_loop)
        t.daemon = True # Thread mati otomatis kalau program utama stop
        t.start()
            
    info("*** Running CLI\n")
    CLI(net)
    
    info("*** Stopping Network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()