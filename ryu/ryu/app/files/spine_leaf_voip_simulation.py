#!/usr/bin/env python3
"""
SIMPLIFIED Spine-Leaf VoIP Simulation + VARIABLE BURST ATTACK
- 3 Hosts only (h1, h2, h3) attached to separate Leaves
- Traffic h1 -> h2 (Steady VoIP)
- Traffic h3 -> h2 (BURSTY variable intervals: 20m -> 30m -> 10m -> repeat)
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading 

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (3 Hosts Version)\n")
    
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
        
    info("*** Creating Links (Spine-Leaf)\n")
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    info("*** Adding Hosts (Manual Assignment)\n")
    hosts = []
    
    # Host h1 -> Leaf l1
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    hosts.append(h1)
    
    # Host h2 -> Leaf l2
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    hosts.append(h2)
    
    # Host h3 -> Leaf l3
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')
    hosts.append(h3)

    info("*** Starting Network\n")
    net.start()
    
    time.sleep(3)
    
    info("*** Testing Connectivity (PingAll)\n")
    net.pingAll()
    
    if check_ditg():
        info("*** Starting D-ITG Traffic Setup\n")
        
        # 1. Setup Receiver di h2
        h2.cmd('ITGRecv -l /tmp/recv.log &')
        time.sleep(1)
        
        # 2. Main Traffic: h1 -> h2 (Steady VoIP - 1 Jam)
        dst_ip = h2.IP()
        info(f"    [STEADY] h1 -> h2 (UDP VoIP started for 1 hour)\n")
        h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 31536000000 -l /tmp/send_h1.log &')
        
        # 3. BURST Traffic Logic: h3 -> h2 (Variable Sequence)
        def burst_loop():
            # Urutan interval dalam MENIT (20 -> 30 -> 10 -> Ulang)
            intervals_minutes = [20, 30, 10]
            
            info(f"    [BURST] h3 armed with interval sequence: {intervals_minutes} minutes\n")
            
            while True:
                for mins in intervals_minutes:
                    seconds_to_wait = mins * 60
                    
                    info(f"\n[BURST SCHEDULER] Waiting {mins} minutes ({seconds_to_wait}s) before next attack...\n")
                    
                    # Tunggu sesuai jadwal saat ini
                    time.sleep(seconds_to_wait) 
                    
                    info(f"\n*** [BURST] h3 SENDING SPIKE TRAFFIC TO h2 (Duration: 60s) ***\n")
                    # Command Burst: Packet Size 1KB, Rate 500 pkt/s selama 60 detik (60000 ms)
                    h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 1024 -C 350 -t 60000 -l /tmp/send_h3_burst.log')
                    
                    info("*** [BURST] Done. Preparing for next cycle...\n")

        # Jalankan loop burst di thread terpisah
        t = threading.Thread(target=burst_loop)
        t.daemon = True 
        t.start()
            
    info("*** Running CLI\n")
    CLI(net)
    
    info("*** Stopping Network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()