#!/usr/bin/env python3
"""
BURSTY Spine-Leaf Simulation
- h1 -> h2: VoIP Traffic (Stabil 1 Jam)
- h3 -> h2: BURST Attack (5 detik ON, 25 detik OFF) - Setiap 30 Detik
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (With Burst Attack)\n")
    
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)
    
    spines = []
    leaves = []
    
    # Spines
    for i in range(1, 4):
        s = net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13')
        spines.append(s)
        
    # Leaves
    for i in range(1, 4):
        l = net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13')
        leaves.append(l)
        
    info("*** Creating Links\n")
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    info("*** Adding Hosts\n")
    hosts = []
    
    # h1 di l1
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    hosts.append(h1)
    
    # h2 di l2 (Target Korban)
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    hosts.append(h2)
    
    # h3 di l3 (Penyerang / Pengganggu)
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')
    hosts.append(h3)

    info("*** Starting Network\n")
    net.start()
    time.sleep(3)
    
    info("*** Testing Connectivity\n")
    net.pingAll()
    
    if check_ditg():
        info("*** Starting Traffic Scenarios\n")
        
        # 1. Receiver di h2 (Menerima dari h1 dan h3)
        h2.cmd('ITGRecv -l /tmp/recv.log &')
        time.sleep(1)
        
        # 2. Normal VoIP: h1 -> h2 (Stabil)
        info("    h1 -> h2: Normal VoIP (Started)\n")
        h1.cmd(f'ITGSend -T UDP -a {h2.IP()} -c 160 -C 50 -t 3600000 -l /tmp/send_voip.log &')
        
        # 3. BURST Traffic: h3 -> h2
        # -O 5000  : ON selama 5000ms (5 detik)
        # -V 25000 : OFF selama 25000ms (25 detik)
        # Total Cycle = 30 Detik
        # Rate: -c 500 -C 200 = 100.000 Bytes/sec (Jauh diatas Sine Wave 19.000) -> SPIKE!
        info("    h3 -> h2: BURST Attack (5s ON, 25s OFF) - Every 30s\n")
        h3.cmd(f'ITGSend -T UDP -a {h2.IP()} -c 500 -C 200 -t 3600000 -O 5000 -V 25000 -l /tmp/send_burst.log &')
            
    info("*** Running CLI\n")
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()