#!/usr/bin/env python3
"""
CHAOS TOPOLOGY & TRAFFIC GEN
- Bandwidth Limited to 10Mbps to create easily reachable congestion thresholds.
- Non-deterministic traffic generation logic.
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import random
import threading
import subprocess

# Target Congestion: Link Capacity is 10 Mbps
LINK_BW_MBPS = 10 
# Normal Traffic: 2-4 Mbps
# Congestion Traffic: > 8 Mbps
def traffic_generator(net):
    h1 = net.get('h1')
    h2 = net.get('h2')
    h3 = net.get('h3')
    dst_ip = h2.IP()
    
    info("*** Starting Receiver & VoIP...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &')
    time.sleep(1)
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 3600000 &') # Stabil ~64kbps

    # STATE MACHINE VARIABLES
    current_state = "NORMAL"
    
    while True:
        # Logika Transisi State (Pola untuk dipelajari AI)
        # NORMAL -> Bisa ke RAMP_UP (20%) atau tetap NORMAL (80%)
        # RAMP_UP -> Bisa ke CONGESTION (70%) atau kembali NORMAL (30%)
        # CONGESTION -> Pasti ke COOLING_DOWN
        
        if current_state == "NORMAL":
            if random.random() < 0.3: # 30% chance naik ke warning
                current_state = "RAMP_UP"
                pps = random.randint(4000, 6000) # ~3-5 Mbps (Pre-congestion)
                duration = random.randint(5, 10)
            else:
                current_state = "NORMAL"
                pps = random.randint(500, 2000)  # ~0.5-1.5 Mbps
                duration = random.randint(10, 20)

        elif current_state == "RAMP_UP":
            if random.random() < 0.7: # 70% chance jadi Congestion beneran
                current_state = "CONGESTION"
                pps = random.randint(9000, 12000) # ~7-9 Mbps (High Load)
                duration = random.randint(5, 8)   # Spike durasi pendek
            else:
                current_state = "NORMAL" # False alarm, balik normal
                pps = random.randint(1000, 3000)
                duration = random.randint(5, 10)

        elif current_state == "CONGESTION":
            current_state = "NORMAL" # Setelah spike, pasti cooling down
            pps = random.randint(100, 1000)
            duration = random.randint(8, 15)

        info(f"[CHAOS GEN] State: {current_state} | Rate: {pps} pps | Dur: {duration}s\n")
        
        # Eksekusi Traffic
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 100 -C {pps} -t {duration*1000} -l /dev/null')
        time.sleep(random.uniform(0.1, 1.0))
        
def run():
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)

    info("*** Adding Switches\n")
    # Topology Spine Leaf Sederhana
    s1 = net.addSwitch('s1', dpid='1') # Spine
    l1 = net.addSwitch('l1', dpid='2') # Leaf 1
    l2 = net.addSwitch('l2', dpid='3') # Leaf 2

    info("*** Adding Hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    info("*** Creating Links (With Bottleneck)\n")
    # Spine - Leaf (Backbone kuat, 100Mbps)
    net.addLink(s1, l1, bw=100)
    net.addLink(s1, l2, bw=100)
    
    # Leaf - Host (BOTTLENECK 10 Mbps)
    # Max Queue Size dibatasi agar packet drop terjadi saat congestion
    net.addLink(h1, l1, bw=LINK_BW_MBPS, max_queue_size=100, delay='1ms') 
    net.addLink(h2, l1, bw=LINK_BW_MBPS, max_queue_size=100, delay='1ms') # Target Link
    net.addLink(h3, l2, bw=LINK_BW_MBPS, max_queue_size=100, delay='1ms')

    info("*** Starting Network\n")
    net.start()
    net.pingAll()

    # Jalankan Traffic Generator di Thread terpisah
    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()