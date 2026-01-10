#!/usr/bin/env python3
"""
SPINE-LEAF WITH STP
- Enabled STP to prevent loops/broadcast storms.
- Link Host 10Mbps (Bottleneck).
- Traffic Chaos Pareto.
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import random
import threading

def traffic_generator(net):
    h1 = net.get('h1')
    h2 = net.get('h2') 
    h3 = net.get('h3') 
    dst_ip = h2.IP()
    
    info("*** Starting Receiver on h2...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &')
    time.sleep(2)
    
    info("*** Starting Background Traffic h1 -> h2...\n")
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 3600000 &')

    info("*** Starting CHAOS Generator on h3...\n")
    current_pps = 500
    trend = 1.0 
    
    while True:
        # Pareto Chaos Logic
        change = random.uniform(0.9, 1.15) 
        trend = trend * change
        if trend > 2.0: trend = 1.8
        if trend < 0.5: trend = 0.6
        target_pps = int(current_pps * trend)
        
        if random.random() < 0.15: # Burst
            real_pps = int(target_pps * random.uniform(1.5, 3.0))
            duration = random.uniform(1.0, 3.0)
            mode = "BURST"
        else:
            real_pps = target_pps
            duration = random.uniform(2.0, 4.0)
            mode = "FLOW"
            
        if real_pps > 12000: real_pps = 12000
        if real_pps < 100: real_pps = 100
        packet_size = random.choice([200, 500, 1000, 1400])
        
        # Kirim Traffic
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c {packet_size} -C {real_pps} -t {int(duration*1000)} -l /dev/null')
        current_pps = real_pps if mode == "FLOW" else target_pps
        
        # Log status
        mbps = (real_pps * packet_size * 8) / 1e6
        info(f"[GEN] {mode} | {real_pps} pps | {mbps:.2f} Mbps\n")
        time.sleep(0.1)

def run():
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)

    info("*** Adding Switches (Enabled STP)\n")
    spines = []
    leaves = []
    
    # KUNCI UTAMA: stp=True agar tidak terjadi Loop
    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', stp=True))
    
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', stp=True))

    info("*** Creating Links\n")
    # Backbone
    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000)

    # Hosts (Bottleneck 10Mbps)
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    net.addLink(h1, leaves[0], bw=10, max_queue_size=20, delay='1ms')
    net.addLink(h2, leaves[1], bw=10, max_queue_size=20, delay='1ms')
    net.addLink(h3, leaves[2], bw=10, max_queue_size=20, delay='1ms')

    info("*** Starting Network\n")
    net.start()
    
    info("⏳ WAITING 45 SECONDS FOR STP CONVERGENCE (DO NOT INTERRUPT)...\n")
    time.sleep(45) # Wajib tunggu STP
    
    info("*** Pinging All to verify connectivity...\n")
    net.pingAll() # Sekarang harusnya aman

    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()