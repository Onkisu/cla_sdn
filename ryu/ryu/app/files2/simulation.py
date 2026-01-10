#!/usr/bin/env python3
"""
THE FIXED SIMULATION
- Topology: Spine-Leaf
- Victim: Host h2 (10.0.0.2)
- Bottleneck: 10Mbps di link l1 <-> h2
- Mapping Interface Terjamin: l1-eth3 ADALAH LINK KE VICTIM
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
    h1 = net.get('h1') # User
    h2 = net.get('h2') # Victim
    h3 = net.get('h3') # Attacker
    
    dst_ip = h2.IP()
    
    info("*** Starting Server on h2...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &') 
    time.sleep(2)
    
    # Traffic Background (User Normal)
    info("*** User h1 starting VoIP traffic...\n")
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 100 -C 50 -t 3600000 &')

    # Traffic Attack (Naik turun biar grafik bagus)
    info("*** Attacker h3 initializing...\n")
    while True:
        # Generate burst traffic acak
        rate = random.choice([500, 2000, 8000, 15000]) # PPS
        duration = random.randint(5, 10)
        info(f"⚡ ATTACK: {rate} PPS for {duration}s\n")
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 100 -C {rate} -t {duration*1000} -l /dev/null')
        time.sleep(duration + 1)

def run():
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    
    info("*** Adding Controller\n")
    c0 = net.addController('c0', ip='127.0.0.1', port=6653)

    info("*** Adding Switches\n")
    s1 = net.addSwitch('s1', dpid='1')
    l1 = net.addSwitch('l1', dpid='2') # Switch Korban
    l2 = net.addSwitch('l2', dpid='3') # Switch Attacker

    info("*** Adding Hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2') # VICTIM
    h3 = net.addHost('h3', ip='10.0.0.3')

    info("*** Wiring Links (URUTAN PENTING!)\n")
    # Link Backbone (100Mbps)
    net.addLink(s1, l1, bw=100) # l1-eth1
    net.addLink(s1, l2, bw=100) # l2-eth1
    
    # Link Host
    net.addLink(h1, l1, bw=100)      # l1-eth2 -> h1
    
    # === THE BOTTLENECK ===
    # Port 3 di l1 nyambung ke h2. Interface: l1-eth3
    net.addLink(h2, l1, bw=10, max_queue_size=100) 
    
    net.addLink(h3, l2, bw=100)      # l2-eth2 -> h3

    info("*** Starting Network\n")
    net.build()
    c0.start()
    s1.start([c0])
    l1.start([c0])
    l2.start([c0])

    # Pastikan interface up
    info("*** Verifying Interfaces...\n")
    print("--- CHECKING: l1-eth3 should exist ---")
    l1.cmd('ip link set l1-eth3 up')

    # Start Traffic Thread
    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()