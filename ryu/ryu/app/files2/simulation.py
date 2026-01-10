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
    h1 = net.get('h1') # User Biasa (VoIP)
    h2 = net.get('h2') # Victim / Server
    h3 = net.get('h3') # Interfering Traffic / Attacker
    
    dst_ip = h2.IP()
    
    info("*** Starting Receiver on h2...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &') # Log lokal dimatikan supaya disk hemat
    time.sleep(2)
    
    info("*** Starting Continuous VoIP from h1 (Normal User)...\n")
    # H1 mengirim stabil ~100kbps (VoIP standard)
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 3600000 &')

    info("*** Starting CHAOS Generator on h3...\n")
    
    while True:
        # 1. Tentukan State: Normal, Warning, atau Critical (Congestion)
        # Probabilitas acak untuk mensimulasikan ketidakpastian
        state_prob = random.random()
        
        if state_prob < 0.5:
            # STATE: NORMAL (Idle / Browsing)
            # Rate rendah: 500 Kbps - 2 Mbps
            pps = random.randint(500, 2000)
            duration = random.randint(10, 30)
            mode = "NORMAL"
            
        elif state_prob < 0.8:
            # STATE: RAMP UP (Tanda-tanda bahaya)
            # Rate menengah: 3 Mbps - 6 Mbps
            # Ini pola yang harus dipelajari AI sebagai "Pre-Congestion"
            pps = random.randint(3000, 6000)
            duration = random.randint(5, 15)
            mode = "RAMP_UP"
            
        else:
            # STATE: CONGESTION (Burst/Spike)
            # Rate tinggi: 8 Mbps - 12 Mbps (Melibihi link 10Mbps)
            pps = random.randint(8000, 12000) 
            duration = random.randint(3, 10) # Burst biasanya singkat tapi fatal
            mode = "CONGESTION_SPIKE"

        info(f"[CHAOS GEN] Mode: {mode} | Rate: {pps} pps | Dur: {duration}s\n")
        
        # Jalankan D-ITG blocking (tunggu sampai durasi selesai)
        # Packet size 100 bytes. 10.000 pps * 100 bytes * 8 = 8 Mbps
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 100 -C {pps} -t {duration*1000} -l /dev/null')
        
        # Jeda acak antar flow (inter-arrival time variation)
        sleep_time = random.uniform(0.5, 3.0)
        time.sleep(sleep_time)

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