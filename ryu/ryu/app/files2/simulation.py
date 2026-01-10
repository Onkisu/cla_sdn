#!/usr/bin/env python3
"""
CHAOS TOPOLOGY & TRAFFIC GEN - STATE MACHINE EDITION
- Logic: Finite State Machine (Markov Chain-like)
- Flow: NORMAL <-> RAMP_UP -> CONGESTION -> RECOVERY -> NORMAL
- Purpose: Create learnable patterns for AI Forecasting
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import random
import threading

LINK_BW_MBPS = 10 

def traffic_generator(net):
    h1 = net.get('h1') 
    h2 = net.get('h2') 
    h3 = net.get('h3') # Attacker
    
    dst_ip = h2.IP()
    
    info("*** Starting Receiver on h2...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &') 
    time.sleep(2)
    
    info("*** Starting Continuous VoIP from h1 (Normal User)...\n")
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 3600000 &')

    info("*** Starting SMART CHAOS Generator on h3...\n")
    
    # INITIAL STATE
    current_state = "NORMAL"
    
    while True:
        # --- STATE MACHINE LOGIC ---
        # Logika transisi bertahap agar AI bisa melihat 'Acceleration'
        
        next_state = current_state # Default tetap
        prob = random.random()
        
        if current_state == "NORMAL":
            # 80% Stay Normal, 20% go to RAMP_UP
            if prob > 0.8: next_state = "RAMP_UP"
            
            pps = random.randint(500, 2000) # ~1 Mbps
            duration = random.randint(10, 20)
            
        elif current_state == "RAMP_UP":
            # 50% Back to Normal (False Alarm), 50% Attack (Congestion)
            if prob > 0.5: 
                next_state = "CONGESTION_SPIKE"
            else:
                next_state = "NORMAL"
            
            # Rate naik signifikan (Pattern Recognition Opportunity)
            pps = random.randint(4000, 7000) # ~4-5 Mbps
            duration = random.randint(5, 10) # Singkat, fase transisi
            
        elif current_state == "CONGESTION_SPIKE":
            # Harus Cooling Down setelah Spike
            next_state = "COOLING_DOWN"
            
            # Rate Meledak (> Link Capacity)
            pps = random.randint(9000, 13000) # ~8-10 Mbps (Packet Loss Area)
            duration = random.randint(5, 8) 
            
        elif current_state == "COOLING_DOWN":
            next_state = "NORMAL"
            pps = random.randint(1000, 3000)
            duration = random.randint(5, 10)

        info(f"[SMART GEN] State: {current_state} -> {next_state} | Rate: {pps} pps | Dur: {duration}s\n")
        
        # Eksekusi Traffic
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 100 -C {pps} -t {duration*1000} -l /dev/null')
        
        # Update State untuk putaran berikutnya
        current_state = next_state
        
        # Jeda minimal agar DB bisa mencatat row per detik
        time.sleep(1)

def run():
    # ... (Bagian Topology Tetap Sama seperti file asli Anda) ...
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)

    info("*** Adding Switches\n")
    s1 = net.addSwitch('s1', dpid='1')
    l1 = net.addSwitch('l1', dpid='2') # Connects to h1, h2
    l2 = net.addSwitch('l2', dpid='3') # Connects to h3

    info("*** Adding Hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    info("*** Creating Links\n")
    net.addLink(s1, l1, bw=100)
    net.addLink(s1, l2, bw=100)
    
    # Bottleneck links
    net.addLink(h1, l1, bw=LINK_BW_MBPS, max_queue_size=100) 
    net.addLink(h2, l1, bw=LINK_BW_MBPS, max_queue_size=100) 
    net.addLink(h3, l2, bw=LINK_BW_MBPS, max_queue_size=100)

    info("*** Starting Network\n")
    net.start()
    
    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()