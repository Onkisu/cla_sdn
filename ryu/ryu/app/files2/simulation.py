#!/usr/bin/env python3
"""
FINAL STABLE SIMULATION
- Queue: 10 Packets (Strict dropping for VoIP realism)
- Safety: Background execution (nohup style) to prevent blocking/crashing
- Scenario: VoIP Trunk (h1) vs UDP Flood (h3)
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import random
import threading

# KONFIGURASI
LINK_BW_MBPS = 10
QUEUE_SIZE = 10 # Buffer sangat kecil -> Paksa Hardware Drop saat macet

def traffic_generator(net):
    h1 = net.get('h1') 
    h2 = net.get('h2') 
    h3 = net.get('h3') 
    
    dst_ip = h2.IP()
    
    info("*** Starting Receiver on h2 (Safe Mode)...\n")
    # Kill process lama dengan aman (|| true biar gak error exit code)
    h2.cmd('killall -9 ITGRecv || true') 
    time.sleep(1)
    
    # Jalankan Receiver di background, buang log ke null
    h2.cmd('ITGRecv -l /dev/null > /dev/null 2>&1 &') 
    time.sleep(2)
    
    info("*** Starting VoIP Trunk (Normal User)...\n")
    # 20 concurrent calls simulasi
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 172 -C 1000 -t 3600000 > /dev/null 2>&1 &')

    info("*** Starting SMART ATTACK Generator on h3...\n")
    
    current_state = "NORMAL"
    
    # Bungkus Loop utama dengan Try-Except biar Thread gak mati diam-diam
    try:
        while True:
            next_state = current_state
            prob = random.random()
            
            # --- STATE MACHINE ---
            if current_state == "NORMAL":
                if prob > 0.8: next_state = "RAMP_UP"
                pkt_size = 1000 
                pps = random.randint(100, 300) 
                duration = random.randint(10, 20)
                
            elif current_state == "RAMP_UP":
                if prob > 0.6: next_state = "CONGESTION_SPIKE"
                else: next_state = "NORMAL"
                pkt_size = 1400 
                pps = random.randint(500, 800) 
                duration = random.randint(5, 10)
                
            elif current_state == "CONGESTION_SPIKE":
                next_state = "COOLING_DOWN"
                # UDP Flood Packet Kecil (Musuh VoIP)
                pkt_size = 64 
                pps = random.randint(12000, 15000) 
                duration = random.randint(4, 7) 
                
            elif current_state == "COOLING_DOWN":
                next_state = "NORMAL"
                pkt_size = 500
                pps = random.randint(200, 500)
                duration = random.randint(5, 10)

            info(f"[GEN] State: {current_state} | Rate: {pps} pps | Dur: {duration}s\n")
            
            # Eksekusi Attack
            h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c {pkt_size} -C {pps} -t {duration*1000} -l /dev/null > /dev/null 2>&1')
            
            current_state = next_state
            
            # Sleep aman
            time.sleep(0.5)
            
    except Exception as e:
        info(f"!!! Traffic Generator Error: {e}\n")

def run():
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)

    info("*** Adding Switches\n")
    s1 = net.addSwitch('s1', dpid='1')
    l1 = net.addSwitch('l1', dpid='2') 
    l2 = net.addSwitch('l2', dpid='3') 

    info("*** Adding Hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    info("*** Creating Links (VoIP Tuning)\n")
    net.addLink(s1, l1, bw=100)
    net.addLink(s1, l2, bw=100)
    
    # Bottleneck Links dengan Queue Sangat Kecil (10)
    # Ini kuncinya biar "Hardware Drop" ke-detect Controller
    net.addLink(h1, l1, bw=LINK_BW_MBPS, max_queue_size=QUEUE_SIZE) 
    net.addLink(h2, l1, bw=LINK_BW_MBPS, max_queue_size=QUEUE_SIZE) 
    net.addLink(h3, l2, bw=LINK_BW_MBPS, max_queue_size=QUEUE_SIZE)

    info("*** Starting Network\n")
    net.start()
    
    # Tunggu switch connect ke controller sebelum gen traffic
    info("*** Waiting for controller connection...\n")
    net.waitConnected()
    
    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()