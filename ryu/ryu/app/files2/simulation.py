#!/usr/bin/env python3
"""
VOIP SPECIALIZED TOPOLOGY
- Scenario: Small Office VoIP Trunk (h1) vs UDP Flood (h3)
- VoIP Spec: G.711 Codec (64kbps + overhead ~ 87kbps), 50 pps/call.
- Link: 10 Mbps (Bottleneck).
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import random
import threading

# KONFIGURASI LINK
LINK_BW_MBPS = 10
# VoIP Buffer harus KECIL. 
# Jika buffer besar, suara jadi delay/robot. Jika kecil, suara putus-putus.
# Kita set 20 paket (sangat ketat) agar simulasi drop lebih cepat terjadi.
QUEUE_SIZE = 20 

def traffic_generator(net):
    h1 = net.get('h1') # VOIP TRUNK (Korban)
    h2 = net.get('h2') # VOIP SERVER (Tujuan)
    h3 = net.get('h3') # ATTACKER
    
    dst_ip = h2.IP()
    
    info("*** Starting Receiver on h2...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &') 
    time.sleep(2)
    
    # --- 1. SETTING VOIP NORMAL (h1) ---
    # Simulasi: 20 Concurrent Calls (Kantor sedang sibuk)
    # Spec G.711: 50 pps/call x 20 calls = 1000 pps
    # Size: 172 bytes (160 payload + 12 RTP header)
    info("*** Starting VoIP Trunk (20 Active Calls) from h1...\n")
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 172 -C 1000 -t 3600000 &')

    info("*** Starting SMART ATTACK Generator on h3...\n")
    
    current_state = "NORMAL"
    
    while True:
        next_state = current_state
        prob = random.random()
        
        # --- STATE MACHINE KHUSUS VOIP ---
        # VoIP sangat sensitif. Gangguan sedikit saja (Ramp Up) sudah menurunkan MOS (Mean Opinion Score).
        
        if current_state == "NORMAL":
            # 80% Aman, 20% mulai ada gangguan
            if prob > 0.8: next_state = "RAMP_UP"
            
            # Trafik Background (Browsing biasa, paket besar, rate rendah)
            pkt_size = 1000 
            pps = random.randint(100, 300) 
            duration = random.randint(10, 20)
            
        elif current_state == "RAMP_UP":
            # Fase ini mensimulasikan "Video Streaming" masuk ke jaringan
            if prob > 0.6: next_state = "CONGESTION_SPIKE"
            else: next_state = "NORMAL"
            
            pkt_size = 1400 # Paket Video (Besar)
            pps = random.randint(500, 800) # ~6-8 Mbps
            duration = random.randint(5, 10)
            
        elif current_state == "CONGESTION_SPIKE":
            # ATTACK: UDP FLOOD (Small Packets)
            # Ini musuh alami VoIP. Paket kecil membanjiri antrian switch (PPS tinggi).
            next_state = "COOLING_DOWN"
            
            pkt_size = 64 # Minimum packet size (Sangat jahat untuk CPU switch)
            # Link 10Mbps / 64 bytes ~= 19.000 PPS Max Theoretical
            # Kita hajar di 15.000 PPS agar switch 'choke'
            pps = random.randint(12000, 15000) 
            duration = random.randint(4, 7) 
            
        elif current_state == "COOLING_DOWN":
            next_state = "NORMAL"
            pkt_size = 500
            pps = random.randint(200, 500)
            duration = random.randint(5, 10)

        info(f"[VOIP GEN] State: {current_state} | PktSize: {pkt_size}B | Rate: {pps} pps | Dur: {duration}s\n")
        
        # Eksekusi Attack
        # -l /dev/null agar log tidak memenuhi disk
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c {pkt_size} -C {pps} -t {duration*1000} -l /dev/null')
        
        current_state = next_state
        time.sleep(0.5)

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

    info("*** Creating Links (VoIP Optimized)\n")
    net.addLink(s1, l1, bw=100)
    net.addLink(s1, l2, bw=100)
    
    # Bottleneck Links
    # max_queue_size diperkecil ke 20 untuk simulasi realitas VoIP:
    # "Lebih baik drop paket daripada delay lama (bufferbloat)"
    net.addLink(h1, l1, bw=LINK_BW_MBPS, max_queue_size=QUEUE_SIZE) 
    net.addLink(h2, l1, bw=LINK_BW_MBPS, max_queue_size=QUEUE_SIZE) 
    net.addLink(h3, l2, bw=LINK_BW_MBPS, max_queue_size=QUEUE_SIZE)

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