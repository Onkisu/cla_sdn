#!/usr/bin/env python3
"""
UPDATED SIMULATION
- Link 10Mbps, Queue 50 packets (Strict Buffer)
- Packet Size 1000 Bytes (Supaya cepat penuh)
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import random
import threading

def traffic_generator(net):
    h2 = net.get('h2')
    h3 = net.get('h3')
    dst_ip = h2.IP()
    
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &')
    time.sleep(1)

    # PACKET SIZE BESAR = 1000 Bytes
    # Bandwidth 10 Mbps = 1250 packets/sec (jika 1000 bytes)
    # Jadi kita main di angka packet rate yang relevan:
    # Normal: 100-300 pps (0.8 - 2.4 Mbps)
    # Ramp Up: 800 pps (6.4 Mbps)
    # Congestion: 1500 pps (12 Mbps -> Pasti Drop)

    current_state = "NORMAL"
    
    while True:
        if current_state == "NORMAL":
            if random.random() < 0.3:
                current_state = "RAMP_UP"
                pps = random.randint(700, 900) 
                duration = random.randint(5, 8)
            else:
                current_state = "NORMAL"
                pps = random.randint(100, 300) 
                duration = random.randint(10, 15)

        elif current_state == "RAMP_UP":
            if random.random() < 0.8: # High chance to fail
                current_state = "CONGESTION"
                pps = random.randint(1300, 1800) # > 10Mbps
                duration = random.randint(4, 7)
            else:
                current_state = "NORMAL"
                pps = random.randint(200, 400)
                duration = random.randint(5, 10)

        elif current_state == "CONGESTION":
            current_state = "NORMAL"
            pps = random.randint(50, 200)
            duration = random.randint(10, 20)

        info(f"[GEN] {current_state} | {pps} pps (sz 1000) | {duration}s\n")
        
        # -c 1000 : Constant packet size 1000 bytes
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c 1000 -C {pps} -t {duration*1000} -l /dev/null')
        time.sleep(0.5)

def run():
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    net.addController('c0', ip='127.0.0.1', port=6653)

    s1 = net.addSwitch('s1', dpid='1')
    l1 = net.addSwitch('l1', dpid='2') # Victim Switch
    l2 = net.addSwitch('l2', dpid='3')

    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    # Backbone
    net.addLink(s1, l1, bw=100)
    net.addLink(s1, l2, bw=100)
    
    # --- BOTTLENECK LINK ---
    # BW=10 Mbps, Queue=50 packets
    # Dengan paket 1000 bytes, 50 paket = 50KB buffer. Cepat penuh jika overload.
    net.addLink(h1, l1, bw=10, max_queue_size=50, delay='1ms') 
    net.addLink(h2, l1, bw=10, max_queue_size=50, delay='1ms') 
    net.addLink(h3, l2, bw=10, max_queue_size=50, delay='1ms')

    net.start()
    
    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()
    
    # Jalankan background traffic kecil dari h1
    h1.cmd('ITGSend -T UDP -a 10.0.0.2 -c 500 -C 50 -t 3600000 &')

    # Simpan network tetap jalan
    while True:
        time.sleep(10)

if __name__ == '__main__':
    setLogLevel('info')
    run()