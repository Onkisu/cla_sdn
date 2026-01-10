#!/usr/bin/env python3
"""
SPINE-LEAF FINAL CHAOS
- Topology: 3 Spines, 3 Leaves, 3 Hosts (Asli sesuai permintaan)
- Modifikasi: Link Host dibatasi 10Mbps (Strict Queue) agar bisa congestion.
- Traffic: Pareto Chaos (Non-deterministic)
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
    # Setup Host
    h1 = net.get('h1') # User Biasa
    h2 = net.get('h2') # VICTIM
    h3 = net.get('h3') # Attacker
    dst_ip = h2.IP()
    
    info("*** Starting Receiver on h2 (Victim)...\n")
    h2.cmd('killall ITGRecv')
    h2.cmd('ITGRecv -l /dev/null &')
    time.sleep(2)
    
    # Background Traffic (VoIP like) dari h1
    info("*** Starting Background Traffic from h1...\n")
    h1.cmd(f'ITGSend -T UDP -a {dst_ip} -c 160 -C 50 -t 3600000 &')

    info("*** Starting CHAOS Traffic from h3...\n")
    
    # Variables for Random Walk
    current_pps = 500
    trend = 1.0 
    
    while True:
        # --- PARETO CHAOS LOGIC ---
        # 1. Trend Perubahan (Naik/Turun pelan)
        change = random.uniform(0.9, 1.15) 
        trend = trend * change
        
        if trend > 2.0: trend = 1.8
        if trend < 0.5: trend = 0.6
        
        target_pps = int(current_pps * trend)
        
        # 2. Burst / Flash Crowd (Tiba-tiba spike)
        # Probabilitas 15% kejadian Burst
        if random.random() < 0.15:
            spike_factor = random.uniform(1.5, 3.0)
            real_pps = int(target_pps * spike_factor)
            duration = random.uniform(1.0, 3.0) # Burst cepat
            mode = "BURST"
        else:
            real_pps = target_pps
            duration = random.uniform(2.0, 4.0)
            mode = "FLOW"
            
        # Hard Limits (Fisika Link 10Mbps ~ 900-1500 pps tergantung size)
        if real_pps > 12000: real_pps = 12000 
        if real_pps < 100: real_pps = 100

        # Ukuran Paket Bervariasi (Membuat Jitter)
        # 1400 bytes -> Cepat penuh bandwidthnya
        # 100 bytes -> Lambat penuh, tapi PPS tinggi
        packet_size = random.choice([200, 500, 1000, 1400])
        
        # Kalkulasi Mbps Estimasi
        mbps = (real_pps * packet_size * 8) / 1_000_000
        
        info(f"[GEN] {mode} | {real_pps} pps | Sz:{packet_size} | {mbps:.2f} Mbps\n")
        
        # Kirim D-ITG
        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -c {packet_size} -C {real_pps} -t {int(duration*1000)} -l /dev/null')
        
        current_pps = real_pps if mode == "FLOW" else target_pps
        time.sleep(0.1)

def run():
    # Gunakan TCLink untuk fitur bandwidth limit
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink)
    
    info("*** Adding Controller\n")
    net.addController('c0', ip='127.0.0.1', port=6653)

    info("*** Adding Switches (Spine-Leaf 3x3)\n")
    spines = []
    leaves = []
    
    # DPID Hex 1-3
    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13'))
    
    # DPID Hex 4-6
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13'))

    info("*** Creating Inter-Switch Links (Backbone 1Gbps)\n")
    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000, delay='1ms')

    info("*** Adding Hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    info("*** Creating Host Links (BOTTLENECK 10Mbps)\n")
    # Disini kita ubah spek kabelnya saja, topologi tetap sama.
    # Queue=20 -> Strict, cepat drop kalau penuh.
    net.addLink(h1, leaves[0], bw=10, max_queue_size=20, delay='1ms') 
    net.addLink(h2, leaves[1], bw=10, max_queue_size=20, delay='1ms') # Target (l2)
    net.addLink(h3, leaves[2], bw=10, max_queue_size=20, delay='1ms')

    info("*** Starting Network\n")
    net.start()
    net.pingAll()

    # Jalankan Generator
    t = threading.Thread(target=traffic_generator, args=(net,))
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()