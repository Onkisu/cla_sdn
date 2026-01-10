#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.log import setLogLevel, info
from mininet.cli import CLI
import time
import subprocess
import random
import threading
import signal
import sys

# === CONFIG TARGET ===
DST_IP = "10.0.0.2" # H2 sebagai Victim

def get_pid(hostname):
    try:
        cmd = f"pgrep -f 'mininet:{hostname}'"
        return subprocess.check_output(cmd, shell=True).decode().strip()
    except:
        return None

def run_itg_burst(src_name, dst_ip, pkt_size, rate_pps, duration_ms):
    """Mengirim traffic burst pendek"""
    pid = get_pid(src_name)
    if not pid: return
    
    # ITGSend command
    cmd = [
        "mnexec", "-a", pid,
        "ITGSend", "-T", "UDP", "-a", dst_ip,
        "-c", str(pkt_size),
        "-C", str(rate_pps),
        "-t", str(duration_ms),
        "-l", "/dev/null"
    ]
    # Run in background non-blocking for Python, but firing packets
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def traffic_pattern_generator():
    """
    Generator Cerdas: Membuat pola yang bisa dipelajari ML.
    Ada fase 'Idle', 'Normal', dan 'Pre-Congestion' (Ramp Up).
    """
    info("\n*** 🧠 Smart Traffic Generator Started (Background) ***\n")
    time.sleep(5) 
    
    while True:
        # Pilih Skenario secara Random Weighted
        scenario = random.choices(
            ['IDLE', 'NORMAL_VOIP', 'NORMAL_DOWNLOAD', 'ATTACK_BUILDUP'],
            weights=[0.3, 0.3, 0.2, 0.2] # 20% kemungkinan serangan/congestion
        )[0]
        
        info(f"\n[TRAFFIC-GEN] Scenario: {scenario}\n")
        
        if scenario == 'IDLE':
            time.sleep(random.randint(2, 5))
            
        elif scenario == 'NORMAL_VOIP':
            # Paket Kecil, Rate Stabil
            run_itg_burst("h1", DST_IP, 160, 200, 5000)
            time.sleep(5)
            
        elif scenario == 'NORMAL_DOWNLOAD':
            # Paket Besar, Rate Lumayan (Tapi Buffer Kuat)
            run_itg_burst("h3", DST_IP, 1400, 500, 8000)
            time.sleep(8)
            
        elif scenario == 'ATTACK_BUILDUP':
            # INI KUNCI PREDIKSI: Pola Ramp-Up
            # Congestion tidak terjadi tiba-tiba, tapi naik bertahap.
            
            # Phase 1: Warning (PPS Naik dikit)
            info("   >>> ⚠️ Phase 1: Traffic Rising...\n")
            run_itg_burst("h1", DST_IP, 64, 800, 2000) 
            run_itg_burst("h3", DST_IP, 64, 800, 2000)
            time.sleep(2)
            
            # Phase 2: Critical (Hampir Penuh)
            info("   >>> ⚠️ Phase 2: High Load (ML Should Alert Here)...\n")
            run_itg_burst("h1", DST_IP, 64, 2000, 2000)
            run_itg_burst("h3", DST_IP, 64, 2000, 2000)
            time.sleep(2)
            
            # Phase 3: Congestion (Drop Happens)
            info("   >>> 💥 Phase 3: CONGESTION HIT!\n")
            # Hajar habis-habisan melebihi BW link (100Mbps)
            # Paket 64 bytes * 100,000 pps = overload CPU & Buffer
            for _ in range(5):
                run_itg_burst("h1", DST_IP, 64, 15000, 1000)
                run_itg_burst("h3", DST_IP, 64, 15000, 1000)
                time.sleep(0.5)
                
            time.sleep(2)

def main():
    setLogLevel('info')

    # === TOPOLOGY START (TIDAK DIUBAH) ===
    net = Mininet(controller=RemoteController, link=TCLink, switch=OVSKernelSwitch)
    
    # Controller IP
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    spines = []
    leaves = []

    # Create Switches
    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13'))
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13'))

    # Create Links Inter-Switch
    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000, delay='1ms')

    # Create Hosts
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')

    # Link Host ke Leaf (BW 100Mbps -> Mudah dibuat macet)
    net.addLink(h1, leaves[0], bw=100, delay='1ms', max_queue_size=100)
    net.addLink(h2, leaves[1], bw=100, delay='1ms', max_queue_size=100)
    net.addLink(h3, leaves[2], bw=100, delay='1ms', max_queue_size=100)

    net.start()
    # === TOPOLOGY END ===

    # Setup Receiver di H2
    info("*** Starting ITGRecv on h2...\n")
    h2.cmd("ITGRecv > /dev/null 2>&1 &")

    # Jalankan Traffic Generator di Thread Terpisah
    t_gen = threading.Thread(target=traffic_pattern_generator, daemon=True)
    t_gen.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    main()