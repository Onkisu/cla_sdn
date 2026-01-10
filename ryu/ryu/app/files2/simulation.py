#!/usr/bin/env python3
# file: sim_smart.py

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.log import setLogLevel, info
from mininet.cli import CLI
import time
import subprocess
import random
import threading

# Target Korban (H2)
DST_IP = "10.0.0.2"

def get_pid(hostname):
    # Helper untuk mencari PID host mininet
    try:
        cmd = f"pgrep -f 'mininet:{hostname}'"
        return subprocess.check_output(cmd, shell=True).decode().strip()
    except:
        return None

def run_itg_burst(src_name, dst_ip, pkt_size, rate_pps, duration_ms):
    """Mengirim paket UDP menggunakan D-ITG"""
    pid = get_pid(src_name)
    if not pid: return
    
    # Perintah ITGSend (Background Process)
    cmd = [
        "mnexec", "-a", pid,
        "ITGSend", "-T", "UDP", "-a", dst_ip,
        "-c", str(pkt_size),
        "-C", str(rate_pps),
        "-t", str(duration_ms),
        "-l", "/dev/null"
    ]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def traffic_logic():
    """LOGIKA GENERATOR TRAFFIC UNTUK ML"""
    info("\n*** 🧠 SMART GENERATOR STARTED (Tunggu 5 detik...) ***\n")
    time.sleep(5)
    
    while True:
        # Pilih Skenario Secara Acak
        # IDLE: Diam
        # NORMAL: Youtube/Download (Paket besar, Rate sedang)
        # ATTACK: DDoS (Paket kecil, Rate TINGGI)
        mode = random.choices(
            ['IDLE', 'NORMAL', 'ATTACK_PATTERN'], 
            weights=[0.3, 0.4, 0.3]
        )[0]
        
        info(f"\n[TRAFFIC] Mode: {mode}\n")

        if mode == 'IDLE':
            time.sleep(3)

        elif mode == 'NORMAL':
            # Simulasi file download (Aman)
            run_itg_burst("h1", DST_IP, 1400, 300, 5000)
            time.sleep(5)

        elif mode == 'ATTACK_PATTERN':
            # INI KUNCINYA AGAR BISA DIPREDIKSI
            # Kita buat pola RAMP-UP (Pemanasan)
            
            # 1. Warning Phase (Trafik mulai naik, ML harus curiga disini)
            info("   >>> ⚠️ PRE-ATTACK (Ramp Up)... ML Should Detect This!\n")
            run_itg_burst("h3", DST_IP, 64, 500, 2000) # 500 pps
            time.sleep(1)
            run_itg_burst("h3", DST_IP, 64, 1500, 2000) # 1500 pps (Akselerasi Tinggi)
            time.sleep(1)
            
            # 2. Impact Phase (Congestion Terjadi Disini)
            info("   >>> 💥 BOOM! Congestion Hit.\n")
            # Rate 10.000pps dengan paket kecil pasti membunuh link 10Mbps
            for _ in range(4):
                run_itg_burst("h1", DST_IP, 64, 10000, 1000)
                run_itg_burst("h3", DST_IP, 64, 10000, 1000)
                time.sleep(0.5)
            
            time.sleep(2)
        
        # Jeda random antar event
        time.sleep(random.randint(1, 3))

def main():
    setLogLevel('info')

    # 1. Setup Topologi (Spine-Leaf Sederhana)
    net = Mininet(controller=RemoteController, link=TCLink, switch=OVSKernelSwitch)
    
    # Controller arahkan ke localhost
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    # Switches
    s1 = net.addSwitch('s1', dpid='0000000000000001') # Spine
    l1 = net.addSwitch('l1', dpid='0000000000000002') # Leaf 1
    l2 = net.addSwitch('l2', dpid='0000000000000003') # Leaf 2

    # Hosts
    h1 = net.addHost('h1', ip='10.0.0.1') # Attacker 1
    h2 = net.addHost('h2', ip='10.0.0.2') # Victim
    h3 = net.addHost('h3', ip='10.0.0.3') # Attacker 2

    # Links
    # Batasi Bandwidth 10Mbps agar mudah macet (Congestion)
    # Batasi Queue 100 paket agar packet loss cepat terjadi
    link_opts = {'bw': 10, 'delay': '1ms', 'max_queue_size': 100}
    
    net.addLink(s1, l1, **link_opts)
    net.addLink(s1, l2, **link_opts)
    net.addLink(h1, l1, **link_opts) # h1 -> l1
    net.addLink(h2, l2, **link_opts) # h2 -> l2
    net.addLink(h3, l1, **link_opts) # h3 -> l1

    net.start()
    
    # 2. Jalankan Receiver di H2 (Korban)
    h2.cmd("ITGRecv > /dev/null 2>&1 &")
    
    # 3. Jalankan Traffic Generator (Thread)
    t = threading.Thread(target=traffic_logic, daemon=True)
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    main()