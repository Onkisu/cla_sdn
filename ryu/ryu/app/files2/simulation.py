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
    try:
        cmd = f"pgrep -f 'mininet:{hostname}'"
        return subprocess.check_output(cmd, shell=True).decode().strip()
    except:
        return None

def run_itg_burst(src_name, dst_ip, pkt_size, rate_pps, duration_ms):
    pid = get_pid(src_name)
    if not pid: return
    
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
    info("\n*** 🧠 SMART GENERATOR STARTED (Waiting 5s...) ***\n")
    time.sleep(5)
    
    while True:
        # Probabilitas Serangan diperbesar
        mode = random.choices(
            ['IDLE', 'NORMAL', 'ATTACK_PATTERN'], 
            weights=[0.2, 0.4, 0.4] 
        )[0]
        
        info(f"\n[TRAFFIC] Mode: {mode}\n")

        if mode == 'IDLE':
            time.sleep(2)

        elif mode == 'NORMAL':
            # Normal Download (Paket Besar 1400B, Rate Rendah 200pps)
            # Bandwidth 1Mbps cukup untuk ini
            run_itg_burst("h1", DST_IP, 1400, 50, 4000)
            time.sleep(4)

        elif mode == 'ATTACK_PATTERN':
            # === FASE RAMP-UP (PREDIKSI) ===
            info("   >>> ⚠️ PRE-ATTACK (Ramp Up)...\n")
            # Naikkan perlahan agar ML sempat deteksi akselerasi
            run_itg_burst("h3", DST_IP, 64, 200, 1000) # 200 pps
            time.sleep(0.5)
            run_itg_burst("h3", DST_IP, 64, 800, 1000) # 800 pps
            time.sleep(0.5)
            run_itg_burst("h3", DST_IP, 64, 1500, 1000) # 1500 pps
            time.sleep(1.0)
            
            # === FASE CONGESTION (DROP) ===
            info("   >>> 💥 BOOM! Congestion Hit.\n")
            # Hajar dengan 20.000 PPS.
            # Link 1Mbps + Queue 50 pasti JEBOL (Drops terjadi)
            for _ in range(5):
                run_itg_burst("h1", DST_IP, 64, 20000, 500)
                run_itg_burst("h3", DST_IP, 64, 20000, 500)
                time.sleep(0.4)
            
            time.sleep(2)
        
        time.sleep(random.randint(1, 2))

def main():
    setLogLevel('info')

    net = Mininet(controller=RemoteController, link=TCLink, switch=OVSKernelSwitch)
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    s1 = net.addSwitch('s1', dpid='0000000000000001')
    l1 = net.addSwitch('l1', dpid='0000000000000002')
    l2 = net.addSwitch('l2', dpid='0000000000000003')

    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')
    h3 = net.addHost('h3', ip='10.0.0.3')

    # === BAGIAN PENTING YANG DIUBAH ===
    # Bandwidth diturunkan ke 1 Mbps (Sangat kecil)
    # Queue Size diturunkan ke 50 paket (Cepat penuh)
    link_opts = {'bw': 1, 'delay': '1ms', 'max_queue_size': 50}
    
    net.addLink(s1, l1, **link_opts)
    net.addLink(s1, l2, **link_opts)
    net.addLink(h1, l1, **link_opts)
    net.addLink(h2, l2, **link_opts)
    net.addLink(h3, l1, **link_opts)

    net.start()
    h2.cmd("ITGRecv > /dev/null 2>&1 &")
    
    t = threading.Thread(target=traffic_logic, daemon=True)
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    main()