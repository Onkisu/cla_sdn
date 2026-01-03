#!/usr/bin/env python3
"""
FIXED MOUNTAIN BURST (ISOLATED PORTS) - Version 2
- h1 -> h2 (Port 9000): AMAN, tidak akan putus.
- h3 -> h2 (Port 9001): GUNUNG, naik turun tiap detik tanpa ganggu h1.
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading 
import random 

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (Fixed Version v2)\n")
    
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    net.addController('c0', ip='127.0.0.1', port=6653)
    
    spines = []
    leaves = []
    
    # Topology Setup
    for i in range(1, 4):
        s = net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13')
        spines.append(s)
    for i in range(1, 4):
        l = net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13')
        leaves.append(l)
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    # Hosts
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    info("*** Starting Network & PingAll\n")
    net.start()
    time.sleep(3)
    net.pingAll()
    
    if check_ditg():
        info("*** Setup Traffic Receiver (ISOLATED PORTS)\n")
        
        # Kill any existing ITG processes
        h1.cmd('pkill -9 -f ITGSend')
        h2.cmd('pkill -9 -f ITGRecv')
        h3.cmd('pkill -9 -f ITGSend')
        time.sleep(1)
        
        # --- RECEIVERS (Isolated by Port) ---
        # 1. Receiver for h1 traffic - Port 9000 (Default)
        h2.cmd('ITGRecv -Sp 9000 -l /tmp/recv_h1_voip.log > /tmp/recv_h1.out 2>&1 &')
        
        # 2. Receiver for h3 traffic - Port 9001
        h2.cmd('ITGRecv -Sp 9001 -l /tmp/recv_h3_burst.log > /tmp/recv_h3.out 2>&1 &')
        
        time.sleep(2)
        
        # Verify receivers are running
        info("*** Verifying Receivers...\n")
        result = h2.cmd('pgrep -a ITGRecv')
        info(f"    Active receivers: {result}")
        
        # -----------------------------------------------------------
        # TRAFIK 1: h1 -> h2 (Steady VoIP on Port 9000)
        # -----------------------------------------------------------
        dst_ip = h2.IP()
        dst_port = 9000
        
        info(f"    [STEADY] Starting h1 -> h2:{dst_port} (Persistent VoIP)\n")
        # Use nohup to ensure it doesn't get killed accidentally
        h1.cmd(f'nohup ITGSend -T UDP -a {dst_ip} -rp {dst_port} -c 160 -C 50 -t 86400000 -l /tmp/h1_send.log > /tmp/h1_voip.out 2>&1 &')
        
        time.sleep(1)
        
        # Verify h1 sender is running
        result = h1.cmd('pgrep -a ITGSend')
        info(f"    h1 sender status: {result}")
        
        # -----------------------------------------------------------
        # TRAFIK 2: h3 -> h2 (Mountain Burst on Port 9001)
        # -----------------------------------------------------------
        def burst_loop():
            # Interval waktu antar serangan (menit)
            intervals_minutes = [2, 3, 4]
            
            # Settingan Gunung
            duration_sec = 30
            peak_rate = 1000
            base_rate = 500
            noise_factor = 0.3
            
            # Rumus Parabola
            midpoint = duration_sec / 2
            curvature = (peak_rate - base_rate) / (midpoint ** 2)

            dst_port_burst = 9001
            info(f"    [BURST] h3 armed on Port {dst_port_burst}. Pattern: Mountain.\n")
            
            while True:
                for mins in intervals_minutes:
                    # For testing, use small values like 10, 20, 30 seconds
                    # For production, use: mins * 60
                    seconds_to_wait = mins * 60  # Change to small values for testing
                    
                    info(f"\n[SCHEDULER] Waiting {mins} minutes ({seconds_to_wait}s)...\n")
                    time.sleep(seconds_to_wait) 
                    
                    info(f"\n*** [ATTACK START] h3 sending Mountain Burst to {dst_ip}:{dst_port_burst} ***\n")
                    
                    # Looping Detik-per-Detik untuk membentuk gunung
                    for t in range(duration_sec + 1):
                        # 1. Hitung Rate
                        dist = t - midpoint
                        clean = peak_rate - (curvature * (dist ** 2))
                        noise = random.uniform(-noise_factor, noise_factor)
                        final = int(clean * (1 + noise))
                        if final < base_rate: final = base_rate
                        
                        # 2. Visualisasi
                        bar = "#" * int(final / 10)
                        info(f" t={t:02d} | Rate={final:03d} | {bar}\n")
                        
                        # 3. Kill previous h3 ITGSend ONLY (not h1!)
                        # Use more specific pattern to avoid killing h1
                        h3.cmd('pkill -9 -f "ITGSend.*9001"')
                        time.sleep(0.1)  # Small delay to ensure clean kill
                        
                        # 4. Launch new ITGSend for 1 second burst
                        # -t 1000 = 1 second duration
                        h3.cmd(f'ITGSend -T UDP -a {dst_ip} -rp {dst_port_burst} -c 160 -C {final} -t 1000 -l /dev/null > /dev/null 2>&1 &')
                        
                        time.sleep(1)  # Wait 1 second before next iteration
                        
                    # Clean up h3 sender after burst
                    h3.cmd('pkill -9 -f "ITGSend.*9001"')
                    info("\n*** [ATTACK END] Cycle finished.\n")
                    
                    # Verify h1 is still running
                    result = h1.cmd('pgrep -a ITGSend')
                    if 'ITGSend' not in result:
                        info("!!! WARNING: h1 traffic stopped! Restarting...\n")
                        h1.cmd(f'nohup ITGSend -T UDP -a {dst_ip} -rp {dst_port} -c 160 -C 50 -t 86400000 -l /tmp/h1_send.log > /tmp/h1_voip.out 2>&1 &')

        t = threading.Thread(target=burst_loop)
        t.daemon = True 
        t.start()
            
    info("*** Running CLI\n")
    info("*** Tip: Check traffic with 'h1 ps aux | grep ITG' and 'h3 ps aux | grep ITG'\n")
    CLI(net)
    
    info("*** Stopping network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()