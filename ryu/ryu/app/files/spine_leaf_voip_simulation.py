#!/usr/bin/env python3
"""
FLAT BURST (SINGLE PORT)
- h1 -> h2 : Steady VoIP (AMAN)
- h3 -> h2 : Flat Burst 300 pkt/s selama 60 detik
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (Flat Burst Version)\n")
    
    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True
    )
    net.addController('c0', ip='127.0.0.1', port=6653)

    # ---------------- TOPOLOGY ----------------
    spines, leaves = [], []

    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13'))
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13'))

    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')

    # ---------------- HOSTS ----------------
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')

    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    # ---------------- START ----------------
    net.start()
    time.sleep(3)
    net.pingAll()

    if not check_ditg():
        CLI(net)
        net.stop()
        return

    info("*** Starting ITG Receiver (Port 9000)\n")
    h2.cmd('ITGRecv -l /tmp/recv_all.log &')
    time.sleep(1)

    dst_ip = h2.IP()

    # ---------------- STEADY TRAFFIC ----------------
    info("*** Starting Steady VoIP: h1 -> h2\n")
    h1.cmd(
        f'ITGSend -T UDP -a {dst_ip} '
        f'-c 160 -C 50 '
        f'-t 86400000 '
        f'-l /dev/null &'
    )

    # ---------------- FLAT BURST ----------------
    def burst_loop():
        intervals_minutes = [1, 3, 4]   # jeda antar burst
        burst_rate = 300                # packet rate
        burst_duration = 25             # detik

        info("*** Burst Scheduler Ready (FLAT)\n")

        while True:
            for mins in intervals_minutes:
                info(f"\n[SCHEDULER] Waiting {mins} minutes...\n")
                time.sleep(mins * 60)

                info(f"\n*** [BURST START] h3 -> h2 | Rate={burst_rate} | Duration={burst_duration}s ***\n")

                # Pastikan tidak ada ITGSend lama
                h3.cmd('pkill -f ITGSend')

                h3.cmd(
                    f'ITGSend -T UDP -a {dst_ip} '
                    f'-c 160 -C {burst_rate} '
                    f'-t {burst_duration * 1000} '
                    f'-l /dev/null &'
                )

                time.sleep(burst_duration)
                h3.cmd('pkill -f ITGSend')

                info("*** [BURST END]\n")

    t = threading.Thread(target=burst_loop)
    t.daemon = True
    t.start()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
