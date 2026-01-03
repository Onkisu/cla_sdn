#!/usr/bin/env python3
"""
STEADY + DUAL FLAT BURST (SINGLE PORT 9000)

- h1 -> h2 : Steady VoIP (50 pkt/s, long running)
- h3 -> h2 : Burst A (300 pkt/s, 60s, interval [2,3,4])
- h3 -> h2 : Burst B (500 pkt/s, 30s, interval [1,2,2])

AMAN:
- 1 ITGRecv
- Tidak ada spawn per detik
- Tidak ada pkill global
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading

# --------------------------------------------------
# UTIL
# --------------------------------------------------
def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def start_itgsend(host, cmd):
    return host.popen(cmd, shell=True)

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def run():
    info("*** Starting Spine-Leaf VoIP Simulation (Dual Burst)\n")

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

    dst_ip = h2.IP()

    # ---------------- RECEIVER ----------------
    info("*** Starting ITGRecv (port 9000)\n")
    h2.cmd('ITGRecv -l /tmp/recv_all.log &')
    time.sleep(1)

    # ---------------- STEADY ----------------
    info("*** Starting Steady Traffic: h1 -> h2\n")
    h1.cmd(
        f'ITGSend -T UDP -a {dst_ip} '
        f'-c 160 -C 50 '
        f'-t 86400000 '
        f'-l /dev/null &'
    )

    # ---------------- BURST A (300 pkt/s) ----------------
    def burst_loop_300():
        intervals = [2, 3, 4]
        rate = 300
        duration = 60

        info("*** Burst Loop A (300 pkt/s) READY\n")

        while True:
            for mins in intervals:
                info(f"[A] Waiting {mins} minutes...\n")
                time.sleep(mins * 60)

                info(f"[A] START burst 300 pkt/s for {duration}s\n")
                p = start_itgsend(
                    h3,
                    f'ITGSend -T UDP -a {dst_ip} '
                    f'-c 160 -C {rate} '
                    f'-t {duration * 1000} '
                    f'-l /dev/null'
                )

                time.sleep(duration)
                p.terminate()
                info("[A] END burst\n")

    # ---------------- BURST B (500 pkt/s) ----------------
    def burst_loop_500():
        intervals = [1, 2, 2]
        rate = 500
        duration = 30

        info("*** Burst Loop B (500 pkt/s) READY\n")

        while True:
            for mins in intervals:
                info(f"[B] Waiting {mins} minutes...\n")
                time.sleep(mins * 60)

                info(f"[B] START burst 500 pkt/s for {duration}s\n")
                p = start_itgsend(
                    h3,
                    f'ITGSend -T UDP -a {dst_ip} '
                    f'-c 160 -C {rate} '
                    f'-t {duration * 1000} '
                    f'-l /dev/null'
                )

                time.sleep(duration)
                p.terminate()
                info("[B] END burst\n")

    # ---------------- THREADS ----------------
    t1 = threading.Thread(target=burst_loop_300, daemon=True)
    t2 = threading.Thread(target=burst_loop_500, daemon=True)

    t1.start()
    t2.start()

    CLI(net)
    net.stop()

# --------------------------------------------------
if __name__ == '__main__':
    setLogLevel('info')
    run()
