#!/usr/bin/env python3
"""
FIXED BURST SIMULATION (SINGLE PORT 9000)

- h1 -> h2 : steady VoIP (AMAN)
- h3 -> h2 : 2 parallel flat bursts
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
        info("WARNING: D-ITG not found\n")
        return False
    return True


def run():
    info("*** Starting Spine-Leaf Simulation\n")

    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True
    )

    net.addController('c0', ip='127.0.0.1', port=6653)

    # ---------------- TOPOLOGY ----------------
    spines = []
    leaves = []

    for i in range(1, 4):
        spines.append(
            net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13')
        )

    for i in range(1, 4):
        leaves.append(
            net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13')
        )

    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000, delay='1ms')

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

    if check_ditg():
        info("*** Starting ITGRecv on h2 (port 9000)\n")
        h2.cmd('ITGRecv -p 9000 -l /tmp/itg_recv.log &')

        time.sleep(1)
        dst_ip = h2.IP()

        # -------- STEADY TRAFFIC (h1) --------
        info("*** Steady traffic h1 -> h2\n")
        h1.cmd(
            f'ITGSend -T UDP -a {dst_ip} '
            f'-rp 9000 -c 160 -C 50 '
            f'-t 86400000 -l /dev/null &'
        )

        # -------- BURST LOOP 300 --------
        def burst_300():
            intervals = [2, 3, 4]
            rate = 300
            duration = 60

            while True:
                for m in intervals:
                    time.sleep(m * 60)
                    info("\n[BURST 300 START]\n")

                    p = h3.popen(
                        f'ITGSend -T UDP -a {dst_ip} '
                        f'-rp 9000 -c 160 -C {rate} '
                        f'-t {duration * 1000} -l /dev/null',
                        shell=True
                    )

                    time.sleep(duration)
                    p.terminate()
                    info("[BURST 300 END]\n")

        # -------- BURST LOOP 500 --------
        def burst_500():
            intervals = [1, 2, 2]
            rate = 500
            duration = 30

            while True:
                for m in intervals:
                    time.sleep(m * 60)
                    info("\n[BURST 500 START]\n")

                    p = h3.popen(
                        f'ITGSend -T UDP -a {dst_ip} '
                        f'-rp 9000 -c 160 -C {rate} '
                        f'-t {duration * 1000} -l /dev/null',
                        shell=True
                    )

                    time.sleep(duration)
                    p.terminate()
                    info("[BURST 500 END]\n")

        t1 = threading.Thread(target=burst_300, daemon=True)
        t2 = threading.Thread(target=burst_500, daemon=True)

        t1.start()
        t2.start()

    info("*** Running CLI\n")
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    run()
