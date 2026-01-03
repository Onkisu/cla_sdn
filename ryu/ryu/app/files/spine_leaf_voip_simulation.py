#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading

STEADY_DURATION_MS = 60000   # 60 detik per sesi (aman)
STEADY_RATE = 50             # pps
PKT_SIZE = 160               # bytes
RESTART_DELAY = 1            # detik

def check_ditg():
    return subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0

def keep_steady_traffic(host, dst_ip):
    """
    Watchdog loop:
    - Jalankan ITGSend durasi terbatas
    - Jika selesai / mati → start ulang
    """
    while True:
        info("*** [WATCHDOG] (Re)starting STEADY VoIP h1 -> h2\n")
        host.cmd(
            f'ITGSend -T UDP -a {dst_ip} '
            f'-c {PKT_SIZE} -C {STEADY_RATE} '
            f'-t {STEADY_DURATION_MS} -l /dev/null'
        )
        time.sleep(RESTART_DELAY)

def run():
    info("*** Starting Spine-Leaf Topology (STEADY ONLY + WATCHDOG)\n")

    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True
    )

    net.addController('c0', ip='127.0.0.1', port=6653)

    spines, leaves = [], []

    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13'))
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13'))

    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000, delay='1ms')

    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')

    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    net.start()
    time.sleep(3)
    net.pingAll()

    if check_ditg():
        info("*** Starting ITGRecv on h2 (port 9000)\n")
        h2.cmd('ITGRecv -l /tmp/recv_steady.log &')
        time.sleep(1)

        info("*** Starting STEADY VoIP Watchdog (h1 -> h2)\n")
        t = threading.Thread(
            target=keep_steady_traffic,
            args=(h1, h2.IP())
        )
        t.daemon = True
        t.start()
    else:
        info("!!! D-ITG not installed, traffic disabled\n")

    info("*** Running Mininet CLI\n")
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
