#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess

def check_ditg():
    return subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0

def run():
    info("*** Starting Spine-Leaf Topology (STEADY ONLY)\n")

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

        info("*** h1 -> h2 STEADY VoIP\n")
        h1.cmd(f'ITGSend -T UDP -a {h2.IP()} -c 160 -C 50 -t 86400000 -l /dev/null &')

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
  