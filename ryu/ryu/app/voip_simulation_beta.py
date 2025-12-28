#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI

def run():
    net = Mininet(
        controller=None,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True
    )

    c0 = net.addController(
        'c0',
        controller=RemoteController,
        ip='127.0.0.1',
        port=6633
    )

    # Spines
    spines = []
    for i in range(1, 4):
        spines.append(net.addSwitch(f'spine{i}'))

    # Leaves
    leaves = []
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'leaf{i}'))

    # Hosts
    h1 = net.addHost('h1', ip='131.202.240.101/24')
    h2 = net.addHost('h2', ip='131.202.240.65/24')

    # Leaf-host
    net.addLink(h1, leaves[0])
    net.addLink(h2, leaves[1])

    # Leaf-spine full mesh
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000)

    net.start()

    # Start tcpdump on h1
    h1.cmd('tcpdump -i h1-eth0 -w /tmp/voip.pcap &')

    # Start D-ITG
    h2.cmd('ITGRecv &')
    h1.cmd(
        'ITGSend -T UDP -a 131.202.240.65 '
        '-C 100 -c 80 -t 300000 &'
    )

    CLI(net)
    net.stop()

if __name__ == '__main__':
    run()
