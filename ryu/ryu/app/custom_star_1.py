#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.nodelib import NAT

class StarSwitchTopo(Topo):
    def build(self, hosts=3):
        c1 = self.addSwitch('c1', dpid='0000000000000001')

        for i in range(1, hosts + 1):
            s = self.addSwitch('s%d' % i, dpid='000000000000000%d' % (i + 1))
            h = self.addHost('h%d' % i)
            self.addLink(h, s)
            self.addLink(s, c1)

def simpleTest():
    topo = StarSwitchTopo()
    net = Mininet(
        topo=topo,
        controller=RemoteController,
        switch=OVSSwitch,
        autoSetMacs=True
    )

    # Tambahkan NAT
    nat = net.addHost('nat0', cls=NAT, ip='10.0.0.254/24', inNamespace=False)
    c1 = net.get('c1')
    net.addLink(nat, c1)

    net.start()
    nat.configDefault()

    # Set route + DNS
    for host in net.hosts:
        host.cmd('ip route add default via 10.0.0.254')
        host.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")

    print("--- Test koneksi internal ---")
    h1, h3 = net.get('h1', 'h3')
    print(h1.cmd('ping -c 3 %s' % h3.IP()))

    print("--- Test koneksi internet dari h1 ---")
    print(h1.cmd('ping -c 3 8.8.8.8'))

    CLI(net)
    net.stop()

if __name__ == '__main__':
    simpleTest()
