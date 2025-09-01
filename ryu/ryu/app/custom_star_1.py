#!/usr/bin/python

"""
custom_topo.py

A simple Mininet script to create a custom star topology of switches
and hosts, and connect it to a remote Ryu controller.
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI

# Define the custom topology class. It inherits from Mininet's Topo class.
class StarSwitchTopo(Topo):
    """
    Custom star topology with switches and hosts.
    - A single central switch (c1)
    - Multiple peripheral switches (s1, s2, ...)
    - One host connected to each peripheral switch (h1, h2, ...)
    """
    def build(self, hosts=3):
        # Add a central switch to the topology with a unique dpid
        c1 = self.addSwitch('c1', dpid='0000000000000001')

        # Add peripheral switches and hosts, and connect them to the central switch
        for i in range(1, hosts + 1):
            s = self.addSwitch('s%d' % i, dpid='000000000000000%d' % (i + 1))
            h = self.addHost('h%d' % i)
            
            # Connect the host to its peripheral switch
            self.addLink(h, s)
            
            # Connect the peripheral switch to the central switch
            self.addLink(s, c1)

def simpleTest():
    # Buat topo dan tambahkan NAT sebelum start
    topo = StarSwitchTopo()
    net = Mininet(
        topo=topo,
        controller=RemoteController,
        switch=OVSSwitch,
        autoSetMacs=True
    )

    # Tambahkan NAT node & hubungkan ke central switch
    nat = net.addNAT(name='nat0', ip='10.0.0.254/24')  # kasih IP NAT
    c1 = net.get('c1')
    net.addLink(nat, c1)

    # Start network setelah semua node/link ditambahkan
    net.start()
    nat.configDefault()

    # Set default route + DNS hanya di namespace host Mininet
    for host in net.hosts:
        host.cmd('ip route add default via 10.0.0.254')
        host.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")

    print("--- Test koneksi internal ---")
    h1 = net.get('h1')
    h3 = net.get('h3')
    print(h1.cmd('ping -c 3 %s' % h3.IP()))

    print("--- Test koneksi internet dari h1 ---")
    print(h1.cmd('ping -c 3 8.8.8.8'))

    print("--- Starting CLI ---")
    CLI(net)
    net.stop()


if __name__ == '__main__':
    simpleTest()
