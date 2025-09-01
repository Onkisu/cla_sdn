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
    """
    This function sets up the Mininet network and connects it to a remote controller.
    """
    # Create the network using the custom topology and a remote controller.
    net = Mininet(
        topo=StarSwitchTopo(),
        controller=RemoteController,
        switch=OVSSwitch,
        autoSetMacs=True # Automatically set MAC addresses
    )

    # Start the network. This will launch all nodes.
    # nat = net.addNAT(name='nat0', connect=False)

    net.start()

    # c1 = net.get('c1')
    # net.addLink(nat, c1)
    # nat.configDefault()

    # for host in net.hosts:
    #     host.cmd('ip route add default via 10.0.0.254')
        # host.cmd('echo nameserver 8.8.8.8 >> /etc/resolv.conf')



    # Get references to the hosts and test connectivity.
    print("--- Pinging hosts to test connectivity ---")
    h1 = net.get('h1')
    h3 = net.get('h3')
    # Use h1 to ping h3 to test connectivity through the central switch
    h1.cmd('ping -c 3 %s' % h3.IP())

    # Start the Mininet CLI for interactive control.
    print("--- Starting CLI ---")
    CLI(net)

    # Clean up and stop the network when the CLI is exited.
    net.stop()

if __name__ == '__main__':
    simpleTest()
