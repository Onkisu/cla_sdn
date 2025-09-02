#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI

class ComplexTopo(Topo):
    def build(self):
        # Switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        # Hosts (dari 3 subnet berbeda)
        h1 = self.addHost('h1', ip='10.0.0.1/24')
        h2 = self.addHost('h2', ip='10.0.0.2/24')
        h3 = self.addHost('h3', ip='10.0.0.3/24')

        h4 = self.addHost('h4', ip='10.0.1.1/24')
        h5 = self.addHost('h5', ip='10.0.1.2/24')
        h6 = self.addHost('h6', ip='10.0.1.3/24')

        h7 = self.addHost('h7', ip='10.0.2.1/24')  # server/uplink

        # Links (optional bandwidth/latency bisa ditambahkan)
        self.addLink(h1, s1)
        self.addLink(h2, s1)
        self.addLink(h3, s1)

        self.addLink(h4, s2)
        self.addLink(h5, s2)
        self.addLink(h6, s2)

        self.addLink(h7, s3)

        # Backbone inter-switch
        self.addLink(s1, s3)
        self.addLink(s2, s3)




if __name__=="__main__":
    net = Mininet(topo=ComplexTopo(), switch=OVSSwitch, controller=lambda name: RemoteController(name, ip="127.0.0.1"), link=TCLink)
    net.start()
    CLI(net)
    net.stop()
