#!/usr/bin/python3
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.link import TCLink

class ComplexTopo(Topo):
    def build(self):
        # Switch
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        # Hosts
        h1 = self.addHost('h1', ip='10.0.0.1/24')
        h2 = self.addHost('h2', ip='10.0.0.2/24')
        h3 = self.addHost('h3', ip='10.0.0.3/24')
        h4 = self.addHost('h4', ip='10.0.1.1/24')
        h5 = self.addHost('h5', ip='10.0.1.2/24')
        h6 = self.addHost('h6', ip='10.0.1.3/24')
        h7 = self.addHost('h7', ip='10.0.2.1/24')  # server/uplink
        # Links with bandwidth limit optional
        self.addLink(h1,s1)
        self.addLink(h2,s1)
        self.addLink(h3,s1)
        self.addLink(h4,s2)
        self.addLink(h5,s2)
        self.addLink(h6,s2)
        self.addLink(h7,s3)
        self.addLink(s1,s3)
        self.addLink(s2,s3)

if __name__=="__main__":
    net = Mininet(topo=ComplexTopo(), controller=RemoteController, link=TCLink)
    net.start()
    CLI(net)
    net.stop()
