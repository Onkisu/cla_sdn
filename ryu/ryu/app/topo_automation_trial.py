#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, Node
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel, info
import subprocess

class LinuxRouter(Node):
    """Router Linux dengan IP forwarding."""
    def config(self, **params):
        super().config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

def run():
    net = Mininet(controller=RemoteController, link=TCLink)

    info('*** Adding controller\n')
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    info('*** Adding router\n')
    r1 = net.addHost('r1', cls=LinuxRouter, ip='10.0.0.1/24')

    info('*** Adding switch\n')
    s1 = net.addSwitch('s1')

    info('*** Adding hosts\n')
    h1 = net.addHost('h1', ip='10.0.0.2/24', defaultRoute='via 10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.3/24', defaultRoute='via 10.0.0.1')

    info('*** Creating links\n')
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(r1, s1)
    
    info('*** Starting network\n')
    net.start()

    # Setup NAT di router untuk koneksi internet
    info('*** Configuring NAT\n')
    r1.cmd('iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE')
    r1.cmd('iptables -A FORWARD -i eth0 -o r1-eth0 -m state --state RELATED,ESTABLISHED -j ACCEPT')
    r1.cmd('iptables -A FORWARD -i r1-eth0 -o eth0 -j ACCEPT')

    info('*** Network ready\n')
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
