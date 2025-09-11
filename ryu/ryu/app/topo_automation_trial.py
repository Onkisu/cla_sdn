#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, Node
from mininet.nodelib import NAT
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def run():
    net = Mininet(controller=RemoteController, link=TCLink)

    info('*** Adding controller\n')
    c0 = net.addController('c0', ip='127.0.0.1', port=6633)

    info('*** Adding switch\n')
    s1 = net.addSwitch('s1')

    info('*** Adding NAT host\n')
    nat0 = net.addHost('nat0', cls=NAT, ip='10.0.0.254/24', inetIntf='eth0')

    info('*** Adding hosts\n')
    h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
    h2 = net.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')

    info('*** Creating links\n')
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(nat0, s1)

    info('*** Starting network\n')
    net.start()

    info('*** Configuring NAT\n')
    nat0.configDefault()  # otomatis setup NAT dan IP forwarding

    info('*** Network ready\n')
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
