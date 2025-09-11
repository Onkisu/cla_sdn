#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, Node
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info

class LinuxRouter(Node):
    def config(self, **params):
        super().config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

def run():
    net = Mininet(controller=RemoteController, link=TCLink)
    
    info("*** Adding controller\n")
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)

    info("*** Adding router\n")
    r1 = net.addHost('r1', cls=LinuxRouter, ip='10.0.0.254/24')

    info("*** Adding hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
    h2 = net.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')

    info("*** Adding switch\n")
    s1 = net.addSwitch('s1')

    info("*** Creating links\n")
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(r1, s1)

    # Attach VPS side of veth
    r1_intf = net.addLink(r1, None, intfName1='r1-eth1', params1={'ip':'192.168.200.2/24'})
    r1.cmd('ip link set r1-eth1 up')

    info("*** Starting network\n")
    net.start()

    info("*** Adding default route on R1\n")
    r1.cmd('ip route add default via 192.168.200.1')

    info("*** Running CLI\n")
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
