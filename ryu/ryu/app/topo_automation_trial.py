#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import Node, RemoteController
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import subprocess

class LinuxRouter(Node):
    "A Node with IP forwarding enabled."
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        # enable forwarding
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl -w net.ipv4.ip_forward=0')
        super(LinuxRouter, self).terminate()

def run():
    net = Mininet(controller=RemoteController, link=TCLink)

    info('*** Adding controller\n')
    c0 = net.addController('c0')

    info('*** Adding router\n')
    r1 = net.addHost('r1', cls=LinuxRouter, ip='10.0.0.254/24')

    info('*** Adding hosts\n')
    h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
    h2 = net.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')

    info('*** Adding switch\n')
    s1 = net.addSwitch('s1')

    info('*** Creating links\n')
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(r1, s1)

    info('*** Setup veth pair VPS <-> r1\n')
    VPS_IF = 'eth0'          # ganti sesuai interface VPS internet
    R1_VETH = 'r1-to-vps'
    VPS_VETH = 'vps-to-r1'
    VPS_IP = '192.168.200.1/24'
    R1_IP = '192.168.200.2/24'

    # create veth pair if not exist
    existing = subprocess.getoutput('ip link show {}'.format(R1_VETH))
    if 'does not exist' in existing:
        subprocess.call(['sudo', 'ip', 'link', 'add', R1_VETH, 'type', 'veth', 'peer', 'name', VPS_VETH])

    # bring up VPS side
    subprocess.call(['sudo', 'ip', 'link', 'set', VPS_VETH, 'up'])
    subprocess.call(['sudo', 'ip', 'addr', 'add', VPS_IP, 'dev', VPS_VETH])

    # NAT & forwarding
    subprocess.call(['sudo', 'sysctl', '-w', 'net.ipv4.ip_forward=1'])
    subprocess.call(['sudo', 'iptables', '-t', 'nat', '-A', 'POSTROUTING', '-s', '192.168.200.0/24', '-o', VPS_IF, '-j', 'MASQUERADE'])

    # bring up router side
    r1.cmd('ip link set {} up'.format(R1_VETH))
    r1.cmd('ip addr add {} dev {}'.format(R1_IP, R1_VETH))
    r1.cmd('ip route add default via 192.168.200.1 dev {}'.format(R1_VETH))

    info('*** Starting network\n')
    net.start()

    info('*** Testing connectivity\n')
    net.pingAll()

    info('*** Running CLI\n')
    CLI(net)

    info('*** Stopping network\n')
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
