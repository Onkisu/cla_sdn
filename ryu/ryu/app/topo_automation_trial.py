from mininet.net import Mininet
from mininet.node import RemoteController, Node
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel, info

def run():
    net = Mininet(controller=RemoteController, link=TCLink)

    info('*** Adding controller\n')
    net.addController('c0', ip='127.0.0.1', port=6653)

    info('*** Adding router\n')
    r1 = net.addHost('r1', ip='10.0.0.254/24')

    info('*** Adding hosts\n')
    h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
    h2 = net.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')

    info('*** Adding switch\n')
    s1 = net.addSwitch('s1')

    info('*** Creating links\n')
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(r1, s1)

    info('*** Enable NAT for router\n')
    r1.cmd('echo 1 > /proc/sys/net/ipv4/ip_forward')
    r1.cmd('iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE')

    net.start()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
