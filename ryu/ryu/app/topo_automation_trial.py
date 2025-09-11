from mininet.net import Mininet
from mininet.node import RemoteController, Node
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel
import os

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        os.system("sysctl -w net.ipv4.ip_forward=1")

def run():
    net = Mininet(controller=RemoteController, link=TCLink)

    c0 = net.addController('c0', ip='127.0.0.1', port=6653)

    # Hosts
    hosts = [net.addHost(f'h{i}', ip=f'10.0.{i}.1/24') for i in range(1,4)]

    # Switches
    s1 = net.addSwitch('s1')

    # Router
    r1 = net.addHost('r1', cls=LinuxRouter, ip='10.0.0.254/24')

    # Links
    for h in hosts:
        net.addLink(h, s1)
    net.addLink(r1, s1)

    net.start()

    # Set default routes
    for i,h in enumerate(hosts, start=1):
        h.cmd(f'ip route add default via 10.0.{i}.254')  # router IP sesuai subnet

    # Buat veth pair otomatis VPS <-> r1
    os.system("ip link add veth-vps type veth peer name veth-r1")
    os.system("ip link set veth-vps up")
    os.system("ip addr add 192.168.200.1/24 dev veth-vps")  # VPS side

    # Masukkan sisi r1 ke Mininet
    r1.cmd("ip link set veth-r1 up")
    r1.cmd("ip addr add 192.168.200.2/24 dev veth-r1")

    # NAT supaya paket Mininet bisa keluar
    os.system("iptables -t nat -A POSTROUTING -s 192.168.200.0/24 -o eth0 -j MASQUERADE")

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
