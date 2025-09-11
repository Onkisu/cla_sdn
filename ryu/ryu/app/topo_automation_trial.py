from mininet.net import Mininet
from mininet.node import Node, OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.link import TCLink

class LinuxRouter(Node):
    def config(self, **params):
        super().config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super().terminate()

net = Mininet(controller=lambda name: RemoteController(name, ip='127.0.0.1'),
              switch=OVSSwitch, link=TCLink)

r1 = net.addHost('r1', cls=LinuxRouter)
s1 = net.addSwitch('s1')
h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')

net.addLink(h1, s1)
net.addLink(r1, s1)

# attach veth r1-to-vps ke r1
r1.cmd("ip link set r1-to-vps name r1-eth0 up")
r1.cmd("ip addr add 192.168.200.2/24 dev r1-eth0")
r1.cmd("ip route add default via 192.168.200.1 dev r1-eth0")

net.start()

CLI(net)
net.stop()
