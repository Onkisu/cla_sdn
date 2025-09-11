from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController, Node
from mininet.nodelib import NAT
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel, info

class LinuxRouter(Node):
    def config(self, **params):
        super().config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super().terminate()

class SimpleTopo(Topo):
    def build(self):
        r1 = self.addNode('r1', cls=LinuxRouter)

        # NAT
        nat = self.addNode('nat0', cls=NAT, ip='192.168.100.254/24', inNamespace=False)
        s_nat = self.addSwitch('s99')
        self.addLink(nat, s_nat)
        self.addLink(r1, s_nat, intfName1='r1-eth0', params1={'ip':'192.168.100.1/24'})

        # Internal switches & hosts
        s1 = self.addSwitch('s1')
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        self.addLink(h1, s1)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.0.254/24'})

if __name__=="__main__":
    setLogLevel("info")
    net = Mininet(topo=SimpleTopo(), switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip='127.0.0.1'),
                  link=TCLink)
    net.start()

    # Setup NAT
    nat = net.get('nat0')
    r1 = net.get('r1')
    nat.configDefault()
    r1.cmd("ip route add default via 192.168.100.254")

    # DNS hosts
    h1 = net.get('h1')
    h1.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")

    CLI(net)
    net.stop()
