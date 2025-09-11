from mininet.net import Mininet
from mininet.node import Node, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

class LinuxRouter(Node):
    def config(self, **params):
        super().config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super().terminate()

class InternetTopo(Topo):
    def build(self):
        # Router internal
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # Switch internal
        s1 = self.addSwitch('s1')

        # Hosts internal
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')

        # Links
        self.addLink(h1, s1)
        self.addLink(h2, s1)
        self.addLink(r1, s1, intfName1='r1-eth0', params1={'ip':'10.0.0.254/24'})

def startInternet(r1):
    # Bind router ke interface VPS (ens3) untuk akses internet
    r1_int = "r1-eth1"
    ens3 = "ens3"
    
    # Tambah interface router ke ens3 (veth pair)
    r1.cmd(f"ip link add {r1_int} type veth peer name {r1_int}-host")
    r1.cmd(f"ip link set {r1_int} up")
    r1.cmd(f"ip addr add 10.171.241.200/24 dev {r1_int}")

    # Enable NAT masquerade dari internal ke ens3
    r1.cmd("iptables -t nat -F")
    r1.cmd("iptables -t nat -A POSTROUTING -o ens3 -j MASQUERADE")
    r1.cmd("iptables -A FORWARD -i r1-eth0 -o ens3 -j ACCEPT")
    r1.cmd("iptables -A FORWARD -i ens3 -o r1-eth0 -m state --state RELATED,ESTABLISHED -j ACCEPT")

if __name__ == "__main__":
    setLogLevel('info')
    net = Mininet(topo=InternetTopo(), switch=OVSSwitch, link=TCLink)
    net.start()

    # Setup router NAT
    r1 = net.get('r1')
    startInternet(r1)

    # Set DNS untuk host
    for h in ['h1','h2']:
        host = net.get(h)
        host.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")

    CLI(net)
    net.stop()
