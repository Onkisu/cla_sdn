#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import subprocess, time

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")  # enable forwarding

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class InternetTopo(Topo):
    def build(self):
        # Router
        r1 = self.addNode('r1', cls=LinuxRouter)

        # Switch internal
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        # Hosts internal
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')
        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')
        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Link hosts ke switch
        self.addLink(h1, s1); self.addLink(h2, s1); self.addLink(h3, s1)
        self.addLink(h4, s2); self.addLink(h5, s2); self.addLink(h6, s2)
        self.addLink(h7, s3)

        # Link router ke internal switches
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip':'10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip':'10.0.2.254/24'})

        # Link router ke internet (host OS)
        # r1-eth0 akan mengambil IP otomatis dari host via DHCP / NAT host
        self.addLink(r1, self.addSwitch('s99'), intfName1='r1-eth0')

def setup_internet_nat(router):
    """
    Atur NAT internet via iptables di r1
    """
    # ambil nama interface yang ke host
    intf = "r1-eth0"
    router.cmd("ip addr flush dev {}".format(intf))
    router.cmd("dhclient {}".format(intf))  # ambil IP dari host OS
    router.cmd("sysctl -w net.ipv4.ip_forward=1")

    # MASQUERADE semua subnet internal ke interface ini
    router.cmd("iptables -t nat -A POSTROUTING -s 10.0.0.0/16 -o {} -j MASQUERADE".format(intf))
    router.cmd("iptables -A FORWARD -i {} -o r1-eth1 -m state --state ESTABLISHED,RELATED -j ACCEPT".format(intf))
    router.cmd("iptables -A FORWARD -o {} -i r1-eth1 -j ACCEPT".format(intf))
    router.cmd("iptables -A FORWARD -i {} -o r1-eth2 -m state --state ESTABLISHED,RELATED -j ACCEPT".format(intf))
    router.cmd("iptables -A FORWARD -o {} -i r1-eth2 -j ACCEPT".format(intf))
    router.cmd("iptables -A FORWARD -i {} -o r1-eth3 -m state --state ESTABLISHED,RELATED -j ACCEPT".format(intf))
    router.cmd("iptables -A FORWARD -o {} -i r1-eth3 -j ACCEPT".format(intf))

if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=InternetTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    r1 = net.get('r1')

    # Set NAT / internet
    setup_internet_nat(r1)

    # Set DNS di semua host
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        h.cmd("bash -c 'echo \"nameserver 8.8.8.8\" > /etc/resolv.conf'")
        h.cmd("bash -c 'echo \"nameserver 8.8.4.4\" >> /etc/resolv.conf'")

    # Test konektivitas internal & internet
    info("*** Testing internal connectivity:\n")
    for hname in ("h1","h4","h7"):
        h = net.get(hname)
        result = h.cmd("ping -c 2 {}".format(r1.IP(hname[-1])))  # ping ke router
        info(result)

    info("*** Testing internet connectivity:\n")
    for hname in ("h1","h4","h7"):
        h = net.get(hname)
        result = h.cmd("ping -c 3 8.8.8.8")
        info(result)

    CLI(net)
    net.stop()
