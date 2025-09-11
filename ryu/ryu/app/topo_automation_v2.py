#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info

# Router internal
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

# Topology internal
class InternetTopo(Topo):
    def build(self):
        # Router internal
        r1 = self.addNode('r1', cls=LinuxRouter)

        # Internal switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        # Hosts
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

        # Link router ke switch
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(
        topo=InternetTopo(),
        switch=OVSSwitch,
        controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
        link=TCLink
    )
    net.start()

    r1 = net.get('r1')

    # Default route router ke VPS default gateway (ens3)
    # Dapatkan IP default gateway VPS
    gw = net.getNodeByName('r1').cmd("ip route | grep default | awk '{print $3}'").strip()
    if not gw:
        gw = "10.171.241.1"  # ganti sesuai default gateway VPSmu jika tidak otomatis
    r1.cmd(f"ip route add default via {gw}")

    # Set DNS untuk semua host
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        h.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")
        h.cmd("echo 'nameserver 8.8.4.4' >> /etc/resolv.conf")

    # Test koneksi internet dari semua host
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        info(f"*** Testing {hname} ping 8.8.8.8\n")
        info(h.cmd("ping -c 2 8.8.8.8"))

    CLI(net)
    net.stop()
