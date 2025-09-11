#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.nodelib import NAT
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info

class LinuxRouter(Node):
    """Router internal dengan IP forwarding"""
    def config(self, **params):
        super().config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super().terminate()

class InternetTopo(Topo):
    """Topo internal + NAT untuk internet via VPS ens3"""
    def build(self):
        # Router internal
        r1 = self.addNode('r1', cls=LinuxRouter)

        # NAT node → arahkan ke interface fisik VPS ens3
        nat = self.addNode('nat0', cls=NAT, ip='192.168.100.254/24',
                           inNamespace=False, inetIntf='ens3')
        s_nat = self.addSwitch('s99')
        self.addLink(nat, s_nat)
        self.addLink(r1, s_nat, intfName1='r1-eth0', params1={'ip':'192.168.100.1/24'})

        # Internal switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        # Hosts
        hosts = [
            ('h1','10.0.0.1/24','10.0.0.254'),
            ('h2','10.0.0.2/24','10.0.0.254'),
            ('h3','10.0.0.3/24','10.0.0.254'),
            ('h4','10.0.1.1/24','10.0.1.254'),
            ('h5','10.0.1.2/24','10.0.1.254'),
            ('h6','10.0.1.3/24','10.0.1.254'),
            ('h7','10.0.2.1/24','10.0.2.254')
        ]
        host_objs = {}
        for name, ip, gw in hosts:
            host_objs[name] = self.addHost(name, ip=ip, defaultRoute=f'via {gw}')

        # Link hosts → switches
        self.addLink(host_objs['h1'], s1)
        self.addLink(host_objs['h2'], s1)
        self.addLink(host_objs['h3'], s1)
        self.addLink(host_objs['h4'], s2)
        self.addLink(host_objs['h5'], s2)
        self.addLink(host_objs['h6'], s2)
        self.addLink(host_objs['h7'], s3)

        # Router → switches
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip':'10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip':'10.0.2.254/24'})

if __name__=="__main__":
    setLogLevel("info")
    net = Mininet(topo=InternetTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    # Ambil node
    r1 = net.get('r1')
    nat = net.get('nat0')

    # Konfigurasi NAT
    nat.configDefault()

    # Router internal default route ke NAT
    r1.cmd("ip route add default via 192.168.100.254")

    # Set DNS di semua host
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        h.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")
        h.cmd("echo 'nameserver 8.8.4.4' >> /etc/resolv.conf")

    # Tes koneksi internet host
    info("*** Tes koneksi internet semua host:\n")
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        info(f"{hname} ping 8.8.8.8:\n")
        info(h.cmd("ping -c 2 8.8.8.8"))

    CLI(net)
    net.stop()
