#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        # Enable IP forwarding
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class VPSReadyTopo(Topo):
    def build(self, uplink_intf='ens3', uplink_ip='192.168.100.1/24', uplink_gw='10.171.241.1'):
        # Router
        r1 = self.addNode('r1', cls=LinuxRouter)

        # Internal switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s99 = self.addSwitch('s99')  # switch untuk uplink

        # Internal hosts
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')
        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')
        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Link hosts ke switch internal
        self.addLink(h1, s1); self.addLink(h2, s1); self.addLink(h3, s1)
        self.addLink(h4, s2); self.addLink(h5, s2); self.addLink(h6, s2)
        self.addLink(h7, s3)

        # Link router ke internal switch
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip':'10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip':'10.0.2.254/24'})

        # Link router ke uplink VPS
        self.addLink(r1, s99, intfName1='r1-eth0', params1={'ip':uplink_ip})

        # Simpan parameter uplink
        r1.uplink_intf = uplink_intf
        r1.uplink_gw = uplink_gw

def setup_internet_nat(router):
    """
    Konfigurasi NAT di r1 agar host internal bisa akses internet
    """
    intf = router.uplink_intf
    gw = router.uplink_gw

    # Set default route di router
    router.cmd(f"ip route add default via {gw} dev {intf}")

    # MASQUERADE semua subnet internal ke uplink
    router.cmd(f"iptables -t nat -A POSTROUTING -s 10.0.0.0/16 -o {intf} -j MASQUERADE")

    # Forward trafik untuk semua interface internal
    for eth in ['r1-eth1','r1-eth2','r1-eth3']:
        router.cmd(f"iptables -A FORWARD -i {intf} -o {eth} -m state --state ESTABLISHED,RELATED -j ACCEPT")
        router.cmd(f"iptables -A FORWARD -o {intf} -i {eth} -j ACCEPT")

if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(
        topo=VPSReadyTopo(uplink_intf='ens3',
                           uplink_ip='192.168.100.1/24',
                           uplink_gw='10.171.241.1'),
        switch=OVSSwitch,
        controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
        link=TCLink
    )
    net.start()

    r1 = net.get('r1')
    setup_internet_nat(r1)

    # Set DNS di semua host
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        h.cmd("bash -c 'echo \"nameserver 8.8.8.8\" > /etc/resolv.conf'")
        h.cmd("bash -c 'echo \"nameserver 8.8.4.4\" >> /etc/resolv.conf'")

    # Test konektivitas internal & internet
    info("*** Testing connectivity:\n")
    for hname in ("h1","h4","h7"):
        h = net.get(hname)
        info(f"Ping router dari {hname}:\n")
        info(h.cmd(f"ping -c 2 {r1.IP(hname[-1])}"))
        info(f"Ping internet dari {hname}:\n")
        info(h.cmd("ping -c 3 8.8.8.8"))

    CLI(net)
    net.stop()
