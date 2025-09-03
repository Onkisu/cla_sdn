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
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class ComplexTopo(Topo):
    def build(self):
        # Router
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # Switch
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
        self.addLink(h1, s1, bw=5); self.addLink(h2, s1, bw=5); self.addLink(h3, s1, bw=5)
        self.addLink(h4, s2, bw=10); self.addLink(h5, s2, bw=10); self.addLink(h6, s2, bw=10)
        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)

        # Link router ke masing-masing switch (gateway tiap subnet)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

def start_traffic(net):
    """Jalankan traffic otomatis"""
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h6 = net.get('h4', 'h5', 'h6')
    h7 = net.get('h7')

    info("*** Starting iperf servers\n")
    h4.cmd("iperf -s -p 5001 > /tmp/h4_iperf.log &")
    h5.cmd("iperf -s -p 5002 > /tmp/h5_iperf.log &")
    h7.cmd("iperf -u -s -p 5003 > /tmp/h7_udp.log &")

    info("*** Starting iperf clients\n")
    h1.cmd("iperf -c 10.0.1.1 -p 5001 -t 60 -i 5 > /tmp/h1_to_h4.log &")
    h2.cmd("iperf -c 10.0.1.2 -p 5002 -t 60 -i 5 > /tmp/h2_to_h5.log &")
    h3.cmd("iperf -u -c 10.0.2.1 -p 5003 -b 1M -t 60 -i 5 > /tmp/h3_to_h7_udp.log &")

    info("*** Starting ping background\n")
    h1.cmd("ping 10.0.2.1 -i 0.2 -c 100 > /tmp/ping_h1_h7.log &")

if __name__=="__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    # mulai generate traffic
    start_traffic(net)

    CLI(net)  # biar bisa cek manual juga
    net.stop()
