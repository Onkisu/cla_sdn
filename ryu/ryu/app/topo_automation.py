#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import threading, subprocess, time

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

        # NAT ke interface yang connect ke Mininet NAT switch
        self.cmd("iptables -t nat -A POSTROUTING -o r1-eth99 -j MASQUERADE")
        self.cmd("iptables -A FORWARD -i r1-eth99 -o r1-eth1 -m state --state RELATED,ESTABLISHED -j ACCEPT")
        self.cmd("iptables -A FORWARD -i r1-eth1 -o r1-eth99 -j ACCEPT")
        self.cmd("iptables -A FORWARD -i r1-eth99 -o r1-eth2 -m state --state RELATED,ESTABLISHED -j ACCEPT")
        self.cmd("iptables -A FORWARD -i r1-eth2 -o r1-eth99 -j ACCEPT")
        self.cmd("iptables -A FORWARD -i r1-eth99 -o r1-eth3 -m state --state RELATED,ESTABLISHED -j ACCEPT")
        self.cmd("iptables -A FORWARD -i r1-eth3 -o r1-eth99 -j ACCEPT")

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

        # NAT switch untuk akses internet
        s_nat = self.addSwitch('s99')

        # Hosts
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')

        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')

        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Link hosts ke switch dengan bandwidth / delay / loss
        self.addLink(h1, s1, bw=5)
        self.addLink(h2, s1, bw=5)
        self.addLink(h3, s1, bw=5)

        self.addLink(h4, s2, bw=10, delay='32ms', loss=2)
        self.addLink(h5, s2, bw=10, delay='47ms', loss=2)
        self.addLink(h6, s2, bw=10)

        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)

        # Link router ke switch (gateway subnet)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip':'10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip':'10.0.2.254/24'})
        self.addLink(r1, s_nat, intfName1='r1-eth99', params1={'ip':'192.168.100.1/24'})  # NAT interface

        # Hubungkan switch NAT ke host default Mininet NAT
        nat0 = self.addHost('nat0', cls=NAT, ip='192.168.100.2/24', defaultRoute='via 192.168.100.1')
        self.addLink(s_nat, nat0)


def start_traffic(net):
    """Jalankan traffic otomatis"""
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h6 = net.get('h4', 'h5', 'h6')
    h7 = net.get('h7')

    info("*** Starting iperf servers\n")
    h4.cmd("iperf -s -u -p 443 &") 
    h5.cmd("iperf -s -u -p 443 &") 
    h7.cmd("iperf -u -s -p 1935 &") 

    info("*** Starting iperf clients\n")
    h1.cmd("bash -c 'while true; do iperf -u -c 10.0.1.1 -p 443 -b 4M -t 10 -i 5; sleep 1; done &'")
    h2.cmd("bash -c 'while true; do iperf -u -c 10.0.1.2 -p 443 -b 2M -t 10 -i 5; sleep 1; done &'")
    h3.cmd("bash -c 'while true; do iperf -u -c 10.0.2.1 -p 1935 -b 1M -t 10 -i 5; sleep 1; done &'")

def run_forecast_loop():
    """Loop tiap 15 menit panggil forecast.py"""
    while True:
        info("\n*** Running AI Forecast...\n")
        subprocess.call(["sudo","python3","forecast.py"])
        time.sleep(900)

if __name__=="__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    start_traffic(net)

    t = threading.Thread(target=run_forecast_loop, daemon=True)
    t.start()

    CLI(net)
    net.stop()
