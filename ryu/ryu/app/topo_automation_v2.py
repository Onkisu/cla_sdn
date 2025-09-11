#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.nodelib import NAT
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import threading, subprocess, time

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class InternetTopo(Topo):
    def build(self):
        # Router internal
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # NAT node with proper external interface (using ens3)
        nat = self.addNode('nat0', cls=NAT, ip='10.255.0.1/24', 
                          inNamespace=False,
                          subnet='10.255.0.0/24')
        
        # Switch for NAT connection
        s_nat = self.addSwitch('s99')
        self.addLink(nat, s_nat)
        self.addLink(r1, s_nat, intfName1='r1-eth0', params1={'ip': '10.255.0.254/24'})

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

        # Link host ke switch
        self.addLink(h1, s1); self.addLink(h2, s1); self.addLink(h3, s1)
        self.addLink(h4, s2); self.addLink(h5, s2); self.addLink(h6, s2)
        self.addLink(h7, s3)

        # Link router ke switch-subnet
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

def run_forecast_loop():
    while True:
        info("\n*** Running AI Forecast...\n")
        subprocess.call(["sudo", "python3", "forecast.py"])
        time.sleep(900)

if __name__=="__main__":
    setLogLevel("info")
    net = Mininet(
        topo=InternetTopo(),
        switch=OVSSwitch,
        controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
        link=TCLink
    )
    net.start()

    # Get NAT and router objects
    nat = net.get('nat0')
    r1 = net.get('r1')

    # Configure NAT to use ens3 interface for external connectivity
    nat.cmd('ifconfig nat0-eth0 down')
    nat.cmd('ifconfig nat0-eth0 0')
    nat.cmd('ovs-vsctl del-port s99 nat0-eth0')
    nat.cmd('ovs-vsctl add-port s99 ens3')
    
    # Configure NAT IP on the bridge interface
    nat.cmd('ifconfig s99 10.255.0.1/24 up')
    
    # Set default route on router to NAT
    r1.cmd("ip route del default 2>/dev/null || true")
    r1.cmd("ip route add default via 10.255.0.1")

    # Add routes for internal networks on NAT
    nat.cmd("ip route add 10.0.0.0/24 via 10.255.0.254")
    nat.cmd("ip route add 10.0.1.0/24 via 10.255.0.254")
    nat.cmd("ip route add 10.0.2.0/24 via 10.255.0.254")

    # Enable IP forwarding on NAT
    nat.cmd("sysctl -w net.ipv4.ip_forward=1")
    
    # Configure iptables for NAT
    nat.cmd("iptables -t nat -A POSTROUTING -o ens3 -j MASQUERADE")
    nat.cmd("iptables -A FORWARD -i ens3 -o s99 -m state --state RELATED,ESTABLISHED -j ACCEPT")
    nat.cmd("iptables -A FORWARD -i s99 -o ens3 -j ACCEPT")

    # Set DNS on all hosts
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        h.cmd("bash -c 'echo \"nameserver 8.8.8.8\" > /etc/resolv.conf'")

    # Start forecast loop in background
    t = threading.Thread(target=run_forecast_loop, daemon=True)
    t.start()

    # Enter CLI
    CLI(net)
    net.stop()