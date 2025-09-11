#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.nodelib import NAT
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import threading, subprocess, time

# Custom router node to enable IP forwarding
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

# The network topology
class InternetTopo(Topo):
    def build(self):
        # Add routers. r1 is the gateway to the internet, r2 is the internal router.
        r1 = self.addNode('r1', cls=LinuxRouter)
        r2 = self.addNode('r2', cls=LinuxRouter, ip='192.168.100.2/24')
        
        # We will use the NAT node to connect the Mininet topology to the host's
        # real-world network and provide internet access.
        # This node must be a gateway for the Mininet network.
        nat = self.addNode('nat0', cls=NAT, inNamespace=False)
        
        # Switches for different subnets
        # NOTE: Renamed switches to remove hyphens, as they cause a "datapath ID" error.
        s_internet = self.addSwitch('s_inet')  # Connects r1 to the internet via NAT
        s_internal = self.addSwitch('s_int')  # Connects r1 to r2
        s1 = self.addSwitch('s1')  # Subnet 10.0.0.0/24
        s2 = self.addSwitch('s2')  # Subnet 10.0.1.0/24
        s3 = self.addSwitch('s3')  # Subnet 10.0.2.0/24

        # Link NAT to the internet switch
        self.addLink(nat, s_internet)
        
        # Link r1 to the internet switch and the internal switch
        self.addLink(r1, s_internet, intfName1='r1-eth0', params1={'ip': '10.171.241.200/24'})
        self.addLink(r1, s_internal, intfName1='r1-eth1', params1={'ip': '192.168.100.1/24'})
        
        # Link r2 to the internal switch and the subnet switches
        self.addLink(r2, s_internal, intfName1='r2-eth0', params1={'ip': '192.168.100.2/24'})
        self.addLink(r2, s1, intfName1='r2-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r2, s2, intfName1='r2-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r2, s3, intfName1='r2-eth3', params1={'ip': '10.0.2.254/24'})

        # Add hosts and link them to their respective switches
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')
        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')
        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        self.addLink(h1, s1); self.addLink(h2, s1); self.addLink(h3, s1)
        self.addLink(h4, s2); self.addLink(h5, s2); self.addLink(h6, s2)
        self.addLink(h7, s3)

# Function to run the AI forecast script in a loop
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

    # Get nodes
    nat = net.get('nat0')
    r1 = net.get('r1')
    r2 = net.get('r2')
    
    # Manually configure NAT to use the host's default gateway for internet access
    info("*** Setting up NAT and routing...\n")
    nat.cmd('ip addr del 10.0/8 dev nat0-eth0')
    nat.cmd('ip link set nat0-eth0 down')
    
    # Configure NAT properly by making it the default gateway for r1's network
    # Note: We don't need a default gateway on the NAT node itself if its
    # interface is correctly configured on the host's bridge.
    
    # Set up routing for the entire topology
    # r1 must know how to reach the internal networks (10.0.0.0/16) via r2
    r1.cmd("ip route add 10.0.0.0/16 via 192.168.100.2")
    # r1's default gateway is the NAT node for internet access
    r1.cmd("ip route del default 2>/dev/null || true")
    r1.cmd("ip route add default via 10.171.241.201")
    
    # r2's default gateway is r1 for all external traffic
    r2.cmd("ip route del default 2>/dev/null || true")
    r2.cmd("ip route add default via 192.168.100.1")
    
    # Configure NAT iptables rules for masquerading
    # This is a critical step to allow internal IPs to reach the internet
    nat.cmd("iptables -t nat -A POSTROUTING -o ens3 -j MASQUERADE")
    nat.cmd("iptables -A FORWARD -i ens3 -o s_inet -m state --state RELATED,ESTABLISHED -j ACCEPT")
    nat.cmd("iptables -A FORWARD -i s_inet -o ens3 -j ACCEPT")

    # Set DNS on all hosts
    for hname in ("h1", "h2", "h3", "h4", "h5", "h6", "h7"):
        h = net.get(hname)
        h.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")
        h.cmd("echo 'nameserver 8.8.4.4' >> /etc/resolv.conf")

    # Test connectivity from the most nested host
    info("\n*** Testing Internet connectivity from host h1...\n")
    h1 = net.get('h1')
    result = h1.cmd('ping -c 3 google.com')
    info(result)
    
    if "bytes from" in result:
        info("*** Internet connectivity is working!\n")
    else:
        info("*** Internet connectivity test failed from h1\n")

    # Start forecast loop in background
    # t = threading.Thread(target=run_forecast_loop, daemon=True)
    # t.start()

    # Enter CLI
    CLI(net)
    net.stop()
