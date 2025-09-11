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
        # Main router that will connect to your VPS network
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.171.241.200/24')

        # Internal router for your Mininet network
        r2 = self.addNode('r2', cls=LinuxRouter, ip='192.168.100.2/24')

        # NAT node to bridge between VPS network and Mininet network
        nat = self.addNode('nat0', cls=NAT, ip='10.171.241.201/24', 
                          inNamespace=False)
        
        # Switch for connecting to VPS network
        s_vps = self.addSwitch('s99')
        self.addLink(nat, s_vps)
        self.addLink(r1, s_vps, intfName1='r1-eth0', params1={'ip': '10.171.241.200/24'})

        # Switch for connecting internal networks
        s_internal = self.addSwitch('s98')
        self.addLink(r1, s_internal, intfName1='r1-eth1', params1={'ip': '192.168.100.1/24'})
        self.addLink(r2, s_internal, intfName1='r2-eth0', params1={'ip': '192.168.100.2/24'})

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

        # Link hosts to switches
        self.addLink(h1, s1); self.addLink(h2, s1); self.addLink(h3, s1)
        self.addLink(h4, s2); self.addLink(h5, s2); self.addLink(h6, s2)
        self.addLink(h7, s3)

        # Link internal router to subnet switches
        self.addLink(r2, s1, intfName1='r2-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r2, s2, intfName1='r2-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r2, s3, intfName1='r2-eth3', params1={'ip': '10.0.2.254/24'})

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

    # Configure NAT to use the VPS's default gateway
    try:
        default_gw = subprocess.check_output("ip route | grep default | awk '{print $3}'", shell=True).decode().strip()
        info("*** Default gateway: %s\n" % default_gw)
    except:
        default_gw = "10.171.241.1"  # Fallback to typical gateway for this subnet
        info("*** Using fallback gateway: %s\n" % default_gw)
    
    # Configure NAT with the correct gateway
    nat.cmd("ip route del default 2>/dev/null || true")
    nat.cmd("ip route add default via %s" % default_gw)
    
    # Set up routing
    # r1 needs to know how to reach the internal network
    r1.cmd("ip route add 10.0.0.0/16 via 192.168.100.2")
    
    # r1 needs default route through NAT
    r1.cmd("ip route del default 2>/dev/null || true")
    r1.cmd("ip route add default via 10.171.241.201")
    
    # r2 needs default route through r1
    r2.cmd("ip route del default 2>/dev/null || true")
    r2.cmd("ip route add default via 192.168.100.1")
    
    # Add route on NAT for internal networks
    nat.cmd("ip route add 10.0.0.0/16 via 10.171.241.200")
    
    # Enable IP forwarding on all routers
    r1.cmd("sysctl -w net.ipv4.ip_forward=1")
    r2.cmd("sysctl -w net.ipv4.ip_forward=1")
    
    # Configure NAT iptables rules
    nat.cmd("sysctl -w net.ipv4.ip_forward=1")
    nat.cmd("iptables -t nat -F")
    nat.cmd("iptables -t nat -A POSTROUTING -o nat0-eth0 -j MASQUERADE")
    nat.cmd("iptables -A FORWARD -i nat0-eth0 -o s99 -m state --state RELATED,ESTABLISHED -j ACCEPT")
    nat.cmd("iptables -A FORWARD -i s99 -o nat0-eth0 -j ACCEPT")
    
    # Set DNS on all hosts
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        h = net.get(hname)
        h.cmd("bash -c 'echo \"nameserver 8.8.8.8\" > /etc/resolv.conf'")
        h.cmd("bash -c 'echo \"nameserver 8.8.4.4\" >> /etc/resolv.conf'")

    # Test connectivity between r2 and r1 first
    info("*** Testing connectivity from r2 to r1:\n")
    result = r2.cmd("ping -c 2 192.168.100.1")
    info(result)
    
    if "64 bytes" in result:
        info("*** Connectivity between r2 and r1 is working!\n")
        
        # Test connectivity from r1 to NAT
        info("*** Testing connectivity from r1 to NAT:\n")
        result = r1.cmd("ping -c 2 10.171.241.201")
        info(result)
        
        if "64 bytes" in result:
            info("*** Connectivity from r1 to NAT is working!\n")
            
            # Test connectivity from NAT to gateway
            info("*** Testing connectivity from NAT to gateway:\n")
            result = nat.cmd("ping -c 2 %s" % default_gw)
            info(result)
            
            if "64 bytes" in result:
                info("*** Connectivity from NAT to gateway is working!\n")
                
                # Test internet connectivity from NAT
                info("*** Testing internet connectivity from NAT:\n")
                result = nat.cmd("ping -c 2 8.8.8.8")
                info(result)
                
                if "64 bytes" in result:
                    info("*** Internet connectivity from NAT is working!\n")
                    
                    # Test internet connectivity from host
                    info("*** Testing internet connectivity from host...\n")
                    result = net.get('h1').cmd('ping -c 3 8.8.8.8')
                    info(result)
                    
                    if "64 bytes" in result:
                        info("*** Internet connectivity is working!\n")
                    else:
                        info("*** Internet connectivity test failed from host\n")
                else:
                    info("*** Internet connectivity test failed from NAT\n")
            else:
                info("*** Connectivity from NAT to gateway failed\n")
        else:
            info("*** Connectivity from r1 to NAT failed\n")
    else:
        info("*** Connectivity between r2 and r1 failed\n")

    # Start forecast loop in background
    # t = threading.Thread(target=run_forecast_loop, daemon=True)
    # t.start()

    # Enter CLI
    CLI(net)
    net.stop()