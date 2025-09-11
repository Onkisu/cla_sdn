#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, Node
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

def run():
    net = Mininet(controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633), link=TCLink)
    
    r1 = net.addHost('r1', cls=LinuxRouter)
    
    # Switch internal
    s1 = net.addSwitch('s1')
    s2 = net.addSwitch('s2')
    s3 = net.addSwitch('s3')
    
    # Hosts
    h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
    h2 = net.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
    h3 = net.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')
    h4 = net.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
    h5 = net.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
    h6 = net.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')
    h7 = net.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')
    
    # Link hosts ke switch
    net.addLink(h1, s1); net.addLink(h2, s1); net.addLink(h3, s1)
    net.addLink(h4, s2); net.addLink(h5, s2); net.addLink(h6, s2)
    net.addLink(h7, s3)
    
    # Link router ke switch
    net.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.0.254/24'})
    net.addLink(r1, s2, intfName1='r1-eth2', params1={'ip':'10.0.1.254/24'})
    net.addLink(r1, s3, intfName1='r1-eth3', params1={'ip':'10.0.2.254/24'})
    
    # Link router ke VPS
    net.addLink(r1, None, intfName1='r1-eth0', params1={'ip':'192.168.200.2/24'}) # peer vps-to-r1 harus sudah up
    
    net.start()
    
    # Set DNS untuk semua host
    for h in [h1,h2,h3,h4,h5,h6,h7]:
        h.cmd("bash -c 'echo nameserver 8.8.8.8 > /etc/resolv.conf'")
        h.cmd("bash -c 'echo nameserver 8.8.4.4 >> /etc/resolv.conf'")
    
    info("*** Testing ping internal\n")
    for h in [h1,h2,h3,h4,h5,h6,h7]:
        info(h.name, h.cmd("ping -c 2 " + h.defaultIntf().ip))
    
    CLI(net)
    net.stop()

if __name__=="__main__":
    setLogLevel('info')
    run()
