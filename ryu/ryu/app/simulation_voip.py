#!/usr/bin/python3
"""
[SIMULATION MAIN - FIXED]
- Auto PINGALL (Wajib agar collector bisa baca MAC)
- Auto Cleanup saat exit
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI
import time
import os

# Konfigurasi Host
HOSTS = {
    'h1': {'ip': '10.0.0.1', 'mac': '00:00:00:00:00:01'},
    'h2': {'ip': '10.0.0.2', 'mac': '00:00:00:00:00:02'},
    'h3': {'ip': '10.0.0.3', 'mac': '00:00:00:00:00:03'},
    'h4': {'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04'}
}

class LeafSpineTopo(Topo):
    def build(self):
        s1 = self.addSwitch('s1', dpid='0000000000000001')
        h1 = self.addHost('h1', **HOSTS['h1'])
        h2 = self.addHost('h2', **HOSTS['h2'])
        h3 = self.addHost('h3', **HOSTS['h3'])
        h4 = self.addHost('h4', **HOSTS['h4'])
        link_opts = dict(bw=100, delay='2ms')
        self.addLink(h1, s1, **link_opts)
        self.addLink(h2, s1, **link_opts)
        self.addLink(h3, s1, **link_opts)
        self.addLink(h4, s1, **link_opts)

def start_traffic_scenario(net):
    h1, h2, h3, h4 = net.get('h1', 'h2', 'h3', 'h4')
    
    info("\n*** Menyiapkan Server...\n")
    h3.cmd('ITGRecv -l h3_recv.log > h3_console.log 2>&1 &')
    h4.cmd('iperf -s > h4_iperf.log 2>&1 &')
    time.sleep(2)

    info("*** [H1] Start VoIP Call...\n")
    # Kirim VoIP (UDP)
    h1.cmd("ITGSend -T UDP -a 10.0.0.3 -rp 5060 -C 50 -c 160 -t 300000 > h1_send.log 2>&1 &")

    info("*** [H2] Start Background Traffic...\n")
    # Kirim Background (TCP)
    h2.cmd("iperf -c 10.0.0.4 -t 300 -b 10M > h2_traffic.log 2>&1 &")

def run():
    topo = LeafSpineTopo()
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    net = Mininet(topo=topo, controller=c0, switch=OVSSwitch, link=TCLink)
    
    try:
        net.start()
        for h in net.hosts:
            h.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")

        info("\n*** Melakukan Ping All (Wajib agar Flow terbentuk)...\n")
        net.pingAll() 

        start_traffic_scenario(net)
        
        info("\n*** Simulasi Berjalan! Tekan Ctrl+C untuk berhenti.\n")
        CLI(net)
        
    except Exception as e:
        info(f"*** Error: {e}\n")
        
    finally:
        info("\n*** Cleanup...\n")
        os.system('sudo ovs-ofctl del-flows s1 -O OpenFlow13')
        net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()