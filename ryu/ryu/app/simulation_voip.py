#!/usr/bin/python3
"""
[SIMULATION MODULAR]
- Topologi Leaf-Spine
- Traffic Generator (ITGSend & Iperf) berdasarkan config_voip.py
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI
import time
import os
import sys

# Import Config
try:
    import config_voip as cfg
except ImportError:
    print("Error: config_voip.py tidak ditemukan!")
    sys.exit(1)

class LeafSpineTopo(Topo):
    def build(self):
        s1 = self.addSwitch('s1', dpid='0000000000000001')
        
        # Add Hosts secara dinamis dari Config
        hosts_obj = {}
        for name, meta in cfg.HOST_MAP.items():
            hosts_obj[name] = self.addHost(name, ip=meta['ip'], mac=meta['mac'])
            
            # Link Config (100Mbps, 2ms)
            self.addLink(hosts_obj[name], s1, bw=100, delay='2ms')

def run():
    topo = LeafSpineTopo()
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    net = Mininet(topo=topo, controller=c0, switch=OVSSwitch, link=TCLink)
    
    try:
        net.start()
        
        # 1. Disable IPv6
        for h in net.hosts:
            h.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")

        # 2. Ping All (WAJIB)
        info("\n*** Melakukan Ping All (Learning MAC Address)...\n")
        net.pingAll()
        time.sleep(1)

        # 3. Start Receivers (Servers)
        info("\n*** Menjalankan Receiver...\n")
        h3 = net.get('h3')
        h4 = net.get('h4')
        
        h3.cmd('ITGRecv -l h3.log > /dev/null 2>&1 &') # VoIP Server
        h4.cmd('iperf -s > /dev/null 2>&1 &')          # Bg Server

        time.sleep(2)

        # 4. Start Senders (Traffic Generation)
        info("*** Menjalankan Traffic Generator...\n")
        h1 = net.get('h1') # VoIP Sender
        h2 = net.get('h2') # Background Sender

        # VoIP (UDP G.711)
        info(f" -> {cfg.HOST_MAP['h1']['type']} (H1 -> H3)\n")
        h1.cmd(f"ITGSend -T UDP -a {cfg.HOST_MAP['h1']['target_ip']} -rp 5060 -C 50 -c 160 -t 300000 > /dev/null 2>&1 &")

        # Background (TCP)
        info(f" -> {cfg.HOST_MAP['h2']['type']} (H2 -> H4)\n")
        h2.cmd(f"iperf -c {cfg.HOST_MAP['h2']['target_ip']} -t 300 -b 10M > /dev/null 2>&1 &")

        info("\n*** Simulasi Berjalan! Jalankan 'collector_modular.py' sekarang.\n")
        CLI(net)

    except Exception as e:
        info(f"*** Error: {e}\n")

    finally:
        info("\n*** Membersihkan OVS Flows & Stop...\n")
        os.system('sudo ovs-ofctl del-flows s1 -O OpenFlow13')
        net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()