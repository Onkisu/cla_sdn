#!/usr/bin/python3
"""
[SIMULATION MAIN] Leaf-Spine Topology
Menjalankan topologi dan memanggil script trafik eksternal.
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI
import time
import os

# Konfigurasi Host Statis (MAC & IP Sesuai PDF)
HOSTS = {
    'h1': {'ip': '10.0.0.1', 'mac': '00:00:00:00:00:01'}, # VoIP Caller
    'h2': {'ip': '10.0.0.2', 'mac': '00:00:00:00:00:02'}, # Attacker (Background)
    'h3': {'ip': '10.0.0.3', 'mac': '00:00:00:00:00:03'}, # VoIP Receiver
    'h4': {'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04'}  # Victim
}

class LeafSpineTopo(Topo):
    def build(self):
        # Topology Setup (Tetap sama)
        s1 = self.addSwitch('s1', dpid='0000000000000001')
        s2 = self.addSwitch('s2', dpid='0000000000000002')
        l1 = self.addSwitch('l1', dpid='0000000000000011')
        l2 = self.addSwitch('l2', dpid='0000000000000012')

        # Link Backbone
        link_core = dict(bw=100, delay='1ms')
        for s in [s1, s2]:
            for l in [l1, l2]:
                self.addLink(s, l, **link_core)

        # Host Links
        link_host = dict(bw=50, delay='2ms')
        h1 = self.addHost('h1', ip=HOSTS['h1']['ip'], mac=HOSTS['h1']['mac'])
        h2 = self.addHost('h2', ip=HOSTS['h2']['ip'], mac=HOSTS['h2']['mac'])
        h3 = self.addHost('h3', ip=HOSTS['h3']['ip'], mac=HOSTS['h3']['mac'])
        h4 = self.addHost('h4', ip=HOSTS['h4']['ip'], mac=HOSTS['h4']['mac'])

        self.addLink(h1, l1, **link_host)
        self.addLink(h2, l1, **link_host)
        self.addLink(h3, l2, **link_host)
        self.addLink(h4, l2, **link_host)

def start_traffic_scenario(net):
    h1, h2, h3, h4 = net.get('h1', 'h2', 'h3', 'h4')
    
    info("*** Menyiapkan ITGRecv (Penerima)...\n")
    h3.cmd('ITGRecv -l h3_recv.log &')
    h4.cmd('ITGRecv -l h4_recv.log &')
    time.sleep(2)

    # ---------------------------------------------------------
    # 1. VoIP TRAFFIC (Stabil) - H1 ke H3
    # ---------------------------------------------------------
    info("*** [H1] Memulai VoIP Call (Stabil, UDP, G.711)...\n")
    # Kita buat VoIP tetap konstan agar kita bisa lihat efek gangguan terhadapnya
    # Port 3000 -> 5060
    h1.cmd("ITGSend -T UDP -a 10.0.0.3 -sp 3000 -rp 5060 -C 50 -c 160 -t 300000 &")

    # ---------------------------------------------------------
    # 2. BACKGROUND TRAFFIC (Dinamis) - H2 ke H4
    # ---------------------------------------------------------
    info("*** [H2] Menjalankan Script Traffic Dinamis (background_traffic.py)...\n")
    # H2 akan mengeksekusi file python eksternal
    # Pastikan file background_traffic.py ada di folder yang sama
    h2.cmd("python3 background_traffic.py > h2_traffic.log 2>&1 &")

def run():
    topo = LeafSpineTopo()
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    net = Mininet(topo=topo, controller=c0, switch=OVSSwitch, link=TCLink)
    net.start()
    
    # Disable IPv6
    for h in net.hosts:
        h.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")

    # Jalankan Skenario
    start_traffic_scenario(net)
    
    info("\n*** Simulasi Berjalan!\n")
    info("*** VoIP berjalan stabil di background.\n")
    info("*** Background Traffic (H2) sedang melakukan random micro-bursts.\n")
    info("*** Jalankan 'sudo python3 collection_fix_proposal.py' di terminal lain untuk melihat data.\n")
    
    CLI(net)
    
    info("*** Menghentikan Network...\n")
    net.stop()
    os.system("killall ITGSend ITGRecv python3") # Kill script python background juga

if __name__ == '__main__':
    setLogLevel('info')
    run()