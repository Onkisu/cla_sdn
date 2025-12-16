#!/usr/bin/python3
"""
[SIMULATION LIVE - SHOW TRAFFIC LOGS]
- Auto Pingall
- Force Traffic Log to Screen (tail -f)
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
import time
import os
import subprocess
import signal
import sys

# Konfigurasi Host
HOSTS = {
    'h1': {'ip': '10.0.0.1', 'mac': '00:00:00:00:00:01'}, # VoIP Sender
    'h2': {'ip': '10.0.0.2', 'mac': '00:00:00:00:00:02'}, # BG Sender
    'h3': {'ip': '10.0.0.3', 'mac': '00:00:00:00:00:03'}, # VoIP Recv
    'h4': {'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04'}  # BG Recv
}

class LeafSpineTopo(Topo):
    def build(self):
        s1 = self.addSwitch('s1', dpid='0000000000000001')
        h1 = self.addHost('h1', **HOSTS['h1'])
        h2 = self.addHost('h2', **HOSTS['h2'])
        h3 = self.addHost('h3', **HOSTS['h3'])
        h4 = self.addHost('h4', **HOSTS['h4'])
        
        # Link 100Mbps, 2ms
        self.addLink(h1, s1, bw=100, delay='2ms')
        self.addLink(h2, s1, bw=100, delay='2ms')
        self.addLink(h3, s1, bw=100, delay='2ms')
        self.addLink(h4, s1, bw=100, delay='2ms')

def run():
    topo = LeafSpineTopo()
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    # PENTING: autoSetMacs=True dan OpenFlow13
    net = Mininet(topo=topo, controller=c0, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    
    try:
        net.start()
        
        # Paksa Switch pakai OpenFlow 1.3
        for s in net.switches:
            s.cmd("ovs-vsctl set Bridge %s protocols=OpenFlow13" % s.name)

        # Bersihkan log lama
        os.system("rm -f h1_send.log")

        info("\n*** PINGALL: Learning MAC Address (Wajib)...\n")
        net.pingAll()
        time.sleep(1)

        info("\n*** Start Traffic Generator (Duration: 300,000s)...\n")
        
        # Receiver
        h3, h4 = net.get('h3', 'h4')
        h3.cmd('ITGRecv -l h3_recv.log > /dev/null 2>&1 &')
        h4.cmd('iperf -s > /dev/null 2>&1 &')
        
        # Sender
        h1, h2 = net.get('h1', 'h2')
        
        # VoIP (H1->H3) Log ke file
        info(">>> [H1] Mengirim VoIP Packets...\n")
        h1.cmd("ITGSend -T UDP -a 10.0.0.3 -rp 5060 -C 50 -c 160 -t 300000 > h1_send.log 2>&1 &")
        
        # Background (H2->H4)
        info(">>> [H2] Mengirim Background Traffic...\n")
        h2.cmd("iperf -c 10.0.0.4 -t 300000 -b 10M > /dev/null 2>&1 &")

        info("\n" + "="*60)
        info("\n*** LIVE TRAFFIC LOG (H1 VoIP Sender) ***")
        info("\n*** Layar ini menampilkan log asli dari D-ITG. ***")
        info("\n*** Jika angka bertambah, berarti trafik sedang dikirim. ***")
        info("\n*** Tekan Ctrl+C untuk STOP Simulasi. ***\n")
        info("="*60 + "\n")
        
        # TRIK LIVE VIEW: Tail -f log file ke layar utama
        # Ini akan menahan terminal disini sampai di-cancel
        subprocess.call(["tail", "-f", "h1_send.log"])

    except KeyboardInterrupt:
        info("\n*** Stopping...\n")
    finally:
        info("*** Cleaning up...\n")
        os.system('sudo ovs-ofctl del-flows s1 -O OpenFlow13')
        net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()