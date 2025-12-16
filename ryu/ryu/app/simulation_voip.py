#!/usr/bin/python3
"""
[SIMULATION LIVE LOG]
- Tidak masuk ke CLI Mininet.
- Langsung menampilkan Log VoIP secara real-time.
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
import time
import os
import sys
import subprocess

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

def follow_log(filename):
    """Fungsi untuk membaca log file secara live (seperti tail -f)"""
    try:
        f = subprocess.Popen(['tail','-F',filename], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        while True:
            line = f.stdout.readline()
            if line:
                print(f"[VoIP LOG] {line.decode().strip()}")
    except KeyboardInterrupt:
        return

def run():
    topo = LeafSpineTopo()
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    net = Mininet(topo=topo, controller=c0, switch=OVSSwitch, link=TCLink)
    
    try:
        net.start()
        
        # 1. Bersihkan Log Lama
        os.system("rm -f h1_send.log")
        
        # 2. Disable IPv6
        for h in net.hosts:
            h.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")

        info("\n*** PINGALL: Membangun Flow Rules... (Tunggu sebentar)\n")
        net.pingAll()
        time.sleep(2)

        # 3. Start Receivers
        h3, h4 = net.get('h3', 'h4')
        info("*** Menyiapkan Receiver...\n")
        h3.cmd('ITGRecv -l h3_recv.log > /dev/null 2>&1 &')
        h4.cmd('iperf -s > /dev/null 2>&1 &')
        
        # 4. Start Traffic Senders
        h1, h2 = net.get('h1', 'h2')
        info("*** Memulai VoIP Traffic & Background Traffic...\n")
        
        # VoIP (Log disimpan ke h1_send.log)
        h1.cmd("ITGSend -T UDP -a 10.0.0.3 -rp 5060 -C 50 -c 160 -t 300000 > h1_send.log 2>&1 &")
        
        # Background
        h2.cmd("iperf -c 10.0.0.4 -t 300 -b 10M > /dev/null 2>&1 &")

        info("\n" + "="*50)
        info("\n*** SIMULASI BERJALAN DALAM MODE LIVE VIEW ***")
        info("\n*** Menampilkan Log VoIP dari H1 (Tekan Ctrl+C untuk stop) ***\n")
        info("="*50 + "\n")
        
        # 5. LIVE LOG VIEW (Pengganti CLI)
        follow_log("h1_send.log")

    except Exception as e:
        info(f"*** Error: {e}\n")
    except KeyboardInterrupt:
        info("\n*** Stopping...\n")
        
    finally:
        info("\n*** Cleanup...\n")
        os.system('sudo ovs-ofctl del-flows s1 -O OpenFlow13')
        net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()