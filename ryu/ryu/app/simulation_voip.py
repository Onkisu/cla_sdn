#!/usr/bin/python3
"""
[SIMULATION FINAL - REAL ACTIVITY MONITOR]
- Menjalankan Traffic Generator
- Menampilkan Live TX Statistics dari Interface H1
- Memberikan bukti visual bahwa trafik berjalan
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
import time
import os
import sys

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
        
        self.addLink(h1, s1, bw=100, delay='2ms')
        self.addLink(h2, s1, bw=100, delay='2ms')
        self.addLink(h3, s1, bw=100, delay='2ms')
        self.addLink(h4, s1, bw=100, delay='2ms')

def read_tx_bytes(host_node, intf_name):
    """Membaca TX Bytes langsung dari kernel"""
    cmd = f"cat /sys/class/net/{intf_name}/statistics/tx_bytes"
    try:
        # Jalankan cat di dalam namespace host
        result = host_node.cmd(cmd).strip()
        return int(result)
    except:
        return 0

def monitor_live(net):
    """Looping untuk menampilkan statistik trafik H1"""
    h1 = net.get('h1')
    prev_bytes = read_tx_bytes(h1, 'h1-eth0')
    start_time = time.time()
    
    print("\n" + "="*60)
    print("*** MONITORING AKTIVITAS H1 (VoIP Sender) ***")
    print(f"{'TIME':<10} | {'TOTAL TX (Bytes)':<20} | {'STATUS'}")
    print("="*60)

    try:
        while True:
            time.sleep(1)
            curr_bytes = read_tx_bytes(h1, 'h1-eth0')
            delta = curr_bytes - prev_bytes
            now = datetime.now().strftime("%H:%M:%S")
            
            status = "SILENT"
            if delta > 1000:
                status = ">>> SENDING TRAFFIC"
            
            print(f"{now:<10} | {curr_bytes:<20} | {status}")
            
            prev_bytes = curr_bytes
            
    except KeyboardInterrupt:
        return

from datetime import datetime

def run():
    topo = LeafSpineTopo()
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    net = Mininet(topo=topo, controller=c0, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    
    try:
        net.start()
        for s in net.switches:
            s.cmd("ovs-vsctl set Bridge %s protocols=OpenFlow13" % s.name)

        # Matikan IPv6 agar tidak noise
        for h in net.hosts:
            h.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")

        info("\n*** PINGALL: Learning MAC Address...\n")
        net.pingAll()
        time.sleep(1)

        info("\n*** Starting Traffic (Duration: 300,000s)...\n")
        
        # Receiver
        h3, h4 = net.get('h3', 'h4')
        h3.cmd('ITGRecv -l h3_recv.log > /dev/null 2>&1 &')
        h4.cmd('iperf -s > /dev/null 2>&1 &')
        
        # Sender
        h1, h2 = net.get('h1', 'h2')
        
        # VoIP (H1->H3) - UDP
        # stdbuf -o0 memaksa agar buffer langsung dikirim (opsional)
        h1.cmd("ITGSend -T UDP -a 10.0.0.3 -rp 5060 -C 50 -c 160 -t 300000 > /dev/null 2>&1 &")
        
        # Background (H2->H4) - TCP
        h2.cmd("iperf -c 10.0.0.4 -t 300000 -b 10M > /dev/null 2>&1 &")

        # MASUK KE LIVE MONITOR
        monitor_live(net)

    except KeyboardInterrupt:
        info("\n*** Stopping...\n")
    finally:
        info("*** Cleanup...\n")
        os.system('sudo ovs-ofctl del-flows s1 -O OpenFlow13')
        net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()