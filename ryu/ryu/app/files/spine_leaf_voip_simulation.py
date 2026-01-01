#!/usr/bin/env python3
"""
FIXED Mininet Spine-Leaf Topology
Ensures port ordering matches Controller logic (Ports 1-3 are Uplinks)
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess

def check_ditg():
    if subprocess.run(['which', 'ITGSend'], capture_output=True).returncode != 0:
        info("WARNING: D-ITG not found. Install: sudo apt-get install d-itg\n")
        return False
    return True

def run():
    info("*** Starting Spine-Leaf VoIP Simulation (FIXED)\n")
    
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    
    info("*** Adding Controller\n")
    # Arahkan ke IP lokal dimana script python Ryu jalan
    net.addController('c0', ip='127.0.0.1', port=6653)
    
    spines = []
    leaves = []
    
    # Create Spines (DPID 1-3)
    for i in range(1, 4):
        s = net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13')
        spines.append(s)
        
    # Create Leaves (DPID 4-6)
    for i in range(1, 4):
        l = net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13')
        leaves.append(l)
        
    info("*** Creating Links (Careful ordering for Controller Logic)\n")
    # Connect Leaves to Spines FIRST.
    # This ensures Leaf Port 1 -> Spine 1, Leaf Port 2 -> Spine 2, etc.
    # This is critical for the "Split Horizon" logic in the controller.
    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine, bw=1000, delay='1ms')
            
    info("*** Adding Hosts\n")
    # Hosts connect to Leaves NEXT. So Host ports will be > 3.
    hosts = []
    host_id = 1
    for leaf in leaves:
        for j in range(2): # 2 hosts per leaf
            h = net.addHost(f'h{host_id}', ip=f'10.0.0.{host_id}/24')
            net.addLink(h, leaf, bw=100, delay='1ms')
            hosts.append(h)
            host_id += 1

    info("*** Starting Network\n")
    net.start()
    
    # Tunggu sebentar agar Switch connect ke Controller
    time.sleep(3)
    
    info("*** Testing Connectivity (PingAll)\n")
    # Pingall harusnya sukses sekarang karena Loop Protection sudah aktif
    net.pingAll()
    
    if check_ditg():
        info("*** Starting D-ITG Traffic\n")
        # Start Recv on odd hosts
        for h in hosts[1::2]:
            h.cmd('ITGRecv -l /tmp/recv.log &')
        
        time.sleep(1)
        
        # Start Send on even hosts
        # Start Send on even hosts
        for i, h in enumerate(hosts[::2]):
            dst = hosts[i*2+1]
            # UDP Traffic (VoIP G.711)
            # GANTI -t 60000 MENJADI -t 3600000 (1 JAM)
            h.cmd(f'ITGSend -T UDP -a {dst.IP()} -c 160 -C 50 -t 3600000 -l /tmp/send.log &')
            info(f"    {h.name} -> {dst.name} (UDP VoIP started for 1 hour)\n")
            
    info("*** Running CLI\n")
    CLI(net)
    
    info("*** Stopping Network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()