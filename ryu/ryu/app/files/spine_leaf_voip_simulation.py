#!/usr/bin/env python3
"""
Mininet Spine-Leaf Simulation - FLAT NETWORK FIX
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import psycopg2

class SpineLeafTopology:
    def __init__(self):
        self.net = None
        self.spines = []
        self.leaves = []
        self.hosts = []

    def create_database_table(self):
        # Database setup (sama seperti sebelumnya, pastikan table ada)
        try:
            conn = psycopg2.connect(host='103.181.142.165', database='development', 
                                  user='dev_one', password='hijack332.', port=5432)
            cur = conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS traffic.flow_stats_ (
                id SERIAL PRIMARY KEY, timestamp TIMESTAMP, dpid VARCHAR(20),
                src_ip VARCHAR(15), dst_ip VARCHAR(15), src_mac VARCHAR(17), dst_mac VARCHAR(17),
                ip_proto INTEGER, tp_src INTEGER, tp_dst INTEGER, bytes_tx BIGINT, bytes_rx BIGINT,
                pkts_tx INTEGER, pkts_rx INTEGER, duration_sec FLOAT, traffic_label VARCHAR(50));""")
            conn.commit(); conn.close()
        except Exception as e:
            info(f"*** DB Info: {e} (Continuing...)\n")

    def run(self):
        self.create_database_table()
        
        # 1. BUILD TOPOLOGY
        self.net = Mininet(
            controller=RemoteController,
            # [PENTING] Protokol harus OpenFlow13 biar ngomong sama Ryu
            switch=lambda name, **kwargs: OVSSwitch(name, stp=True, protocols='OpenFlow13', **kwargs),
            link=TCLink,
            autoSetMacs=True, autoStaticArp=True
        )
        
        self.net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)

        # Switches
        for i in range(1, 4): self.spines.append(self.net.addSwitch(f's{i}', dpid=f'000000000000000{i}'))
        for i in range(1, 4): self.leaves.append(self.net.addSwitch(f'l{i}', dpid=f'00000000000000{i+3}'))

        # Links (Spine-Leaf)
        for spine in self.spines:
            for leaf in self.leaves:
                self.net.addLink(spine, leaf, bw=1000, delay='1ms')

        # Hosts (FLAT NETWORK)
        host_id = 1
        for i, leaf in enumerate(self.leaves):
            for j in range(2):
                # [FIX FATAL] Semua host satu subnet (10.0.0.x)
                # Ini membuat mereka kirim ARP broadcast, bukan mencari Router gateway
                h = self.net.addHost(f'h{host_id}', ip=f'10.0.0.{host_id}/24')
                self.hosts.append(h)
                self.net.addLink(h, leaf, bw=100, delay='1ms')
                host_id += 1

        self.net.start()
        
        # 2. WAIT FOR CONVERGENCE
        info("*** Waiting 35s for STP & Switch Handshake...\n")
        time.sleep(35) 
        
        # 3. PING ALL
        info("*** Ping All...\n")
        self.net.pingAll()

        # 4. TRAFFIC GENERATION
        info("*** Starting D-ITG Traffic...\n")
        for h in self.hosts: h.cmd('ITGRecv &')
        time.sleep(2)
        
        for i, src in enumerate(self.hosts):
            dst = self.hosts[(i + 1) % len(self.hosts)] # Kirim ke tetangga
            src.cmd(f'ITGSend -T UDP -a {dst.IP()} -c 160 -C 50 -t 3600000 &')
            info(f"    {src.name} -> {dst.name} (UDP flow started)\n")

        CLI(self.net)
        self.net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    SpineLeafTopology().run()