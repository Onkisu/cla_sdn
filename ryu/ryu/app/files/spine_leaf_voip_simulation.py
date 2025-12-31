#!/usr/bin/env python3
"""
Mininet Spine-Leaf Simulation (ULTIMATE EDITION)
"""
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import psycopg2

DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class SpineLeafTopology:
    def __init__(self):
        self.net = None
        self.spines = []
        self.leaves = []
        self.hosts = []

    def create_database_table(self):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic.flow_stats_ (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                dpid VARCHAR(20) NOT NULL,
                src_ip VARCHAR(15) NOT NULL,
                dst_ip VARCHAR(15) NOT NULL,
                src_mac VARCHAR(17) NOT NULL,
                dst_mac VARCHAR(17) NOT NULL,
                ip_proto INTEGER NOT NULL,
                tp_src INTEGER NOT NULL,
                tp_dst INTEGER NOT NULL,
                bytes_tx BIGINT NOT NULL,
                bytes_rx BIGINT NOT NULL,
                pkts_tx INTEGER NOT NULL,
                pkts_rx INTEGER NOT NULL,
                duration_sec FLOAT NOT NULL,
                traffic_label VARCHAR(50) NOT NULL
            );
            """)
            conn.commit()
            conn.close()
            info("*** Database table verified.\n")
            return True
        except Exception as e:
            info(f"*** DB Connection Failed: {e}\n")
            info("*** Check VPN or IP Reachability!\n")
            return False

    def build_topology(self):
        self.net = Mininet(
            controller=RemoteController,
            switch=lambda name, **kwargs: OVSSwitch(name, stp=True, **kwargs),
            link=TCLink,
            autoSetMacs=True,
            autoStaticArp=True
        )
        
        info("*** Adding Controller (127.0.0.1:6653)\n")
        self.net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)

        info("*** Building Spine-Leaf\n")
        for i in range(1, 4): self.spines.append(self.net.addSwitch(f's{i}', dpid=f'000000000000000{i}'))
        for i in range(1, 4): self.leaves.append(self.net.addSwitch(f'l{i}', dpid=f'00000000000000{i+3}'))

        for spine in self.spines:
            for leaf in self.leaves:
                self.net.addLink(spine, leaf, bw=1000, delay='1ms')

        host_id = 1
        for i, leaf in enumerate(self.leaves):
            for j in range(2):
                h = self.net.addHost(f'h{host_id}', ip=f'10.0.{i+1}.{j+1}/24')
                self.hosts.append(h)
                self.net.addLink(h, leaf, bw=100, delay='1ms')
                host_id += 1

    def run(self):
        if not self.create_database_table(): return
        self.build_topology()
        self.net.start()
        
        # --- CRITICAL FIX: INCREASED WAIT TIME ---
        info("*** Waiting 35 seconds for STP Convergence (DO NOT SKIP)...\n")
        time.sleep(35) 
        
        info("*** Ping All (Testing Connectivity)...\n")
        loss = self.net.pingAll()
        
        if loss > 0:
            info("*** WARNING: Some pings failed. Retrying in 10s...\n")
            time.sleep(10)
            self.net.pingAll()

        info("*** Starting VoIP Traffic (D-ITG)...\n")
        for h in self.hosts: h.cmd('ITGRecv &')
        time.sleep(2)
        
        for i, src in enumerate(self.hosts):
            dst = self.hosts[(i + 1) % len(self.hosts)]
            # VoIP Codec simulation
            src.cmd(f'ITGSend -T UDP -a {dst.IP()} -c 160 -C 50 -t 3600000 &')
            info(f"    {src.name} -> {dst.name} (UDP Started)\n")

        info("*** Running... Press Ctrl+C to stop.\n")
        CLI(self.net)
        self.net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    SpineLeafTopology().run()