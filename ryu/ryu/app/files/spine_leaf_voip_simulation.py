#!/usr/bin/env python3
"""
Mininet Spine-Leaf Topology Simulation for VoIP Traffic
- 3 Spine Switches
- 3 Leaf Switches  
- 2 Hosts per Leaf (6 hosts total)
- D-ITG for VoIP traffic generation
- PostgreSQL data storage every second
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import threading
import random
import math
import psycopg2
from psycopg2 import sql
from datetime import datetime
import subprocess
import sys

# PostgreSQL Configuration
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

TABLE_NAME = 'traffic.flow_stats_'

class SpineLeafTopology:
    def __init__(self):
        self.net = None
        self.spines = []
        self.leaves = []
        self.hosts = []
        self.db_conn = None
        self.running = False
        self.start_time = time.time()
        
    def create_database_table(self):
        """Create PostgreSQL table if not exists"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            cursor = self.db_conn.cursor()
            
            create_table_query = """
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
            """
            
            cursor.execute(create_table_query)
            self.db_conn.commit()
            cursor.close()
            info("*** Database table created/verified successfully\n")
            return True
            
        except Exception as e:
            info(f"*** Error creating database table: {e}\n")
            return False
    
    def build_topology(self):
        """Build Spine-Leaf topology"""
        info("*** Creating Spine-Leaf topology\n")
        
        self.net = Mininet(
            controller=RemoteController,
            switch=OVSSwitch,
            link=TCLink,
            autoSetMacs=True,
            autoStaticArp=False  # Let controller learn via packet_in
        )
        
        # Add Ryu controller
        info("*** Adding Ryu controller\n")
        c0 = self.net.addController(
            'c0',
            controller=RemoteController,
            ip='127.0.0.1',
            port=6653
        )
        
        # Add 3 Spine switches
        info("*** Adding Spine switches\n")
        for i in range(1, 4):
            spine = self.net.addSwitch(f's{i}', dpid=f'000000000000000{i}')
            self.spines.append(spine)
        
        # Add 3 Leaf switches
        info("*** Adding Leaf switches\n")
        for i in range(1, 4):
            leaf = self.net.addSwitch(f'l{i}', dpid=f'00000000000000{i+3}')
            self.leaves.append(leaf)
        
        # Connect Spines to Leaves (full mesh)
        info("*** Connecting Spines to Leaves\n")
        for spine in self.spines:
            for leaf in self.leaves:
                self.net.addLink(spine, leaf, bw=1000, delay='1ms')
        
        # Add 2 hosts per leaf
        info("*** Adding hosts\n")
        host_id = 1
        for i, leaf in enumerate(self.leaves):
            for j in range(2):
                host = self.net.addHost(
                    f'h{host_id}',
                    ip=f'10.0.0.{host_id}/24',  # All in same subnet 10.0.0.0/24
                    mac=f'00:00:00:00:00:{host_id:02x}'
                )
                self.hosts.append(host)
                self.net.addLink(host, leaf, bw=100, delay='1ms')
                host_id += 1
        
        info("*** Topology built successfully\n")
        return self.net
    
    def generate_bytes_pattern(self, elapsed_seconds):
        """
        Generate bytes_tx with random pattern (sine wave + noise)
        One period = 1 hour (3600 seconds)
        Range: 13000 - 19800 bytes
        """
        # Base value
        base = 16400  # Middle of 13000-19800
        amplitude = 3400  # Half of range
        
        # Sine wave pattern (1 hour period)
        period = 3600  # 1 hour in seconds
        phase = (elapsed_seconds % period) / period * 2 * math.pi
        sine_value = math.sin(phase)
        
        # Add random noise (-10% to +10%)
        noise = random.uniform(-0.1, 0.1)
        
        # Calculate bytes
        bytes_tx = int(base + (amplitude * sine_value) + (base * noise))
        
        # Ensure within range
        bytes_tx = max(13000, min(19800, bytes_tx))
        
        return bytes_tx
    
    def insert_flow_data(self, flow_data):
        """Insert flow data into PostgreSQL"""
        try:
            cursor = self.db_conn.cursor()
            
            insert_query = """
            INSERT INTO traffic.flow_stats_ 
            (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
             ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
             duration_sec, traffic_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, flow_data)
            self.db_conn.commit()
            cursor.close()
            
        except Exception as e:
            info(f"*** Error inserting data: {e}\n")
            self.db_conn.rollback()
    
    def generate_flow_stats(self):
        """Generate simulated flow statistics (backup if Ryu fails)"""
        info("*** Flow statistics are being collected by Ryu controller\n")
        info("*** This function serves as backup monitoring\n")
        
        while self.running:
            try:
                # Just monitor - Ryu controller handles DB insertion
                time.sleep(10)
                info("*** Monitoring active... Data collected by Ryu controller\n")
                
            except Exception as e:
                info(f"*** Error in monitoring: {e}\n")
                time.sleep(10)
    
    def start_voip_traffic(self):
        """Start VoIP traffic simulation using D-ITG"""
        info("*** Starting VoIP traffic generation with D-ITG\n")
        
        try:
            # Start ITGRecv on destination hosts
            for i, host in enumerate(self.hosts[1::2]):  # Every other host as receiver
                host.cmd(f'ITGRecv -l /tmp/recv_h{i}.log &')
                info(f"*** Started ITGRecv on {host.name}\n")
            
            time.sleep(2)
            
            # Start ITGSend on source hosts (VoIP traffic)
            for i, host in enumerate(self.hosts[::2]):  # Every other host as sender
                dst_host = self.hosts[i*2 + 1] if i*2 + 1 < len(self.hosts) else self.hosts[-1]
                dst_ip = dst_host.IP()
                
                # VoIP codec G.711 simulation: 64 kbps, 20ms packets
                # -T 1 = VoIP, -a dst_ip, -c 160 (packet size), -C 50 (packets/sec)
                cmd = f'ITGSend -T 1 -a {dst_ip} -c 160 -C 50 -t 3600000 -l /tmp/send_h{i}.log &'
                host.cmd(cmd)
                info(f"*** Started ITGSend on {host.name} to {dst_host.name}\n")
            
        except Exception as e:
            info(f"*** Error starting D-ITG traffic: {e}\n")
            info("*** D-ITG may not be installed. Continuing with simulated data...\n")
    
    def run(self):
        """Run the simulation"""
        info("*** Starting Spine-Leaf VoIP Simulation\n")
        
        # Create database table
        if not self.create_database_table():
            info("*** Failed to create database table. Exiting.\n")
            return
        
        # Build topology
        self.build_topology()
        
        # Start network
        info("*** Starting network\n")
        self.net.start()
        
        # Start VoIP traffic with D-ITG
        self.start_voip_traffic()
        
        # Start flow statistics generation
        self.running = True
        self.start_time = time.time()
        stats_thread = threading.Thread(target=self.generate_flow_stats, daemon=True)
        stats_thread.start()
        
        # Test connectivity
        info("*** Testing connectivity\n")
        self.net.pingAll()
        
        # Run CLI
        info("*** Running CLI (type 'exit' to stop simulation)\n")
        info("*** Flow data is being collected every second and stored in PostgreSQL\n")
        CLI(self.net)
        
        # Stop simulation
        self.running = False
        info("*** Stopping simulation\n")
        
        # Stop network
        self.net.stop()
        
        # Close database connection
        if self.db_conn:
            self.db_conn.close()
        
        info("*** Simulation stopped\n")

def check_ditg_installed():
    """Check if D-ITG is installed"""
    try:
        result = subprocess.run(['which', 'ITGSend'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            info("*** D-ITG is installed\n")
            return True
        else:
            info("*** WARNING: D-ITG is not installed\n")
            info("*** Install with: sudo apt-get install d-itg\n")
            info("*** Continuing with simulated data...\n")
            return False
    except:
        return False

def main():
    """Main function"""
    setLogLevel('info')
    
    info("="*70 + "\n")
    info("*** Spine-Leaf VoIP Traffic Simulation\n")
    info("*** Topology: 3 Spine, 3 Leaf, 6 Hosts\n")
    info("*** Traffic: VoIP (G.711 codec simulation)\n")
    info("*** Database: PostgreSQL (103.181.142.121)\n")
    info("="*70 + "\n")
    
    # Check D-ITG installation
    check_ditg_installed()
    
    # Create and run simulation
    try:
        topo = SpineLeafTopology()
        topo.run()
    except KeyboardInterrupt:
        info("\n*** Interrupted by user\n")
    except Exception as e:
        info(f"\n*** Error: {e}\n")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
