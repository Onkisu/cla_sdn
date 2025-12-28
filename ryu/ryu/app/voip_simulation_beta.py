#!/usr/bin/python3
"""
REAL TRAFFIC SIMULATION (STABLE VERSION)
Fix: Menggunakan native Mininet command untuk baca counters, 
bukan manual namespace yang sering 'Device busy'.
"""

import threading
import time
import os
import psycopg2
from datetime import datetime

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= DATABASE CONFIG =================
DB_CONFIG = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# Mapping: Nama Host -> Info Database
HOST_MAP = {
    'user1': {'ip': '192.168.100.1', 'dst': '10.10.1.1', 'mac': '00:00:00:00:01:01'},
    'user2': {'ip': '192.168.100.2', 'dst': '10.10.2.1', 'mac': '00:00:00:00:01:02'},
    'user3': {'ip': '192.168.100.3', 'dst': '10.10.1.2', 'mac': '00:00:00:00:01:03'},
    'web1':  {'ip': '10.10.1.1',     'dst': '192.168.100.1', 'mac': '00:00:00:00:0A:01'},
    'app1':  {'ip': '10.10.2.1',     'dst': '10.10.2.2',     'mac': '00:00:00:00:0B:01'},
}

stop_event = threading.Event()

# ================= TOPOLOGY =================
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')
    def terminate(self):
        self.cmd('sysctl -w net.ipv4.ip_forward=0')
        super(LinuxRouter, self).terminate()

class SimpleTopo(Topo):
    def build(self):
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        sw1 = self.addSwitch('sw1')
        sw2 = self.addSwitch('sw2')

        # User Network
        h1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        h2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        h3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        self.addLink(h1, sw1); self.addLink(h2, sw1); self.addLink(h3, sw1)
        self.addLink(sw1, cr1, intfName2='cr1-eth1', params2={'ip': '192.168.100.254/24'})

        # Server Network
        w1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        a1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        d1 = self.addHost('db1',  ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')
        self.addLink(w1, sw2); self.addLink(a1, sw2); self.addLink(d1, sw2)
        self.addLink(sw2, cr1, intfName2='cr1-eth2', params2={'ip': '10.10.1.254/24'})

# ================= REAL DATA COLLECTOR (FIXED) =================
def get_node_bytes(net, node_name):
    """
    Menggunakan API Mininet langsung, lebih stabil daripada ip netns exec
    """
    try:
        node = net.get(node_name)
        # Baca file bytes langsung dari dalam node
        output = node.cmd(f"cat /sys/class/net/{node_name}-eth0/statistics/tx_bytes")
        return int(output.strip())
    except Exception as e:
        # print(f"Read Error {node_name}: {e}")
        return 0

def monitor_thread(net):
    info("*** [Monitor] Starting Real-Time Interface Polling...\n")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONFIG)
    except Exception as e:
        info(f"!!! DB CONNECT ERROR: {e}\n")
        return

    # Initial State
    last_stats = {h: get_node_bytes(net, h) for h in HOST_MAP.keys()}
    
    while not stop_event.is_set():
        time.sleep(1.0)
        rows = []
        now = datetime.now()
        
        total_delta_batch = 0 # Untuk debug log

        for h_name, meta in HOST_MAP.items():
            current_total = get_node_bytes(net, h_name)
            
            # Hitung selisih
            delta_bytes = current_total - last_stats[h_name]
            
            # Update last state (HANYA jika current > last, antisipasi reset counter)
            if current_total >= last_stats[h_name]:
                last_stats[h_name] = current_total
            else:
                last_stats[h_name] = current_total # Reset detected
                delta_bytes = 0 

            total_delta_batch += delta_bytes

            # Insert jika ada traffic (filter noise kecil < 100 bytes)
            if delta_bytes > 100:
                pkts = int(delta_bytes / 180) 
                rows.append((now, 0, meta['ip'], meta['dst'], meta['mac'], 'FF:FF:FF:FF:FF:FF',
                             17, 5060, 5060, delta_bytes, 0, pkts, 0, 1.0, 'data'))

        # Debug di Terminal biar kamu tenang
        if total_delta_batch > 0:
            print(f"   >>> Inserting Data... Total Bytes this second: {total_delta_batch}")
        else:
            print(f"   ... Waiting for traffic ... (Current Read: 0)")

        if rows:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                       ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                       duration_sec , traffic_label) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
            except Exception as e:
                print(f"!!! DB INSERT ERROR: {e}")
                conn.rollback()
    
    if conn: conn.close()

# ================= TRAFFIC GEN =================
def traffic_thread(net):
    info("*** [Traffic] Generator Running (-C 95 -c 160)...\n")
    # Setup Receivers
    for h in net.hosts:
        h.cmd("killall ITGRecv; ITGRecv &")
    time.sleep(2)

    while not stop_event.is_set():
        for h_name, meta in HOST_MAP.items():
            sender = net.get(h_name)
            # -C 95 packets/sec, -c 160 bytes payload
            cmd = f"ITGSend -T UDP -a {meta['dst']} -c 160 -C 95 -t 3000 > /dev/null 2>&1 &"
            sender.cmd(cmd)
        time.sleep(2.8)

# ================= MAIN =================
def cleanup():
    os.system("sudo killall -9 ITGSend ITGRecv > /dev/null 2>&1")

if __name__ == "__main__":
    setLogLevel('info')
    cleanup()
    
    topo = SimpleTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    try:
        net.start()
        net['cr1'].cmd('ip addr add 10.10.2.254/24 dev cr1-eth2')
        
        info("*** Ping Check...\n")
        net.pingAll()
        
        # Pass 'net' object to monitor thread
        t_mon = threading.Thread(target=monitor_thread, args=(net,))
        t_gen = threading.Thread(target=traffic_thread, args=(net,))
        
        t_mon.start()
        t_gen.start()
        
        info("*** SIMULATION RUNNING.\n")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        info("\n*** Stopping...\n")
        stop_event.set()
        net.stop()
        cleanup()