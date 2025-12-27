#!/usr/bin/python3
"""
LEAF-SPINE 3x3 VOIP SIMULATION (SAFE VERSION)
Topology: TIDAK DIUBAH
Fix: RAM leak, OOM, sniff storm
"""

import threading
import time
import random
import os
import signal
import psycopg2
from datetime import datetime
import itertools
from collections import deque

from scapy.all import sniff, IP, UDP
from mininet.net import Mininet
from mininet.node import Controller, OVSKernelSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info

# ================= CONFIG =================
DB_CONFIG = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

TARGET_BPS_MIN = 100000
TARGET_BPS_MAX = 150000

stop_event = threading.Event()

# 🔴 FIX 1: QUEUE DIBATASI
pkt_queue = deque(maxlen=5000)
queue_lock = threading.Lock()

packet_counter = itertools.count(1)
start_time_ref = time.time()

# ================= TOPOLOGY (TIDAK DIUBAH) =================
class LeafSpineTopo(Topo):
    def build(self):
        info("*** Building Leaf-Spine 3x3\n")

        spines = [self.addSwitch(f'spine{i}') for i in range(1, 4)]
        leaves = [self.addSwitch(f'leaf{i}') for i in range(1, 4)]

        for leaf in leaves:
            for spine in spines:
                self.addLink(leaf, spine, bw=1000)

        user1 = self.addHost('user1', ip='10.0.0.1/24', mac='00:00:00:00:00:01')
        self.addLink(user1, leaves[0], bw=100)

        web1 = self.addHost('web1', ip='10.0.0.3/24', mac='00:00:00:00:00:03')
        self.addLink(web1, leaves[1], bw=100)

        user2 = self.addHost('user2', ip='10.0.0.2/24', mac='00:00:00:00:00:02')
        self.addLink(user2, leaves[2], bw=100)

# ================= PACKET PROCESS =================
def process_packet(pkt):
    if IP in pkt and UDP in pkt:
        src = pkt[IP].src
        dst = pkt[IP].dst

        if {src, dst} != {'10.0.0.1', '10.0.0.2'}:
            return

        with queue_lock:
            pkt_queue.append({
                "time": time.time() - start_time_ref,
                "source": src,
                "protocol": "UDP",
                "length": len(pkt),
                "Arrival Time": datetime.now(),
                "info": f"{pkt[UDP].sport} > {pkt[UDP].dport} Len={len(pkt)}",
                "No.": next(packet_counter),
                "destination": dst
            })

# ================= SNIFFER =================
def sniffer():
    info("*** Sniffing ONLY user1-eth0 (SAFE)\n")

    sniff(
        iface="user1-eth0",   # 🔴 FIX 2: BUKAN SEMUA LEAF
        filter="udp and host 10.0.0.1 and host 10.0.0.2",
        prn=process_packet,
        store=0,
        stop_filter=lambda x: stop_event.is_set()
    )

# ================= DB WRITER =================
def db_writer():
    conn = psycopg2.connect(DB_CONFIG)
    cur = conn.cursor()

    query = """INSERT INTO pcap_logs
               ("time","source","protocol","length","Arrival Time","info","No.","destination")
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

    while not stop_event.is_set():
        time.sleep(1)
        batch = []

        with queue_lock:
            while pkt_queue:
                batch.append(pkt_queue.popleft())

        if batch:
            cur.executemany(query, [
                (
                    r["time"], r["source"], r["protocol"], r["length"],
                    r["Arrival Time"], r["info"], r["No."], r["destination"]
                ) for r in batch
            ])
            conn.commit()

    conn.close()

# ================= TRAFFIC =================
def traffic(net):
    u1 = net.get('user1')
    u2 = net.get('user2')

    u2.cmd("ITGRecv &")
    time.sleep(2)

    while not stop_event.is_set():
        bps = random.randint(TARGET_BPS_MIN, TARGET_BPS_MAX)
        size = random.randint(100, 160)
        pps = max(1, int(bps / (size * 8)))

        u1.cmd(
            f"ITGSend -T UDP -a 10.0.0.2 -c {size} -C {pps} -t 2000 > /dev/null 2>&1"
        )
        time.sleep(0.5)

# ================= CLEANUP =================
def cleanup():
    os.system("sudo mn -c > /dev/null 2>&1")
    os.system("sudo killall -9 ITGSend ITGRecv python3 > /dev/null 2>&1")

def signal_handler(sig, frame):
    stop_event.set()

# ================= MAIN =================
if __name__ == "__main__":
    cleanup()
    setLogLevel("info")

    topo = LeafSpineTopo()
    net = Mininet(topo=topo, controller=Controller,
                  switch=OVSKernelSwitch, link=TCLink)

    signal.signal(signal.SIGINT, signal_handler)

    net.start()

    info("*** Enabling STP\n")
    for sw in net.switches:
        sw.cmd(f"ovs-vsctl set Bridge {sw.name} stp_enable=true")

    info("*** Waiting STP convergence\n")
    time.sleep(15)

    net.pingAll()

    threading.Thread(target=db_writer, daemon=True).start()
    threading.Thread(target=sniffer, daemon=True).start()

    try:
        traffic(net)
    finally:
        stop_event.set()
        net.stop()
        cleanup()
