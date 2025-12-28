#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI
import subprocess, time, os, signal

from scapy.all import rdpcap, IP, UDP
import psycopg2
from datetime import datetime

PCAP = "/tmp/voip.pcap"

# =====================================================
# 1. START TCPDUMP
# =====================================================
def start_tcpdump():
    if os.path.exists(PCAP):
        os.remove(PCAP)
    return subprocess.Popen(
        ["tcpdump", "-i", "any", "-s", "0", "-w", PCAP],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid
    )

# =====================================================
# 2. PARSE PCAP → POSTGRES
# =====================================================
def parse_to_psql():
    conn = psycopg2.connect(
        dbname="development",
        user="dev_one",
        password="hijack332.",
        host="localhost"
    )
    cur = conn.cursor()

    pkts = rdpcap(PCAP)
    base_time = pkts[0].time
    no = 1

    for p in pkts:
        if IP in p and UDP in p:
            ts = float(p.time - base_time)
            src = p[IP].src
            dst = p[IP].dst
            length = len(p)
            arrival = datetime.fromtimestamp(p.time)

            sport = p[UDP].sport
            dport = p[UDP].dport
            payload_len = len(p[UDP].payload)

            info = f"{sport}  >  {dport} Len={payload_len}"

            cur.execute("""
                INSERT INTO pcap_logs
                ("time", source, protocol, length,
                 "Arrival Time", info, "No.", destination)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                ts, src, "UDP", length,
                arrival, info, no, dst
            ))
            no += 1

    conn.commit()
    cur.close()
    conn.close()

# =====================================================
# 3. MININET + D-ITG
# =====================================================
def run():
    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True
    )

    net.addController("c0", ip="127.0.0.1", port=6633)

    leaves = [net.addSwitch(f"leaf{i}") for i in range(1,4)]
    spines = [net.addSwitch(f"spine{i}") for i in range(1,4)]

    h1 = net.addHost("h1", ip="131.202.240.101/24")
    h2 = net.addHost("h2", ip="131.202.240.65/24")

    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=1000)

    net.addLink(h1, leaves[0], bw=100)
    net.addLink(h2, leaves[2], bw=100)

    net.start()

    # --- tcpdump ---
    tcpdump = start_tcpdump()
    time.sleep(2)

    # --- D-ITG ---
    h2.cmd("pkill ITGRecv; ITGRecv &")
    time.sleep(2)

    h1.cmd(
        "ITGSend -T UDP "
        "-a 131.202.240.65 "
        "-rp 4000 "
        "-C 50 -c 160 "
        "-t 15000 &"
    )

    time.sleep(15)

    os.killpg(os.getpgid(tcpdump.pid), signal.SIGTERM)
    net.stop()

    parse_to_psql()
    print("=== EXPERIMENT COMPLETE ===")

# =====================================================
if __name__ == "__main__":
    run()
