#!/usr/bin/python3
"""
MININET + RYU + D-ITG + TCPDUMP (FULL AUTO – FINAL)
OUTPUT: /tmp/voip.pcap
CAPTURE: UDP VoIP traffic
"""

from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.link import TCLink
from mininet.log import setLogLevel
import time
import os
import signal

PCAP = "/tmp/voip.pcap"

def run():
    setLogLevel("info")

    # ================= CREATE NET =================
    net = Mininet(
        controller=lambda name: RemoteController(
            name, ip="127.0.0.1", port=6653
        ),
        link=TCLink,
        autoSetMacs=True,
        autoStaticArp=True
    )

    print("*** Creating network")
    h1 = net.addHost("h1")
    h2 = net.addHost("h2")
    s1 = net.addSwitch("s1")
    s2 = net.addSwitch("s2")

    net.addLink(h1, s1, bw=100)
    net.addLink(s1, s2, bw=100)
    net.addLink(s2, h2, bw=100)

    net.start()
    time.sleep(2)

    # ================= CLEAN OLD FILE =================
    os.system(f"rm -f {PCAP}")

    # ================= START TCPDUMP =================
    print("[*] Starting tcpdump on h1-eth0")
    tcpdump = h1.popen(
        f"tcpdump -i h1-eth0 udp -w {PCAP}",
        stdout=open(os.devnull, "w"),
        stderr=open(os.devnull, "w")
    )

    time.sleep(2)

    # ================= START ITGRecv =================
    print("[*] Starting ITGRecv on h2")
    h2.cmd("pkill ITGRecv")
    h2.cmd("ITGRecv &")
    time.sleep(2)

    # ================= START ITGSend =================
    print("[*] Starting ITGSend from h1 to h2")
    h1.cmd(
        f"ITGSend -a {h2.IP()} -T UDP -C 100 -c 120 -t 10000"
    )

    # ================= WAIT FULL TRAFFIC =================
    print("[*] Waiting traffic to finish...")
    time.sleep(12)  # HARUS >= -t ITGSend

    # ================= STOP TCPDUMP =================
    print("[*] Stopping tcpdump")
    tcpdump.send_signal(signal.SIGINT)
    tcpdump.wait()

    # ================= STOP NET =================
    net.stop()
    print("=== EXPERIMENT COMPLETE ===")

    # ================= VERIFY =================
    if os.path.exists(PCAP):
        size = os.path.getsize(PCAP)
        print(f"[✓] PCAP SIZE: {size} bytes")
    else:
        print("[X] PCAP FILE NOT FOUND")

if __name__ == "__main__":
    run()
