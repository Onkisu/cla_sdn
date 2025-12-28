#!/usr/bin/python3
"""
MININET + RYU + D-ITG + TCPDUMP (AUTO)
OUTPUT: /tmp/voip.pcap
"""

from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.link import TCLink
from mininet.log import setLogLevel
import time
import os

PCAP = "/tmp/voip.pcap"

def run():
    net = Mininet(
        controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6653),
        link=TCLink,
        autoSetMacs=True
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

    # ================= TCPDUMP =================
    os.system(f"rm -f {PCAP}")
    tcpdump = h1.popen(f"tcpdump -i h1-eth0 udp -w {PCAP}")
    time.sleep(1)

    # ================= D-ITG ===================
    print("[*] Starting ITGRecv on h2")
    h2.cmd("ITGRecv &")
    time.sleep(1)

    print("[*] Starting ITGSend")
    h1.cmd("ITGSend -a %s -T UDP -C 100 -c 120 -t 10000" % h2.IP())

    time.sleep(2)

    tcpdump.terminate()
    net.stop()

    print("=== EXPERIMENT COMPLETE ===")

if __name__ == "__main__":
    setLogLevel("info")
    run()
