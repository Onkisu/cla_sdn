#!/usr/bin/python3

"""
VOIP EXPERIMENT – FULL AUTOMATION
Mininet + Ryu + D-ITG + tcpdump
Output: /tmp/voip.pcap
"""

import os
import time
import subprocess
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.topo import Topo
from mininet.link import TCLink
from mininet.log import setLogLevel

PCAP_FILE = "/tmp/voip.pcap"
DURATION = 15  # seconds

# ================= TOPOLOGY =================
class SimpleTopo(Topo):
    def build(self):
        s1 = self.addSwitch("s1")
        s2 = self.addSwitch("s2")

        h1 = self.addHost("h1", ip="131.202.240.101/24")
        h2 = self.addHost("h2", ip="131.202.240.65/24")

        self.addLink(h1, s1, bw=100)
        self.addLink(s1, s2, bw=100)
        self.addLink(s2, h2, bw=100)

# ================= MAIN =================
def main():
    setLogLevel("info")

    if os.path.exists(PCAP_FILE):
        os.remove(PCAP_FILE)

    net = Mininet(
        topo=SimpleTopo(),
        controller=RemoteController,
        switch=OVSKernelSwitch,
        link=TCLink
    )

    net.start()
    h1, h2 = net.get("h1", "h2")

    print("[*] Starting ITGRecv on h2")
    h2.cmd("killall ITGRecv")
    h2.cmd("ITGRecv &")

    print("[*] Starting tcpdump")
    tcpdump = subprocess.Popen(
        ["tcpdump", "-i", "any", "udp", "-w", PCAP_FILE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    time.sleep(2)

    print("[*] Starting ITGSend")
    h1.cmd(
        "ITGSend -T UDP -a 131.202.240.65 "
        "-c 120 -C 100 -t 10000 > /dev/null 2>&1 &"
    )

    time.sleep(DURATION)

    print("[*] Stopping tcpdump")
    tcpdump.terminate()
    tcpdump.wait()

    net.stop()
    print("=== EXPERIMENT COMPLETE ===")

if __name__ == "__main__":
    main()
