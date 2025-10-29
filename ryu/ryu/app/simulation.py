#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
from mininet.cli import CLI
import threading
import random
import time
from config import HOST_INFO

# ---------------------- TOPOLOGY DEFINISI ----------------------
class DataCenterTopo(Topo):
    def build(self, spine_count=2, leaf_count=3):
        spines = []
        leaves = []

        # --- Tambah Spine Switches ---
        for i in range(1, spine_count + 1):
            sp = self.addSwitch(f"sp{i}")
            spines.append(sp)

        # --- Tambah Leaf Switches ---
        for j in range(1, leaf_count + 1):
            lf = self.addSwitch(f"lf{j}")
            leaves.append(lf)

            # Connect setiap Leaf ke semua Spine
            for sp in spines:
                self.addLink(lf, sp, bw=10)

        # --- Tambah Hosts untuk tiap tier ---
        web1 = self.addHost("web1", ip="10.0.1.10/24", defaultRoute="via 10.0.1.254")
        app1 = self.addHost("app1", ip="10.0.2.10/24", defaultRoute="via 10.0.2.254")
        db1 = self.addHost("db1", ip="10.0.3.10/24", defaultRoute="via 10.0.3.254")

        # Assign host ke leaf berbeda
        self.addLink(web1, leaves[0], bw=5)
        self.addLink(app1, leaves[1], bw=5)
        self.addLink(db1, leaves[2], bw=5)

# ---------------------- TRAFFIC GENERATOR ----------------------
def simulate_traffic(net):
    """Generate periodic traffic antar host untuk simulasi inter-tier flow"""
    hosts = {
        "web1": net.get("web1"),
        "app1": net.get("app1"),
        "db1": net.get("db1")
    }

    def traffic_loop(src, dst, interval=5):
        while True:
            try:
                size = random.randint(50, 200)
                info(f"\n[*] {src.name} → {dst.name} sending {size}KB\n")
                src.cmd(f"iperf -c {dst.IP()} -u -b {size}K -t 3 > /dev/null 2>&1 &")
            except Exception as e:
                info(f"[!] Traffic error: {e}\n")
            time.sleep(interval)

    # web1 → app1 → db1 (simulasi request chain)
    threading.Thread(target=traffic_loop, args=(hosts["web1"], hosts["app1"], 5), daemon=True).start()
    threading.Thread(target=traffic_loop, args=(hosts["app1"], hosts["db1"], 7), daemon=True).start()

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")

    topo = DataCenterTopo()
    net = Mininet(topo=topo,
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)

    net.start()
    info("\n===== Data Center Topology Started =====\n")

    # Start traffic generator
    simulate_traffic(net)

    CLI(net)
    net.stop()
