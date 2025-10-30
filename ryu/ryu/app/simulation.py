#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import threading
import random
import time
import subprocess
import os
import signal

# ======================================================
#   CLEANUP MININET DAN OVS
# ======================================================
def cleanup():
    info("*** Cleaning up Mininet & OVS...\n")
    subprocess.run(["sudo", "mn", "-c"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "pkill", "-f", "controller"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "pkill", "-f", "ryu-manager"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "ovs-vsctl", "--all", "destroy", "bridge"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "ovs-vsctl", "--all", "destroy", "controller"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "ovs-vsctl", "del-br", "s1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "pkill", "-9", "iperf"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "ip", "-all", "netns", "delete"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# ======================================================
#   THREAD LOCK DAN EVENT
# ======================================================
cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd):
    """Thread-safe command execution"""
    if stop_event.is_set():
        return None
    with cmd_lock:
        if stop_event.is_set():
            return None
        try:
            return node.cmd(cmd, timeout=10)
        except Exception:
            return None

# ======================================================
#   FATTREE TOPOLOGY
# ======================================================
class FatTreeTopo(Topo):
    def build(self, k=4):
        info(f"*** Building Fat-Tree Topology (k={k})...\n")

        num_pods = k
        num_cores = (k // 2) ** 2
        num_aggs_per_pod = k // 2
        num_edges_per_pod = k // 2
        num_hosts_per_edge = k // 2

        cores = []
        for i in range(num_cores):
            cores.append(self.addSwitch(f'c{i+1}'))

        aggs = []
        edges = []
        for p in range(num_pods):
            aggs_pod = []
            for a in range(num_aggs_per_pod):
                aggs_pod.append(self.addSwitch(f'a{p+1}_{a+1}'))
            aggs.append(aggs_pod)

            edges_pod = []
            for e in range(num_edges_per_pod):
                edges_pod.append(self.addSwitch(f'e{p+1}_{e+1}'))
            edges.append(edges_pod)

        for i in range(num_cores):
            core_sw = cores[i]
            agg_index = i // (k // 2)
            for p in range(num_pods):
                self.addLink(core_sw, aggs[p][agg_index], bw=20)

        for p in range(num_pods):
            for a in range(num_aggs_per_pod):
                for e in range(num_edges_per_pod):
                    self.addLink(aggs[p][a], edges[p][e], bw=10)

        ip_count = 1
        for p in range(num_pods):
            for e in range(num_edges_per_pod):
                edge = edges[p][e]
                for _ in range(num_hosts_per_edge):
                    h = self.addHost(f'h{ip_count}', ip=f'10.0.0.{ip_count}/24',
                                     mac=f'00:00:00:00:00:{ip_count:02x}')
                    self.addLink(edge, h, bw=5)
                    ip_count += 1

        info(f"  > Total Hosts: {ip_count - 1}\n")

# ======================================================
#   TRAFFIC FUNCTION
# ======================================================
def _log_iperf(client_name, server_ip, output, dur, bw):
    if not output:
        return
    try:
        lines = output.strip().split("\n")
        for line in reversed(lines):
            if ',' in line and len(line.split(',')) > 7:
                sent = int(line.split(',')[7])
                info(f"[LOG] {client_name} -> {server_ip} sent {sent:,} bytes in {dur}s @ {bw}\n")
                return
    except Exception:
        pass

def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed):
    rng = random.Random(seed)
    while not stop_event.is_set():
        bw = f"{rng.uniform(base_min_bw, base_max_bw):.2f}M"
        dur = f"{rng.uniform(0.5, 2.5):.1f}"
        cmd = f"iperf -u -c {server_ip} -p {port} -b {bw} -t {dur} -y C"
        out = safe_cmd(client, cmd)
        _log_iperf(client.name, server_ip, out, dur, bw)
        stop_event.wait(rng.uniform(0.5, 2.0))

# ======================================================
#   TRAFFIC STARTER
# ======================================================
def start_traffic(net):
    info("\n*** Attaching namespaces...\n")
    subprocess.run(["sudo", "mkdir", "-p", "/var/run/netns"])
    for i in range(1, 17):
        h = net.get(f'h{i}')
        if h:
            subprocess.run(["sudo", "ip", "netns", "attach", h.name, str(h.pid)], stderr=subprocess.DEVNULL)

    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h7 = net.get('h4', 'h5', 'h7')

    safe_cmd(h4, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h5, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h7, "iperf -s -u -p 1935 -i 1 &")
    time.sleep(1)

    net.waitConnected()
    net.pingAll()

    info("\n*** Starting traffic threads...\n")
    threads = [
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.0.4', 443, 0.5, 5.0, 11)),
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.0.5', 443, 0.5, 5.0, 22)),
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.0.7', 1935, 0.5, 5.0, 33))
    ]
    for t in threads:
        t.start()
    return threads

# ======================================================
#   MAIN
# ======================================================
if __name__ == "__main__":
    setLogLevel("info")
    cleanup()
    info("\n*** Starting Mininet with Fat-Tree topology...\n")

    net = Mininet(
        topo=FatTreeTopo(k=4),
        switch=OVSSwitch,
        controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
        link=TCLink,
        autoSetMacs=True
    )

    net.start()
    threads = start_traffic(net)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        info("\n*** Stopping simulation...\n")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=3)
        for i in range(1, 17):
            try:
                subprocess.run(["sudo", "ip", "netns", "del", f"h{i}"], stderr=subprocess.DEVNULL)
            except Exception:
                pass
        net.stop()
        cleanup()
        info("*** Simulation stopped cleanly.\n")
