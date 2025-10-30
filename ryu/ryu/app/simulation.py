#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
import threading, random, time, subprocess

# ---------------------- GLOBAL LOCK & STOP EVENT ----------------------
cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd):
    with cmd_lock:
        if stop_event.is_set():
            return None
        try:
            return node.cmd(cmd)
        except Exception:
            return None

# ---------------------- ROUTER NODE ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super().config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super().terminate()

# ---------------------- FAT-TREE + ROUTER TOPO ----------------------
class FatTreeRouterTopo(Topo):
    def build(self):
        info("*** Building FatTree with router core\n")

        # Router (gateway)
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.1.254/24')

        # Switches (edge)
        s1 = self.addSwitch('s1', dpid='0000000000000101', protocols='OpenFlow13')
        s2 = self.addSwitch('s2', dpid='0000000000000102', protocols='OpenFlow13')

        # Connect router <-> switches
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip':'10.0.1.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip':'10.0.2.254/24'})

        # Hosts in subnet 1
        for i in range(1, 4):
            h = self.addHost(f'h1{i}', ip=f'10.0.1.{i}/24', defaultRoute='via 10.0.1.254')
            self.addLink(h, s1, cls=TCLink, bw=10)

        # Hosts in subnet 2
        for i in range(1, 4):
            h = self.addHost(f'h2{i}', ip=f'10.0.2.{i}/24', defaultRoute='via 10.0.2.254')
            self.addLink(h, s2, cls=TCLink, bw=10)

# ---------------------- TRAFFIC GENERATOR ----------------------
def generate_client_traffic(client, server_ip, port, seed):
    rng = random.Random(seed)
    info(f"Starting traffic thread for {client.name} -> {server_ip}\n")
    while not stop_event.is_set():
        bw = rng.uniform(1.0, 5.0)
        dur = rng.uniform(1.0, 3.0)
        cmd = f"iperf -u -c {server_ip} -p {port} -b {bw:.1f}M -t {dur:.1f} -y C"
        output = safe_cmd(client, cmd)
        if output:
            info(f"[LOG] {client.name} -> {server_ip}: {bw:.1f} Mbps for {dur:.1f}s\n")
        stop_event.wait(rng.uniform(1.0, 2.0))

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    topo = FatTreeRouterTopo()
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip='127.0.0.1', port=6633))

    info("*** Starting network...\n")
    net.start()
    net.waitConnected()

    info("*** Pinging to warm up...\n")
    net.pingAll()

    # Start iperf servers
    h21 = net.get('h21')
    h21.cmd("iperf -s -u -p 5001 -i 1 &")
    h22 = net.get('h22')
    h22.cmd("iperf -s -u -p 5002 -i 1 &")

    # Start traffic
    clients = [net.get('h11'), net.get('h12'), net.get('h13')]
    servers = [('10.0.2.1', 5001), ('10.0.2.2', 5002)]
    threads = []
    for i, client in enumerate(clients):
        ip, port = servers[i % len(servers)]
        t = threading.Thread(target=generate_client_traffic, args=(client, ip, port, 1000+i), daemon=True)
        t.start()
        threads.append(t)

    info("*** Traffic started. Press Ctrl+C to stop.\n")
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        info("\n*** Ctrl+C detected. Stopping...\n")
        stop_event.set()
        for h in net.hosts:
            h.cmd("killall -9 iperf")
        net.stop()
