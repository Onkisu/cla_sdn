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
import math

# ---------------------- GLOBAL LOCK ----------------------
cmd_lock = threading.Lock()
def safe_cmd(node, cmd):
    """Execute node.cmd() safely with lock."""
    with cmd_lock:
        return node.cmd(cmd)

# ---------------------- ROUTER & TOPOLOGY ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class ComplexTopo(Topo):
    def build(self):
        # Router
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # Switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        # Hosts
        # Subnet 1 (10.0.0.x) - High-Priority
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')

        # Subnet 2 (10.0.1.x) - Servers
        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')

        # Subnet 3 (10.0.2.x) - Low-Priority
        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Links hosts to switches
        # S1 (Clients)
        self.addLink(h1, s1, bw=5); self.addLink(h2, s1, bw=5); self.addLink(h3, s1, bw=5)
        # S2 (Servers) - High delay/loss to simulate a remote network
        self.addLink(h4, s2, bw=10, delay='32ms', loss=2); self.addLink(h5, s2, bw=10, delay='47ms', loss=2); self.addLink(h6, s2, bw=10)
        # S3 (Single Client) - Very restrictive link
        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)

        # Links router to switches (Inter-VLAN/Subnet routing via R1)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'}) 
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

# ---------------------- RANDOM TRAFFIC ----------------------
def generate_client_traffic(client, server_ip, port, min_bw, max_bw):
    """
    Generates random UDP traffic bursts using iperf and calculates a randomized 
    byte count estimate based on the target bandwidth.
    """
    info(f"Starting random traffic for {client.name} -> {server_ip}\n")
    
    while True:
        try:
            # 1. Randomly determine the target bandwidth (e.g., 0.5 Mbps to 4.0 Mbps)
            target_bw = random.uniform(min_bw, max_bw)
            bw_str = f"{target_bw:.2f}M"
            burst_time = 1 # iperf burst time is 1 second

            # 2. Execute iperf burst (actual traffic generation)
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time}"
            safe_cmd(client, cmd)

            # 3. Calculate a randomized byte count estimate
            # Expected bytes = (BW in bits/s / 8) * time
            # 1,000,000 converts Mbps to bits/s
            expected_bytes = (target_bw * 1000000 / 8) * burst_time
            
            # Add a random fluctuation (e.g., +/- 10%) for a randomized byte value
            fluctuation = random.uniform(0.9, 1.1)
            delta = int(expected_bytes * fluctuation)
            
            # 4. Display the randomized result
            # Use math.ceil to ensure non-zero byte count
            info(f"{client.name} *simulated* sending {math.ceil(delta):,} bytes in last burst (Target BW: {bw_str}ps)\n")

            # 5. Random pause 0.5–2s
            time.sleep(random.uniform(0.5, 2))

        except Exception as e:
            info(f"Error traffic for {client.name}: {e}\n")
            time.sleep(3)

def start_traffic(net):
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h6 = net.get('h4', 'h5', 'h6')
    h7 = net.get('h7')

    info("\n*** Starting iperf servers (Simulating services)\n")
    safe_cmd(h4, "iperf -s -u -p 443 &")   # Server H4: YouTube (Port 443)
    safe_cmd(h5, "iperf -s -u -p 443 &")   # Server H5: Netflix (Port 443)
    safe_cmd(h7, "iperf -s -u -p 1935 &")  # Server H7: Twitch (Port 1935)
    time.sleep(1)

    info("\n*** Starting client traffic threads (Simulating users)\n")
    threads = [
        # h1 -> h4 (YouTube): High bandwidth range
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.1.1', 443, 1.5, 5.0), daemon=True), 
        # h2 -> h5 (Netflix): Moderate bandwidth range
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.1.2', 443, 0.8, 3.5), daemon=True), 
        # h3 -> h7 (Twitch): Lower bandwidth range
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.2.1', 1935, 0.2, 1.8), daemon=True)
    ]
    for t in threads:
        t.start()

# ---------------------- FORECAST LOOP ----------------------
def run_forecast_loop():
    while True:
        info("\n*** Running AI Forecast...\n")
        try:
            # Assuming 'forecast.py' exists and uses the traffic data
            subprocess.call(["sudo", "python3", "forecast.py"])
        except Exception as e:
            info(f"*** Forecast error: {e}\n")
        time.sleep(900) # Wait 15 minutes

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    start_traffic(net)

    # t_forecast = threading.Thread(target=run_forecast_loop, daemon=True)
    # t_forecast.start()

    CLI(net)
    net.stop()
