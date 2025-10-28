#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import time
import subprocess
import threading
import random
import math
from datetime import datetime

# ---------------------- GLOBAL LOCK ----------------------
cmd_lock = threading.Lock()

def safe_cmd(node, cmd):
    """Execute node.cmd() safely with lock to avoid Mininet poll() conflict."""
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
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')

        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')

        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Links hosts to switches
        self.addLink(h1, s1, bw=5); self.addLink(h2, s1, bw=5); self.addLink(h3, s1, bw=5)
        self.addLink(h4, s2, bw=10, delay='32ms', loss=2); self.addLink(h5, s2, bw=10, delay='47ms', loss=2); self.addLink(h6, s2, bw=10)
        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)

        # Links router to switches (gateway per subnet)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'}) 
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

# ---------------------- DYNAMIC TRAFFIC ----------------------
def generate_client_traffic(client, server_ip, port, base_bw_off_peak, base_bw_peak):
    info(f"Starting traffic for {client.name} -> {server_ip}\n")
    while True:
        try:
            now = time.time()  # seconds since epoch
            hour = datetime.now().hour

            # Peak/off-peak base bandwidth
            base_bw = base_bw_peak if 18 <= hour < 24 else base_bw_off_peak

            # Smooth sine-based variation with real time
            sine_variation = 0.2 * math.sin(now / 60 + random.uniform(0, 2*math.pi))

            # Fast random jitter
            fast_random = random.uniform(-0.1, 0.1)

            # Occasional spike (10% chance)
            spike = random.choices([0, random.uniform(0.5, 1.5)], weights=[0.9,0.1])[0]

            target_bw = base_bw * (1 + sine_variation + fast_random) + spike
            bw_str = f"{target_bw:.2f}M"

            duration = 10
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {duration}"
            safe_cmd(client, cmd)

            # Random pause 1-5s
            time.sleep(random.uniform(1,5))

        except Exception as e:
            info(f"Error traffic for {client.name}: {e}\n")
            time.sleep(10)

def start_traffic(net):
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h6 = net.get('h4', 'h5', 'h6')
    h7 = net.get('h7')

    info("*** Starting iperf servers\n")
    safe_cmd(h4, "iperf -s -u -p 443 &")   # YouTube server
    safe_cmd(h5, "iperf -s -u -p 443 &")   # Netflix server
    safe_cmd(h7, "iperf -s -u -p 1935 &")  # Twitch server

    time.sleep(1)  # Give servers time to start

    info("*** Starting dynamic iperf clients\n")
    threads = [
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.1.1', 443, 1.5, 4.0), daemon=True),
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.1.2', 443, 1.0, 2.5), daemon=True),
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.2.1', 1935, 0.5, 1.5), daemon=True)
    ]
    for t in threads:
        t.start()

# ---------------------- FORECAST LOOP ----------------------
def run_forecast_loop():
    while True:
        info("\n*** Running AI Forecast...\n")
        try:
            subprocess.call(["sudo", "python3", "forecast.py"])
        except Exception as e:
            info(f"*** Forecast error: {e}\n")
        time.sleep(900)  # 15 minutes

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    # Start traffic
    start_traffic(net)

    # Start forecast loop
    t_forecast = threading.Thread(target=run_forecast_loop, daemon=True)
    t_forecast.start()

    # Start CLI safely while traffic threads run
    CLI(net)
    net.stop()
