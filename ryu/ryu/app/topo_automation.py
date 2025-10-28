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

# ---------------------- GLOBAL LOCK & STOP EVENT ----------------------
cmd_lock = threading.Lock()
stop_event = threading.Event() 

def safe_cmd(node, cmd):
    """Execute node.cmd() safely with lock."""
    if stop_event.is_set():
        return None # [FIX] Kembalikan None kalo udah distop
    with cmd_lock:
        return node.cmd(cmd) # Kembalikan output dari command

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

        # Links
        self.addLink(h1, s1, bw=5); self.addLink(h2, s1, bw=5); self.addLink(h3, s1, bw=5)
        self.addLink(h4, s2, bw=10, delay='32ms', loss=2); self.addLink(h5, s2, bw=10, delay='47ms', loss=2); self.addLink(h6, s2, bw=10)
        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'}) 
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

# ---------------------- RANDOM TRAFFIC ----------------------
def generate_client_traffic(client, server_ip, port, min_bw, max_bw):
    """
    Generates random UDP traffic bursts using iperf and logs the *actual*
    byte count reported by iperf.
    """
    info(f"Starting random traffic for {client.name} -> {server_ip}\n")
    
    while not stop_event.is_set():
        try:
            # 1. Randomly determine the target bandwidth
            target_bw = random.uniform(min_bw, max_bw)
            bw_str = f"{target_bw:.2f}M"
            
            # Durasi burst time diacak antara 0.5 detik - 2.5 detik
            burst_time = random.uniform(0.5, 2.5) 
            burst_time_str = f"{burst_time:.1f}"

            # 2. Execute iperf burst (actual traffic generation)
            # [LOGGING FIX] Tambah '-y C' untuk output CSV (gampang diparsing)
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            
            # [LOGGING FIX] Tangkap outputnya
            output = safe_cmd(client, cmd)

            # Kalo outputnya None, artinya stop_event nyala, langsung keluar
            if not output:
                continue

            # 3. [LOGGING FIX] Parsing output asli iperf
            try:
                # Ambil baris terakhir dari output, itu baris CSV-nya
                csv_line = output.strip().split('\n')[-1]
                parts = csv_line.split(',')
                
                # Di format CSV, kolom ke-8 (index 7) adalah total bytes
                actual_bytes = int(parts[7])
                
                # 4. [LOGGING FIX] Cetak log ASLI, bukan simulasi
                info(f"iperf LOG: {client.name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time:.1f}s (Target BW: {bw_str}ps)\n")

            except Exception as e:
                # Kalo parsing gagal (jarang terjadi), cetak error aja
                info(f"Could not parse iperf output for {client.name}: {e}\nOutput was: {output}\n")


            # 5. Random pause 0.5–2s
            stop_event.wait(random.uniform(0.5, 2))

        except Exception as e:
            if stop_event.is_set():
                break
            info(f"Error traffic for {client.name}: {e}\n")
            stop_event.wait(3)

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
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.1.1', 443, 1.5, 5.0), daemon=True), 
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.1.2', 443, 0.8, 3.5), daemon=True), 
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.2.1', 1935, 0.2, 1.8), daemon=True)
    ]
    for t in threads:
        t.start()
    return threads 

# ---------------------- FORECAST LOOP ----------------------
def run_forecast_loop():
    while not stop_event.is_set():
        info("\n*** Running AI Forecast...\n")
        try:
            subprocess.call(["sudo", "python3", "forecast.py"])
        except Exception as e:
            if stop_event.is_set():
                break
            info(f"*** Forecast error: {e}\n")
        stop_event.wait(900)

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    traffic_threads = start_traffic(net) 

    # t_forecast = threading.Thread(target=run_forecast_loop, daemon=True)
    # t_forecast.start()

    CLI(net)

    info("\n*** CLI exited. Stopping traffic threads...\n")
    stop_event.set() 
    time.sleep(1) 
    
    info("*** Stopping Mininet network...\n")
    net.stop()
    info("*** Mininet stopped.\n")