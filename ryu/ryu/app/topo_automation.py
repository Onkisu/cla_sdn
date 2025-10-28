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
        return None 
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

# ---------------------- RANDOM TRAFFIC (FIX v6 - PISAH FUNGSI) ----------------------

def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    """Helper
    fungsi logging biar nggak repetitif."""
    try:
        csv_line = output.strip().split('\n')[-1]
        parts = csv_line.split(',')
        actual_bytes = int(parts[7])
        info(f"CLIENT LOG: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time_str}s (Target BW: {bw_str}ps)\n")
    except Exception as e:
        info(f"Could not parse iperf output for {client_name}: {e}\nOutput was: {output}\n")

# --- FUNGSI A: YOUTUBE (MOODY/SWING BESAR) ---
def generate_youtube_traffic(client, server_ip, port, base_min_bw, base_max_bw):
    rng = random.Random(hash(client.name))
    info(f"Starting YouTube (Moody) traffic for {client.name} (Seed: {hash(client.name)})\n")
    while not stop_event.is_set():
        try:
            # Aturan main 'Moody': swing-nya besar (bisa 20% - 120% dari base)
            current_max_bw = rng.uniform(base_max_bw * 0.2, base_max_bw * 1.2)
            current_min_bw = rng.uniform(base_min_bw * 0.1, current_max_bw * 0.7)
            current_min_bw = max(0.1, current_min_bw)
            current_max_bw = max(current_min_bw + 0.2, current_max_bw)

            target_bw = rng.uniform(current_min_bw, current_max_bw)
            bw_str = f"{target_bw:.2f}M"
            
            # Burst time normal
            burst_time = rng.uniform(0.5, 2.5) 
            burst_time_str = f"{burst_time:.1f}"

            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            output = safe_cmd(client, cmd)
            if not output: continue

            _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)
            
            # Pause normal
            stop_event.wait(rng.uniform(0.5, 2.0))
        except Exception as e:
            if stop_event.is_set(): break
            info(f"Error traffic for {client.name}: {e}\n")
            stop_event.wait(3)

# --- FUNGSI B: NETFLIX (STABIL) ---
def generate_netflix_traffic(client, server_ip, port, base_min_bw, base_max_bw):
    rng = random.Random(hash(client.name))
    info(f"Starting Netflix (Stable) traffic for {client.name} (Seed: {hash(client.name)})\n")
    while not stop_event.is_set():
        try:
            # Aturan main 'Stabil': swing-nya kecil (selalu 70% - 100% dari base)
            current_max_bw = rng.uniform(base_max_bw * 0.7, base_max_bw * 1.0)
            current_min_bw = rng.uniform(base_min_bw * 0.6, current_max_bw * 0.9)
            current_min_bw = max(0.1, current_min_bw)
            current_max_bw = max(current_min_bw + 0.2, current_max_bw)

            target_bw = rng.uniform(current_min_bw, current_max_bw)
            bw_str = f"{target_bw:.2f}M"
            
            # Burst time lebih lama (simulasi buffering/chunk)
            burst_time = rng.uniform(1.0, 3.0) 
            burst_time_str = f"{burst_time:.1f}"

            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            output = safe_cmd(client, cmd)
            if not output: continue

            _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)
            
            # Pause lebih lama
            stop_event.wait(rng.uniform(1.0, 3.0))
        except Exception as e:
            if stop_event.is_set(): break
            info(f"Error traffic for {client.name}: {e}\n")
            stop_event.wait(3)

# --- FUNGSI C: TWITCH (SPIKY/NOISE) ---
def generate_twitch_traffic(client, server_ip, port, base_min_bw, base_max_bw):
    rng = random.Random(hash(client.name))
    info(f"Starting Twitch (Spiky) traffic for {client.name} (Seed: {hash(client.name)})\n")
    while not stop_event.is_set():
        try:
            # Aturan main 'Spiky': range normal, tapi burst-nya pendek2
            current_max_bw = rng.uniform(base_max_bw * 0.5, base_max_bw * 1.1)
            current_min_bw = rng.uniform(base_min_bw * 0.4, current_max_bw * 0.8)
            current_min_bw = max(0.1, current_min_bw)
            current_max_bw = max(current_min_bw + 0.2, current_max_bw)

            target_bw = rng.uniform(current_min_bw, current_max_bw)
            bw_str = f"{target_bw:.2f}M"
            
            # Burst time pendek (spiky)
            burst_time = rng.uniform(0.2, 1.0) 
            burst_time_str = f"{burst_time:.1f}"

            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            output = safe_cmd(client, cmd)
            if not output: continue

            _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)
            
            # Pause juga pendek
            stop_event.wait(rng.uniform(0.3, 1.5))
        except Exception as e:
            if stop_event.is_set(): break
            info(f"Error traffic for {client.name}: {e}\n")
            stop_event.wait(3)

# ---------------------- END RANDOM TRAFFIC ----------------------


def start_traffic(net):
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h6 = net.get('h4', 'h5', 'h6')
    h7 = net.get('h7')

    info("\n*** Starting iperf servers (Simulating services)\n")
    
    safe_cmd(h4, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h5, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h7, "iperf -s -u -p 1935 -i 1 &")
    time.sleep(1)

    info("-----------------------------------------------------------\n")
    info("💡 TELEMETRI LIVE (dari server) akan muncul di bawah ini:\n")
    info("-----------------------------------------------------------\n")


    info("\n*** Starting client traffic threads (Simulating users)\n")
    
    # [FIX v6] Panggil FUNGSI YANG BEDA untuk tiap thread
    threads = [
        # h1 -> YouTube (Moody)
        threading.Thread(target=generate_youtube_traffic, args=(h1, '10.0.1.1', 443, 1.5, 5.0), daemon=True), 
        # h2 -> Netflix (Stable)
        threading.Thread(target=generate_netflix_traffic, args=(h2, '10.0.1.2', 443, 0.8, 3.5), daemon=True), 
        # h3 -> Twitch (Spiky)
        threading.Thread(target=generate_twitch_traffic, args=(h3, '10.0.2.1', 1935, 0.2, 1.8), daemon=True)
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

    info("\n*** Skrip berjalan. Telemetri akan muncul di bawah.")
    info("*** Tekan Ctrl+C untuk berhenti kapan saja.\n")
    try:
        while True:
            time.sleep(30) 
            
    except KeyboardInterrupt:
        info("\n\n*** Ctrl+C diterima. Menghentikan semua proses...\n")
    
    info("*** Mengirim sinyal stop ke semua thread...\n")
    stop_event.set() 
    time.sleep(1) 
    
    info("*** Menghentikan jaringan Mininet...\n")
    net.stop()
    info("*** Mininet berhenti.\n")

