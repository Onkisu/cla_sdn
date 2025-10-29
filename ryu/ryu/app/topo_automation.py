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
# ... (kode safe_cmd, LinuxRouter, ComplexTopo SAMA PERSIS) ...
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

# ---------------------- RANDOM TRAFFIC (FIX v7.1) ----------------------

def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    """Helper fungsi logging."""
# ... (kode _log_iperf SAMA PERSIS) ...
    try:
        # [FIX] Ambil baris PERTAMA [0], bukan baris TERAKHIR [-1]
        csv_line = output.strip().split('\n')[0]
        
        parts = csv_line.split(',')
        actual_bytes = int(parts[7])
        info(f"CLIENT LOG: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time_str}s (Target BW: {bw_str}ps)\n")
    except Exception as e:
        # Kalo parsing baris pertama GAGAL, baru kita print error
        info(f"Could not parse iperf output for {client_name}: {e}\nOutput was: {output}\n")

# ... (kode generate_client_traffic SAMA PERSIS) ...
def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed):
    """
    Generates random UDP traffic bursts using a DEDICATED random generator
    seeded with a hardcoded, unique value.
    """
    
    # Buat "mesin random" (generator) baru yang 100% LOKAL/PRIVAT.
    rng = random.Random()
    
    # Seed (kocok) "mesin" LOKAL ini pake 'seed' yang kita paksa
    rng.seed(seed) 

    info(f"Starting random traffic for {client.name} (Seed: {seed}) -> {server_ip} (Base Range: [{base_min_bw}M - {base_max_bw}M])\n")
    
    while not stop_event.is_set():
        try:
            # 1. Buat range (skala/limit) yang dinamis/acak di setiap loop
            current_max_bw = rng.uniform(base_max_bw * 0.4, base_max_bw * 1.1)
            current_min_bw = rng.uniform(base_min_bw * 0.4, current_max_bw * 0.8)
            current_min_bw = max(0.1, current_min_bw)
            current_max_bw = max(current_min_bw + 0.2, current_max_bw)

            # 2. Tentukan target bandwidth dari range *baru* yang dinamis
            target_bw = rng.uniform(current_min_bw, current_max_bw)
            bw_str = f"{target_bw:.2f}M"
            
            # 3. Durasi burst time (tetap acak)
            burst_time = rng.uniform(0.5, 2.5) 
            burst_time_str = f"{burst_time:.1f}"

            # 4. Execute iperf burst
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            
            output = safe_cmd(client, cmd)

            if not output:
                continue

            # 5. Parsing dan log output asli iperf
            if "Connection refused" in output or "read failed" in output:
                _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)
            else:
                _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)


            # 6. Random pause 0.5–2s
            pause_duration = rng.uniform(0.5, 2.0)
            stop_event.wait(pause_duration)

        except Exception as e:
            if stop_event.is_set():
                break
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

    # --- [FIX "Too many open files"] ---
    # "Panasin" L2 network-nya dulu. Ini bakal maksa Ryu belajar
    # semua MAC address dan nginstal flow L2 (MAC-based)
    info("\n*** Warming up L2 switching paths (pingAll)...\n")
    net.pingAll(timeout='1s') # Cukup 1 detik buat trigger ARP
    info("*** L2 paths learned by controller.\n")
    # --- [SELESAI FIX] ---


    info("-----------------------------------------------------------\n")
    info("💡 TELEMETRI LIVE (dari server) akan muncul di bawah ini:\n")
    info("-----------------------------------------------------------\n")


    info("\n*** Starting client traffic threads (Simulating users)\n")
    
# ... (sisa kode start_traffic dan MAIN SAMA PERSIS) ...
    # SAMAIN SEMUA BASE RANGE (sesuai request lu)
    base_range_min = 0.5
    base_range_max = 5.0
    
    # Panggil FUNGSI YANG SAMA, tapi pake SEED BEDA
    threads = [
        # h1 -> YouTube
        threading.Thread(target=generate_client_traffic, 
                         args=(h1, '10.0.1.1', 443, base_range_min, base_range_max, 12345), # Seed 12345
                         daemon=True), 
        # h2 -> Netflix
        threading.Thread(target=generate_client_traffic, 
                         args=(h2, '10.0.1.2', 443, base_range_min, base_range_max, 67890), # Seed 67890
                         daemon=True), 
        # h3 -> Twitch
        threading.Thread(target=generate_client_traffic, 
                         args=(h3, '10.0.2.1', 1935, base_range_min, base_range_max, 98765), # Seed 98765
                         daemon=True)
    ]
    for t in threads:
        t.start()
        
    return threads 

# ---------------------- FORECAST LOOP ----------------------
def run_forecast_loop():
    while not stop_event.is_set():
# ... (kode run_forecast_loop SAMA PERSIS) ...
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
# ... (sisa kode MAIN SAMA PERSIS) ...
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

