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
        # [FIX] Ganti dari 'addSwitchB' (typo) ke 'addSwitch'
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
def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw):
    """
    Generates random UDP traffic bursts using iperf.
    The *range* (min/max) is also randomized in each loop
    to simulate different user behaviors (e.g., switching video quality).
    """
    
    # [FIX v4] Seed (kocok) mesin random GLOBAL, tapi gunakan
    # hash(client.name) yang DIJAMIN unik untuk tiap thread.
    # Kita gabung juga dengan time() biar tiap run beda.
    seed_value = hash(client.name) + int(time.time() * 1000)
    random.seed(seed_value) 

    info(f"Starting random traffic for {client.name} (Seed: {seed_value}) -> {server_ip} (Base Range: [{base_min_bw}M - {base_max_bw}M])\n")
    
    while not stop_event.is_set():
        try:
            # === [MODIFIKASI INTI] ===
            # 1. Buat range (skala/limit) yang dinamis/acak di setiap loop
            
            # [FIX v4] Kita kembali pakai random.uniform() (global)
            # karena kita udah seed di atas.
            current_max_bw = random.uniform(base_max_bw * 0.4, base_max_bw * 1.1)
            current_min_bw = random.uniform(base_min_bw * 0.4, current_max_bw * 0.8)

            # Pastiin min/max-nya waras (nggak 0 atau min > max)
            current_min_bw = max(0.1, current_min_bw) # Minimal 0.1M
            current_max_bw = max(current_min_bw + 0.2, current_max_bw) # Max minimal 0.2M di atas min

            # info(f"PROFILE: {client.name} new dynamic range: [{current_min_bw:.2f}M - {current_max_bw:.2f}M]\n")
            # === [SELESAI MODIFIKASI INTI] ===

            # 2. Tentukan target bandwidth dari range *baru* yang dinamis
            target_bw = random.uniform(current_min_bw, current_max_bw)
            bw_str = f"{target_bw:.2f}M"
            
            # Durasi burst time (tetap acak)
            burst_time = random.uniform(0.5, 2.5) 
            burst_time_str = f"{burst_time:.1f}"

            # 3. Execute iperf burst
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            
            output = safe_cmd(client, cmd)

            if not output:
                continue

            # 4. Parsing output asli iperf
            try:
                csv_line = output.strip().split('\n')[-1]
                parts = csv_line.split(',')
                actual_bytes = int(parts[7])
                
                # 5. Cetak log ASLI di CLI utama
                info(f"CLIENT LOG: {client.name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time:.1f}s (Target BW: {bw_str}ps)\n")

            except Exception as e:
                info(f"Could not parse iperf output for {client.name}: {e}\nOutput was: {output}\n")


            # 6. Random pause 0.5–2s
            pause_duration = random.uniform(0.5, 2.0)
            stop_event.wait(pause_duration)

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
    
    safe_cmd(h4, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h5, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h7, "iperf -s -u -p 1935 -i 1 &")
    time.sleep(1)

    info("-----------------------------------------------------------\n")
    info("💡 TELEMETRI LIVE (dari server) akan muncul di bawah ini:\n")
    info("-----------------------------------------------------------\n")


    info("\n*** Starting client traffic threads (Simulating users)\n")
    # Argumen di sini (1.5, 5.0, dll) sekarang jadi 'base' range
    threads = [
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.1.1', 443, 1.5, 5.0), daemon=True), 
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.1.2', 443, 0.8, 3.5), daemon=True), 
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.2.1', 1935, 0.2, 1.8), daemon=True)
    ]
    for t in threads:
        t.start()
        # [FIX v4] Kasih jeda dikit banget antar start,
        # biar sistem sempet ngubah time() untuk seed berikutnya
        time.sleep(0.01) 
        
    return threads 

# ---------------------- FORECAST LOOP ----------------------
def run_forecast_loop():
# ... (sisa kode MAIN sama persis, tidak perlu diubah) ...
# ... (scroll ke bawah) ...
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

    # [FIX] Ganti CLI(net) dengan loop ini
    info("\n*** Skrip berjalan. Telemetri akan muncul di bawah.")
    info("*** Tekan Ctrl+C untuk berhenti kapan saja.\n")
    try:
        while True:
            # Biarin main thread tetep idup buat nampung log
            time.sleep(30) 
            
    except KeyboardInterrupt:
        info("\n\n*** Ctrl+C diterima. Menghentikan semua proses...\n")
    
    # [FIX] Cleanup dipindah ke sini
    info("*** Mengirim sinyal stop ke semua thread...\n")
    stop_event.set() 
    time.sleep(1) # Kasih waktu thread buat mati
    
    info("*** Menghentikan jaringan Mininet...\n")
    net.stop()
    info("*** Mininet berhenti.\n")

