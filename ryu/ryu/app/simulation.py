#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
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
        if stop_event.is_set():
             return None
        try:
             return node.cmd(cmd, timeout=10)
        except Exception as e:
             # info(f"  [safe_cmd Error] Node {node.name}, Cmd: '{cmd[:50]}...': {e}\n")
             return None 

# ---------------------- ROUTER & TOPOLOGY ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        if not stop_event.is_set():
            safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class ComplexTopo(Topo):
    def build(self):
        info("*** Membangun Topologi Sesuai v8.0...\n")
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3') 
        
        # --- [FIX DI SINI] ---
        # Kita tambahkan 'intfName1='eth0'' untuk host h1, h2, h3
        # Ini memaksa nama interface di dalam host menjadi 'eth0'
        # ---------------------
        h1 = self.addHost('h1', ip='10.0.0.1/24', mac='00:00:00:00:00:01', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', mac='00:00:00:00:00:02', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', mac='00:00:00:00:00:03', defaultRoute='via 10.0.0.254')
        
        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')
        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Links
        # --- [FIX DI SINI] ---
        # Tambahkan 'intfName1='eth0'' ke link host yang dimonitor
        # ---------------------
        self.addLink(h1, s1, bw=5, intfName1='eth0')
        self.addLink(h2, s1, bw=5, intfName1='eth0')
        self.addLink(h3, s1, bw=5, intfName1='eth0')
        
        # Link untuk host/server lain tidak perlu diubah jika tidak dimonitor
        self.addLink(h4, s2, bw=10, delay='32ms', loss=2)
        self.addLink(h5, s2, bw=10, delay='47ms', loss=2)
        self.addLink(h6, s2, bw=10)
        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)
        
        # Link router
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})


# ---------------------- RANDOM TRAFFIC (SAMA) ----------------------
def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    if not output: 
        return
    try:
        lines = output.strip().split('\n')
        csv_line = None
        for line in reversed(lines): 
             if ',' in line and len(line.split(',')) > 7: 
                  csv_line = line
                  break 
        if csv_line:
            parts = csv_line.split(',')
            actual_bytes = int(parts[7])
            info(f"CLIENT LOG: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time_str}s (Target BW: {bw_str}ps)\n")
        else:
             info(f"Could not find valid CSV in iperf output for {client_name}:\nOutput was:\n{output}\n")
    except Exception as e:
        info(f"Could not parse iperf output for {client_name}: {e}\nOutput was:\n{output}\n")


def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed):
    rng = random.Random()
    rng.seed(seed)
    info(f"Starting random traffic for {client.name} (Seed: {seed}) -> {server_ip} (Base Range: [{base_min_bw}M - {base_max_bw}M])\n")
    while not stop_event.is_set():
        try:
            current_max_bw = rng.uniform(base_max_bw * 0.4, base_max_bw * 1.1)
            current_min_bw = rng.uniform(base_min_bw * 0.4, current_max_bw * 0.8)
            current_min_bw = max(0.1, current_min_bw)
            current_max_bw = max(current_min_bw + 0.2, current_max_bw)
            target_bw = rng.uniform(current_min_bw, current_max_bw)
            bw_str = f"{target_bw:.2f}M"
            burst_time = rng.uniform(0.5, 2.5)
            burst_time_str = f"{burst_time:.1f}"
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            output = safe_cmd(client, cmd)
            _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)
            if stop_event.is_set(): break
            pause_duration = rng.uniform(0.5, 2.0)
            stop_event.wait(pause_duration)
        except Exception as e:
            if stop_event.is_set(): break
            stop_event.wait(1) 

# ---------------------- START TRAFFIC (SAMA) ----------------------
def start_traffic(net):
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h7 = net.get('h4', 'h5', 'h7') 

    # --- Membuat link namespace (SAMA, TETAP DIPERLUKAN) ---
    info("\n*** Membuat link network namespace (untuk collector.py)...\n")
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=True)
    for host in [h1, h2, h3]:
        try:
            pid = host.pid
            cmd = ['sudo', 'ip', 'netns', 'attach', host.name, str(pid)]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            info(f"  > Link namespace untuk {host.name} (PID: {pid}) dibuat.\n")
        except Exception as e:
            info(f"  > GAGAL membuat link namespace for {host.name}: {e}\n")
            if hasattr(e, 'stderr'):
                info(f"  > Stderr: {e.stderr}\n")
    # --- Selesai ---

    info("\n*** Starting iperf servers (Simulating services, v8.0)\n")
    safe_cmd(h4, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h5, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h7, "iperf -s -u -p 1935 -i 1 &")
    time.sleep(1)

    info("\n*** Warming up network with pingAll...\n")
    try:
         info("Waiting for switch <-> controller connection...")
         net.waitConnected()
         info("Connection established. Starting pingAll...")
         net.pingAll(timeout='1') 
         info("*** Warm-up complete.\n")
    except Exception as e:
         info(f"*** Warning: pingAll or waitConnected failed: {e}\n")

    info("-----------------------------------------------------------\n")
    info("💡 TELEMETRI LIVE (dari server iperf) akan muncul di bawah ini:\n")
    info("-----------------------------------------------------------\n")

    info("\n*** Starting client traffic threads (Simulating users, v8.0)\n")
    base_range_min = 0.5
    base_range_max = 5.0
    threads = [
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.1.1', 443, base_range_min, base_range_max, 12345), daemon=False),
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.1.2', 443, base_range_min, base_range_max, 67890), daemon=False),
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.2.1', 1935, base_range_min, base_range_max, 98765), daemon=False)
    ]
    for t in threads:
        t.start()
    return threads

# ---------------------- MAIN (SAMA) ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)

    info("*** Memulai Jaringan Mininet...\n")
    net.start()

    traffic_threads = start_traffic(net)

    info("\n*** Skrip Simulasi & Traffic berjalan.")
    info("*** 'collector.py' harus dijalankan di terminal terpisah.")
    info("*** Tekan Ctrl+C untuk berhenti kapan saja.\n")
    
    try:
        for t in traffic_threads:
            if t.is_alive():
                 t.join() 
    except KeyboardInterrupt:
        info("\n\n*** Ctrl+C diterima. Menghentikan semua proses...\n")
    finally: 
        info("*** Mengirim sinyal stop ke semua thread traffic...\n")
        stop_event.set()

        info("*** Menunggu Traffic threads berhenti...\n")
        for t in traffic_threads:
            if t.is_alive(): 
                t.join(timeout=5) 
            
        info("*** Membersihkan proses iperf yang mungkin tersisa...\n")
        for i in range(1, 8):
            host = net.get(f'h{i}')
            if host:
                 host.cmd("killall -9 iperf") 

        info("*** Membersihkan link network namespace...\n")
        for host_name in ['h1', 'h2', 'h3']:
            cmd = ['sudo', 'ip', 'netns', 'del', host_name]
            try:
                subprocess.run(cmd, check=False, capture_output=True)
            except Exception:
                pass 
        info("*** Menghentikan jaringan Mininet...\n")
        try:
            net.stop()
            info("*** Mininet berhenti.\n")
        except Exception as e:
             info(f"*** ERROR saat net.stop(): {e}. Coba cleanup manual 'sudo mn -c'.\n")