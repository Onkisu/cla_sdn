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

# ---------------------- GLOBAL LOCK & STOP EVENT (SAMA) ----------------------
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
            return None

# ---------------------- ROUTER (SAMA) & TOPOLOGI (BARU) ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        if not stop_event.is_set():
            safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class DataCenterTopo(Topo):
    def build(self):
        info("*** Membangun Topologi Data Center...\n")
        
        # Core Router (L3 Boundary)
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        
        # Switch Eksternal (Tempat user terhubung)
        ext_sw = self.addSwitch('ext_sw') # Akan menjadi DPID 1
        
        # Switch Internal DC (Top-of-Rack)
        tor1 = self.addSwitch('tor1') # DPID 2
        tor2 = self.addSwitch('tor2') # DPID 3
        
        # --- Klien Eksternal ---
        user1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        user2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        user3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        
        # --- Server Internal DC ---
        # Rack 1 (Web & Cache Tier)
        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')
        
        # Rack 2 (App & DB Tier)
        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        # Links Klien Eksternal
        self.addLink(user1, ext_sw, bw=10, intfName1='eth0')
        self.addLink(user2, ext_sw, bw=10, intfName1='eth0')
        self.addLink(user3, ext_sw, bw=10, intfName1='eth0')
        
        # Links Server Internal
        self.addLink(web1, tor1, bw=20, intfName1='eth0')
        self.addLink(web2, tor1, bw=20, intfName1='eth0')
        self.addLink(cache1, tor1, bw=20, intfName1='eth0')
        self.addLink(app1, tor2, bw=20, intfName1='eth0')
        self.addLink(db1, tor2, bw=20, intfName1='eth0')
        
        # Link Router (Interkoneksi)
        # Interface Eksternal
        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        # Interface Internal (ke Rack)
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})


# ---------------------- RANDOM TRAFFIC (SAMA) ----------------------
def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    # (Fungsi ini sama persis, tidak perlu diubah)
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
            info(f"TRAFFIC LOG: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time_str}s (Target BW: {bw_str}ps)\n")
        else:
            info(f"Could not find valid CSV in iperf output for {client_name}:\nOutput was:\n{output}\n")
    except Exception as e:
        info(f"Could not parse iperf output for {client_name}: {e}\nOutput was:\n{output}\n")

def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed):
    # (Fungsi ini sama persis, tidak perlu diubah)
    rng = random.Random()
    rng.seed(seed)
    info(f"Starting traffic for {client.name} (Seed: {seed}) -> {server_ip}:{port} (Base Range: [{base_min_bw}M - {base_max_bw}M])\n")
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

# ---------------------- START TRAFFIC (BARU) ----------------------
def start_traffic(net):
    # Dapatkan semua host
    user1, user2, user3 = net.get('user1', 'user2', 'user3')
    web1, web2, cache1 = net.get('web1', 'web2', 'cache1')
    app1, db1 = net.get('app1', 'db1')
    all_hosts = [user1, user2, user3, web1, web2, cache1, app1, db1]

    # --- Membuat link namespace (SAMA, tapi untuk host baru) ---
    info("\n*** Membuat link network namespace (untuk collector.py)...\n")
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=True)
    for host in all_hosts:
        if not host: continue
        try:
            pid = host.pid
            cmd = ['sudo', 'ip', 'netns', 'attach', host.name, str(pid)]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            info(f"  > Link namespace untuk {host.name} (PID: {pid}) dibuat.\n")
        except Exception as e:
            info(f"  > GAGAL membuat link namespace for {host.name}: {e}\n")
            if hasattr(e, 'stderr'):
                info(f"  > Stderr: {e.stderr}\n")

    info("\n*** Starting iperf servers (Simulating DC Services)\n")
    # Layanan yang diakses dari luar (North-South)
    safe_cmd(web1, "iperf -s -u -p 443 -i 1 &")   # Web Service (HTTPS)
    safe_cmd(web2, "iperf -s -u -p 80 -i 1 &")    # Web Service (HTTP)
    safe_cmd(app1, "iperf -s -u -p 8080 -i 1 &")  # API Service
    # Layanan internal (East-West)
    safe_cmd(cache1, "iperf -s -u -p 6379 -i 1 &") # Cache (Redis)
    safe_cmd(db1, "iperf -s -u -p 5432 -i 1 &")    # Database (Postgres)
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

    info("\n*** Starting client traffic threads (Simulating Users & Services)\n")
    base_range_min = 0.5
    base_range_max = 5.0
    threads = []
    
    # --- 1. North-South Traffic (Users -> DC) ---
    threads.append(threading.Thread(target=generate_client_traffic, args=(user1, '10.10.1.1', 443, base_range_min, base_range_max, 12345), daemon=False))
    threads.append(threading.Thread(target=generate_client_traffic, args=(user2, '10.10.1.2', 80, base_range_min, base_range_max, 67890), daemon=False))
    threads.append(threading.Thread(target=generate_client_traffic, args=(user3, '10.10.2.1', 8080, base_range_min*0.5, base_range_max*0.5, 98765), daemon=False)) # API traffic
    
    # --- 2. East-West Traffic (Server <-> Server) ---
    # (Ini tidak akan terlihat oleh collector kita, karena tidak melewati ext_sw)
    threads.append(threading.Thread(target=generate_client_traffic, args=(web1, '10.10.2.1', 8080, base_range_min, base_range_max, 11111), daemon=False)) # Web1 -> App1
    threads.append(threading.Thread(target=generate_client_traffic, args=(web2, '10.10.1.3', 6379, base_range_min, base_range_max, 22222), daemon=False)) # Web2 -> Cache1
    threads.append(threading.Thread(target=generate_client_traffic, args=(app1, '10.10.2.2', 5432, base_range_min, base_range_max, 33333), daemon=False)) # App1 -> DB1

    for t in threads:
        t.start()
    return threads

# ---------------------- MAIN (SAMA, tapi host disesuaikan) ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=DataCenterTopo(), # <-- Menggunakan Topo Baru
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)

    info("*** Memulai Jaringan Mininet...\n")
    net.start()

    traffic_threads = start_traffic(net)

    info("\n*** Skrip Simulasi & Traffic berjalan.")
    info("*** 'collector_dc.py' harus dijalankan di terminal terpisah.")
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
        # Update range host
        host_names = ['user1', 'user2', 'user3', 'web1', 'web2', 'cache1', 'app1', 'db1']
        for name in host_names:
            host = net.get(name)
            if host:
                host.cmd("killall -9 iperf") 

        info("*** Membersihkan link network namespace...\n")
        # Update range host
        for name in host_names:
            cmd = ['sudo', 'ip', 'netns', 'del', name]
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