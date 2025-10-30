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
# (Tidak ada perubahan di bagian ini)
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
             # Timeout sedikit diperpanjang untuk mengakomodasi jaringan yg lebih kompleks
             return node.cmd(cmd, timeout=15)
        except Exception as e:
             return None 

# ---------------------- ROUTER (TIDAK DIGUNAKAN DI FAT-TREE) ----------------------
# (Tidak ada perubahan di bagian ini)
class LinuxRouter(Node):
    """Router Linux (Tidak digunakan di FatTreeTopo)."""
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        if not stop_event.is_set():
            safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

# ---------------------- TOPOLOGI DATA CENTER (FAT-TREE) ----------------------
# (Tidak ada perubahan di bagian ini)
class FatTreeTopo(Topo):
    """
    Topologi Fat-Tree (k=4) standar untuk Data Center.
    """
    def build(self, k=4):
        info(f"*** Membangun Topologi Fat-Tree (k={k})...\n")
        if k % 2 != 0:
            raise ValueError("k harus angka genap")
        
        num_pods = k
        num_cores = (k // 2) ** 2
        num_aggs_per_pod = k // 2
        num_edges_per_pod = k // 2
        num_hosts_per_edge = k // 2
        
        bw_core_agg = 20
        bw_agg_edge = 10
        bw_edge_host = 5

        # --- Membuat Switch ---
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

        info(f"  > Core Switches: {len(cores)}\n")
        info(f"  > Aggregation Switches: {len(aggs) * len(aggs[0])}\n")
        info(f"  > Edge Switches: {len(edges) * len(edges[0])}\n")

        # --- Link Core <-> Aggregation ---
        for i in range(num_cores):
            core_sw = cores[i]
            agg_index_to_connect = i // (k // 2) 
            for p in range(num_pods):
                agg_sw = aggs[p][agg_index_to_connect]
                self.addLink(core_sw, agg_sw, bw=bw_core_agg)

        # --- Link Aggregation <-> Edge ---
        for p in range(num_pods):
            for a in range(num_aggs_per_pod):
                for e in range(num_edges_per_pod):
                    self.addLink(aggs[p][a], edges[p][e], bw=bw_agg_edge)

        # --- Link Edge <-> Hosts ---
        host_ip_counter = 1
        host_mac_counter = 1
        for p in range(num_pods):
            for e in range(num_edges_per_pod):
                edge_sw = edges[p][e]
                for h in range(num_hosts_per_edge):
                    host_ip = f'10.0.0.{host_ip_counter}/24'
                    # Format MAC: 00:00:00:00:00:XX
                    host_mac = f'00:00:00:00:00:{host_mac_counter:02x}' 
                    host_name = f'h{host_ip_counter}'
                    
                    host = self.addHost(host_name, ip=host_ip, mac=host_mac, intfName1='eth0')
                    
                    self.addLink(edge_sw, host, bw=bw_edge_host)
                    
                    host_ip_counter += 1
                    host_mac_counter += 1
        
        info(f"  > Total Hosts: {host_ip_counter - 1}\n")


# ---------------------- RANDOM TRAFFIC (SAMA) ----------------------
# (Tidak ada perubahan di bagian ini)
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
# (Tidak ada perubahan di bagian ini)
def start_traffic(net):
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h7 = net.get('h4', 'h5', 'h7') 

    info("\n*** Membuat link network namespace (untuk collector.py)...\n")
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=True)
    
    all_hosts = [net.get(f'h{i}') for i in range(1, 17)] 
    
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

    info("\n*** Starting iperf servers (Simulating services, Fat-Tree)\n")
    safe_cmd(h4, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h5, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h7, "iperf -s -u -p 1935 -i 1 &")
    time.sleep(1)

    info("\n*** Warming up network with pingAll...\n")
    try:
         info("Waiting for switch <-> controller connection...")
         net.waitConnected()
         info("Connection established. Starting pingAll...")
         net.pingAll(timeout='3') 
         info("*** Warm-up complete.\n")
    except Exception as e:
         info(f"*** Warning: pingAll or waitConnected failed: {e}\n")

    info("-----------------------------------------------------------\n")
    info("💡 TELEMETRI LIVE (dari server iperf) akan muncul di bawah ini:\n")
    info("-----------------------------------------------------------\n")

    info("\n*** Starting client traffic threads (Simulating users, Fat-Tree)\n")
    base_range_min = 0.5
    base_range_max = 5.0 
    
    threads = [
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.0.4', 443, base_range_min, base_max_bw, 12345), daemon=False),
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.0.5', 443, base_range_min, base_max_bw, 67890), daemon=False),
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.0.7', 1935, base_range_min, base_max_bw, 98765), daemon=False)
    ]
    for t in threads:
        t.start()
    return threads

# ---------------------- MAIN [UPDATE] ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    
    # [UPDATE] Port diubah ke 6633 (default Ryu)
    # Ini adalah perbaikan utamanya.
    net = Mininet(topo=FatTreeTopo(k=4),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink,
                  autoSetMacs=True) 

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
        for i in range(1, 17):
            host = net.get(f'h{i}')
            if host:
                 host.cmd("killall -9 iperf") 

        info("*** Membersihkan link network namespace...\n")
        for i in range(1, 17):
            host_name = f'h{i}'
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