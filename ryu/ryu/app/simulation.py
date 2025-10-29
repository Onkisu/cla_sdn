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

# ---------------------- TOPOLOGI DATA CENTER (SPINE-LEAF) ----------------------
class SpineLeafTopo(Topo):
    """
    Topologi Spine-Leaf 
    
    Spines: 2 (sS1, sS2)
    Leafs: 2 (sL1, sL2)
    Hosts per Leaf: 2 (Total 4 host)
    
    Naming:
    - Spines: sS1, sS2
    - Leafs: sL1, sL2
    - Hosts: h1L1 (Host 1 di Leaf 1), h2L1 (Host 2 di Leaf 1), dst.
    
    IP:
    - h1L1: 10.0.1.1/16
    - h2L1: 10.0.1.2/16
    - h1L2: 10.0.2.1/16
    - h2L2: 10.0.2.2/16
    """
    
    def __init__(self, spines=2, leafs=2, hosts_per_leaf=2):
        Topo.__init__(self)
        
        self.spines = spines
        self.leafs = leafs
        self.hosts_per_leaf = hosts_per_leaf

        info("*** Membangun Topologi Spine-Leaf...\n")
        
        # 1. Tambahkan Spines
        spine_switches = []
        for s in range(1, spines + 1):
            s_name = 'sS%s' % s
            spine_switches.append(self.addSwitch(s_name))
            info(f"  Menambahkan Spine: {s_name}\n")

        # 2. Tambahkan Leafs dan Hosts
        leaf_switches = []
        self.host_list = [] # Untuk melacak nama host
        for l in range(1, leafs + 1):
            l_name = 'sL%s' % l
            leaf_sw = self.addSwitch(l_name)
            leaf_switches.append(leaf_sw)
            info(f"  Menambahkan Leaf: {l_name}\n")

            # 3. Hubungkan setiap Leaf ke SETIAP Spine
            for s_sw in spine_switches:
                self.addLink(leaf_sw, s_sw, bw=100) # Link Spine-Leaf 100Gbps
            
            # 4. Tambahkan host ke Leaf ini (Rack)
            for h in range(1, hosts_per_leaf + 1):
                host_name = 'h%sL%s' % (h, l)
                host_ip = '10.0.%s.%s/16' % (l, h) # IP unik: 10.0.[leaf_id].[host_id]
                host_mac = '00:00:00:%02x:%02x:%02x' % (0, l, h) # MAC unik
                
                info(f"    Menambahkan Host: {host_name} (IP: {host_ip})\n")
                host = self.addHost(host_name, ip=host_ip, mac=host_mac)
                self.host_list.append(host_name)
                
                # Hubungkan host ke Leaf-nya
                # 'intfName1='eth0'' SANGAT PENTING untuk collector.py
                self.addLink(host, leaf_sw, bw=10, intfName1='eth0') # Link Host 10Gbps

# ---------------------- FUNGSI TRAFFIC (SAMA) ----------------------
def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    if not output: return
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
    """Generates random UDP traffic bursts (Mice Flows)."""
    rng = random.Random()
    rng.seed(seed)
    info(f"[Mice Flow] {client.name} -> {server_ip} (UDP Bursts) Dimulai\n")
    while not stop_event.is_set():
        try:
            target_bw = rng.uniform(base_min_bw, base_max_bw) # Bandwidth kecil (misal 0.5-2M)
            bw_str = f"{target_bw:.2f}M"
            burst_time = rng.uniform(0.5, 2.5)
            burst_time_str = f"{burst_time:.1f}"
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            output = safe_cmd(client, cmd)
            # _log_iperf(client.name, server_ip, output, burst_time_str, bw_str) # Kurangi spam

            if stop_event.is_set(): break
            pause_duration = rng.uniform(0.5, 2.0)
            stop_event.wait(pause_duration)
        except Exception as e:
            if stop_event.is_set(): break
            stop_event.wait(1) 

def generate_elephant_traffic(client, server_ip, port, duration_s=3600):
    """Generates a long-running TCP traffic flow (Elephant Flow)."""
    info(f"[Elephant Flow] {client.name} -> {server_ip} (TCP Continuous) Dimulai\n")
    # Bandwidth -b tidak diset, biarkan TCP mengambil sebanyak mungkin
    cmd = f"iperf -c {server_ip} -p {port} -t {duration_s} -i 10"
    safe_cmd(client, cmd)
    info(f"[Elephant Flow] {client.name} -> {server_ip} Selesai\n")

# ---------------------- START TRAFFIC (DIUBAH UNTUK DC) ----------------------
def start_traffic(net):
    """
    Mulai traffic East-West (server-ke-server) yang realistis
    - Mice Flows (UDP bursts, sensitif latensi)
    - Elephant Flows (TCP long-running, sensitif bandwidth)
    """
    
    # Ambil host berdasarkan nama barunya
    h1L1, h2L1 = net.get('h1L1', 'h2L1') # Host di Leaf 1
    h1L2, h2L2 = net.get('h1L2', 'h2L2') # Host di Leaf 2

    info("\n*** Memulai Server iperf (Simulasi Layanan DC)...\n")
    # h1L2 (Web Server) - Menerima Mice Flows (UDP)
    safe_cmd(h1L2, "iperf -s -u -p 5001 -i 1 &")
    
    # h2L1 (API Server) - Menerima Mice Flows (UDP)
    safe_cmd(h2L1, "iperf -s -u -p 5002 -i 1 &")
    
    # h2L2 (Backup Server) - Menerima Elephant Flow (TCP)
    safe_cmd(h2L2, "iperf -s -p 5003 -i 1 &")
    time.sleep(1)

    info("\n*** Memulai Traffic Generator...\n")
    threads = [
        # 1. Mice Flow: h1L1 (Klien) -> h1L2 (Web) - Melintasi Spine
        threading.Thread(target=generate_client_traffic, args=(h1L1, '10.0.2.1', 5001, 0.5, 2.0, 12345), daemon=False),
        
        # 2. Mice Flow: h1L2 (Web) -> h2L1 (API) - Melintasi Spine
        threading.Thread(target=generate_client_traffic, args=(h1L2, '10.0.1.2', 5002, 0.5, 2.0, 67890), daemon=False),
        
        # 3. Elephant Flow: h2L1 (DB) -> h2L2 (Backup) - Melintasi Spine
        threading.Thread(target=generate_elephant_traffic, args=(h2L1, '10.0.2.2', 5003), daemon=False)
    ]
    for t in threads:
        t.start()
    return threads

# ---------------------- HELPER (SAMA PENTING) ----------------------
def link_netns_for_collector(net, topo):
    """Membuat link namespace untuk collector.py"""
    info("\n*** Membuat link network namespace (untuk collector.py)...\n")
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=True)
    
    for host_name in topo.host_list:
        try:
            host = net.get(host_name)
            pid = host.pid
            cmd = ['sudo', 'ip', 'netns', 'attach', host_name, str(pid)]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            info(f"  > Link namespace untuk {host_name} (PID: {pid}) dibuat.\n")
        except Exception as e:
            info(f"  > GAGAL membuat link namespace untuk {host_name}: {e}\n")

def cleanup_netns(topo):
    """Membersihkan link namespace saat berhenti"""
    info("*** Membersihkan link network namespace...\n")
    for host_name in topo.host_list:
        cmd = ['sudo', 'ip', 'netns', 'del', host_name]
        try:
            subprocess.run(cmd, check=False, capture_output=True)
        except Exception:
            pass # Abaikan error jika file sudah tidak ada

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    
    # Buat topo Spine-Leaf
    topo = SpineLeafTopo(spines=2, leafs=2, hosts_per_leaf=2)
    
    net = Mininet(topo=topo,
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)

    info("*** Memulai Jaringan Mininet...\n")
    net.start()

    # PENTING: Link namespace untuk collector
    link_netns_for_collector(net, topo)

    # PENTING: Warm-up jaringan (pingall)
    info("\n*** Warming up network (pingAll)...\n")
    net.pingAll(timeout='1')
    info("*** Warm-up complete.\n")

    # Mulai traffic
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
        info("*** Mengirim sinyal stop ke semua thread...\n")
        stop_event.set()

        info("*** Menunggu Traffic threads berhenti...\n")
        for t in traffic_threads:
            if t.is_alive(): 
                t.join(timeout=5) 
            
        info("*** Membersihkan proses iperf yang mungkin tersisa...\n")
        for host_name in topo.host_list:
            host = net.get(host_name)
            if host:
                 host.cmd("killall -9 iperf") 

        # PENTING: Cleanup namespace
        cleanup_netns(topo)
        
        info("*** Menghentikan jaringan Mininet...\n")
        try:
            net.stop()
            info("*** Mininet berhenti.\n")
        except Exception as e:
             info(f"*** ERROR saat net.stop(): {e}. Coba cleanup manual 'sudo mn -c'.\n")