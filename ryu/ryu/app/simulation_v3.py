#!/usr/bin/python3
"""
Simulasi Data Center + VoIP di Mininet
- Iperf untuk layanan (web/api/db/cache)
- SIPp untuk VoIP (SIP/UAC + uas)
- Daily pattern generator per kategori (VoIP, Web, API, East-West, DB, Cache)
- Thread-safe execution dan cleanup
"""
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
import signal
import sys

# ---------------------- GLOBAL LOCK & STOP EVENT ----------------------
cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd, timeout=15):
    """Execute node.cmd() safely with lock. Returns output or None."""
    if stop_event.is_set():
        return None
    with cmd_lock:
        if stop_event.is_set():
            return None
        try:
            return node.cmd(cmd, timeout=timeout)
        except Exception:
            return None

# ---------------------- ROUTER & TOPOLOGI ----------------------
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
        ext_sw = self.addSwitch('ext_sw', dpid='1') # DPID Eksplisit 1
        
        # Switch Internal DC (Top-of-Rack)
        tor1 = self.addSwitch('tor1', dpid='2') # DPID Eksplisit 2
        tor2 = self.addSwitch('tor2', dpid='3') # DPID Eksplisit 3
        
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

# ---------------------- LOG PARSER ----------------------
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
            info(f"TRAFFIC LOG: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time_str}s (Target BW: {bw_str})\n")
        else:
            info(f"Could not find valid CSV in iperf output for {client_name}:\nOutput was:\n{output}\n")
    except Exception as e:
        info(f"Could not parse iperf output for {client_name}: {e}\nOutput was:\n{output}\n")

# ---------------------- DAILY PATTERN GENERATOR (PER KATEGORI) ----------------------
def daily_pattern(hour, base_min, base_max, seed_val=0, category='data'):
    """
    Smooth daily pattern with category-specific peak shape.
    hour: 0..23
    base_min/base_max: numeric range
    category: 'web','api','voip','east_west','db','cache'
    """
    # base sine wave
    phase = (seed_val % 10) * 0.02
    # category-specific shaping:
    if category == 'web':
        # strong peak during 9..18
        offset = -0.5
        amplitude = 1.0
    elif category == 'api':
        # steady daytime, moderate amplitude
        offset = -0.3
        amplitude = 0.8
    elif category == 'voip':
        # voice peaks during office hours but also morning spikes
        offset = -0.2
        amplitude = 0.9
    elif category == 'east_west':
        # relatively flat inside DC
        offset = -0.1
        amplitude = 0.45
    elif category == 'db':
        # small peaks aligned with API but shorter bursts
        offset = -0.25
        amplitude = 0.6
    elif category == 'cache':
        # cache has many small frequent bursts
        offset = -0.15
        amplitude = 0.7
    else:
        offset = -0.3
        amplitude = 0.6

    # build sine: shift so peak near midday (hour ~ 12)
    sine_val = 0.5 * (1 + math.sin(((hour / 24.0) * 2 * math.pi) - (math.pi/2) + phase + offset))
    baseline = 0.15 * base_max
    scaled = baseline + (sine_val * (base_max - baseline) * amplitude)
    jitter = random.uniform(0.90, 1.10)
    value = max(base_min, min(base_max, scaled * jitter))
    return value

# ---------------------- CLIENT TRAFFIC GENERATOR (IPERF) ----------------------
def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed, category='data'):
    rng = random.Random(seed)
    info(f"Starting traffic for {client.name} (Seed: {seed}) -> {server_ip}:{port} Category={category}\n")
    while not stop_event.is_set():
        try:
            hour = int(time.strftime("%H"))
            # bandwidth mengikuti pola harian berdasarkan kategori
            target_bw = daily_pattern(hour, base_min_bw, base_max_bw, seed, category)
            target_bw = max(0.01, target_bw)  # minimal small bw
            bw_str = f"{target_bw:.2f}M"

            # burst_time dan pause disesuaikan per kategori (interval berbeda2)
            if category == 'web':
                burst_time = rng.uniform(1.0, 3.0)
                pause_low, pause_high = 0.4, 1.5
            elif category == 'api':
                burst_time = rng.uniform(0.8, 2.0)
                pause_low, pause_high = 0.3, 1.0
            elif category == 'cache':
                burst_time = rng.uniform(0.2, 0.6)
                pause_low, pause_high = 0.1, 0.6
            elif category == 'db':
                burst_time = rng.uniform(0.6, 2.5)
                pause_low, pause_high = 0.5, 2.0
            elif category == 'east_west':
                burst_time = rng.uniform(0.5, 1.5)
                pause_low, pause_high = 0.3, 1.2
            else:
                burst_time = rng.uniform(0.5, 2.0)
                pause_low, pause_high = 0.5, 2.0

            burst_time_str = f"{burst_time:.1f}"

            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_time_str} -y C"
            output = safe_cmd(client, cmd)
            _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)

            if stop_event.is_set(): break

            # pause di antara burst: gunakan daily_pattern untuk menentukan frekuensi,
            # lalu gabungkan dengan small randomized pause range per kategori
            pattern_pause = daily_pattern(hour, pause_low, pause_high, seed, category)
            pause = rng.uniform(max(0.05, pattern_pause*0.5), pattern_pause*1.2)
            # bound pause so simulation continues responsively
            pause = max(0.05, min(10.0, pause))
            stop_event.wait(pause)
        except Exception as e:
            if stop_event.is_set(): break
            stop_event.wait(1)

# ---------------------- VOIP TRAFFIC GENERATOR (SIPp) ----------------------
def generate_voip_calls(client, server_ip, seed, category='voip'):
    rng = random.Random(seed)
    info(f"Starting VoIP generator for {client.name} -> SIP server {server_ip} (seed {seed})\n")

    while not stop_event.is_set():
        hour = int(time.strftime("%H"))

        # VoIP: call duration pendek-menengah, frequency lebih sering saat jam kerja
        call_duration = int(max(2, min(120, daily_pattern(hour, 4, 40, seed, 'voip'))))
        # choose codec/flags if desired (keperluan advance)
        # Use sipp uac -d in ms
        cmd = f"sipp {server_ip} -sn uac -s 1000 -d {call_duration*1000} -p 5060 -trace_err -trace_msg -nd"
        safe_cmd(client, cmd)

        # inter-call interval smaller in busy hours
        base_next = daily_pattern(hour, 2.0, 20.0, seed, 'voip')
        # add randomness but keep some lower bound
        jitter = rng.uniform(0.6, 1.4)
        next_call = max(0.5, min(60.0, base_next * jitter))
        stop_event.wait(next_call)

# ---------------------- START TRAFFIC & SETUP ----------------------
def start_traffic(net):
    # Dapatkan semua host
    user1, user2, user3 = net.get('user1', 'user2', 'user3')
    web1, web2, cache1 = net.get('web1', 'web2', 'cache1')
    app1, db1 = net.get('app1', 'db1')
    all_hosts = [user1, user2, user3, web1, web2, cache1, app1, db1]

    # --- Membuat link namespace (untuk collector atau akses ns) ---
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

    # Starting SIPp UAS (SIP server) on app1 and web1 for redundancy test
    info("\n*** Starting VoIP SIPp servers (UAS)...\n")
    safe_cmd(app1, "sipp -sn uas -i 10.10.2.1 -p 5060 -bg &")
    safe_cmd(web1, "sipp -sn uas -i 10.10.1.1 -p 5060 -bg &")

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
    threads = []
    
    # --------------------- TRAFFIC KATEGORI (Kategori & interval berbeda-beda) ---------------------
    # Arguments: (client_host, server_ip, server_port, base_min_bw, base_max_bw, seed, category)

    # 1. Web Heavy (User1 -> Web1) : peak pada jam kerja
    threads.append(threading.Thread(target=generate_client_traffic,
        args=(user1, '10.10.1.1', 443, 1.5, 8.0, 10001, 'web'), daemon=False))

    # 2. API Moderate (User2 -> App1)
    threads.append(threading.Thread(target=generate_client_traffic,
        args=(user2, '10.10.2.1', 8080, 0.8, 4.0, 10002, 'api'), daemon=False))

    # 3. Light browsing (User3 -> Web2)
    threads.append(threading.Thread(target=generate_client_traffic,
        args=(user3, '10.10.1.2', 80, 0.3, 2.0, 10003, 'web'), daemon=False))

    # 4. East-West Web -> App (internal DC traffic)
    threads.append(threading.Thread(target=generate_client_traffic,
        args=(web1, '10.10.2.1', 8080, 0.5, 3.0, 20001, 'east_west'), daemon=False))

    # 5. East-West Web -> Cache (frekuensi tinggi, burst pendek)
    threads.append(threading.Thread(target=generate_client_traffic,
        args=(web2, '10.10.1.3', 6379, 0.2, 2.5, 20002, 'cache'), daemon=False))

    # 6. East-West App -> DB (less frequent but larger bursts)
    threads.append(threading.Thread(target=generate_client_traffic,
        args=(app1, '10.10.2.2', 5432, 0.8, 6.0, 20003, 'db'), daemon=False))

    # 7. VoIP Calls (CLIENT user1 -> SIP server at web1)
    threads.append(threading.Thread(target=generate_voip_calls,
        args=(user1, '10.10.1.1', 30001, 'voip'), daemon=False))

    # 8. VoIP Calls (CLIENT user2 -> SIP server at app1)
    threads.append(threading.Thread(target=generate_voip_calls,
        args=(user2, '10.10.2.1', 30002, 'voip'), daemon=False))

    # start threads
    for t in threads:
        t.start()
    return threads

# ---------------------- CLEANUP HELPERS ----------------------
def cleanup_processes(net, host_names):
    info("*** Membersihkan proses iperf/sipp yang mungkin tersisa...\n")
    for name in host_names:
        try:
            host = net.get(name)
            if host:
                host.cmd("pkill -f iperf || true")
                host.cmd("pkill -f sipp || true")
                host.cmd("killall -9 iperf || true")
                host.cmd("killall -9 sipp || true")
        except Exception:
            pass

def cleanup_netns(host_names):
    info("*** Membersihkan link network namespace...\n")
    for name in host_names:
        cmd = ['sudo', 'ip', 'netns', 'del', name]
        try:
            subprocess.run(cmd, check=False, capture_output=True)
        except Exception:
            pass

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    topo = DataCenterTopo()
    net = Mininet(topo=topo,
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    info("*** Memulai Jaringan Mininet...\n")
    net.start()

    traffic_threads = start_traffic(net)

    info("\n*** Skrip Simulasi & Traffic berjalan.")
    info("*** 'collector_dc.py' (opsional) dapat dijalankan di terminal terpisah untuk mengumpulkan telemetry.")
    info("*** Tekan Ctrl+C untuk berhenti kapan saja.\n")
    
    def _sigint_handler(sig, frame):
        info("\n\n*** SIGINT diterima. Menghentikan semua proses...\n")
        stop_event.set()
    signal.signal(signal.SIGINT, _sigint_handler)

    try:
        for t in traffic_threads:
            if t.is_alive():
                t.join()
    except KeyboardInterrupt:
        info("\n\n*** KeyboardInterrupt diterima. Menghentikan semua proses...\n")
        stop_event.set()
    finally:
        info("*** Mengirim sinyal stop ke semua thread traffic...\n")
        stop_event.set()

        info("*** Menunggu Traffic threads berhenti...\n")
        for t in traffic_threads:
            if t.is_alive():
                t.join(timeout=5)

        # Update host list for cleanup
        host_names = ['user1', 'user2', 'user3', 'web1', 'web2', 'cache1', 'app1', 'db1']
        cleanup_processes(net, host_names)
        cleanup_netns(host_names)

        info("*** Menghentikan jaringan Mininet...\n")
        try:
            net.stop()
            info("*** Mininet berhenti.\n")
        except Exception as e:
            info(f"*** ERROR saat net.stop(): {e}. Coba cleanup manual 'sudo mn -c'.\n")
        info("*** Selesai. Bye.\n")
