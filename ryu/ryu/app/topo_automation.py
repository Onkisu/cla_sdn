#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
# from mininet.cli import CLI # <-- Dihapus
from mininet.log import setLogLevel, info
import threading
import random
import time
import subprocess
import math
import re # <-- Modul baru buat parsing output 'ip'
import psycopg2 # <-- Modul baru buat konek DB
from collections import defaultdict # <-- FIX: Tambahkan import ini

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# [WAJIB] Memori untuk total byte TERAKHIR per host (dari 'ip link')
last_host_bytes = defaultdict(int)

# [OPSIONAL] Mapping Aplikasi ke Host (buat di DB)
HOST_TO_APP_MAP = {
    'h1': 'youtube',
    'h2': 'netflix',
    'h3': 'twitch'
}
APP_TO_CATEGORY = { # Ambil dari apps.yaml
    'youtube': 'video',
    'netflix': 'video',
    'twitch': 'video' # Sesuaikan kalo beda
}


# ---------------------- GLOBAL LOCK & STOP EVENT ----------------------
cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd):
    """Execute node.cmd() safely with lock."""
    # Cek event SEBELUM lock (lebih efisien)
    if stop_event.is_set():
        return None
    with cmd_lock:
        # Cek event LAGI DI DALAM lock (buat jaga-jaga race condition kecil)
        if stop_event.is_set():
             return None
        # Tambah timeout biar nggak nge-hang kalo iperf error
        try:
             # Coba jalankan dengan timeout
             return node.cmd(cmd, timeout=10)
        except Exception as e:
             # Tangkap error timeout atau error cmd lainnya
             # info(f"  [safe_cmd Error] Node {node.name}, Cmd: '{cmd[:50]}...': {e}\n") # Kurangi spam log
             return None # Kembalikan None jika error

# ---------------------- ROUTER & TOPOLOGY (SAMA) ----------------------
class LinuxRouter(Node):
    # ... (SAMA PERSIS) ...
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        # Tambah cek stop_event di terminate juga
        if not stop_event.is_set():
            safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class ComplexTopo(Topo):
    # ... (SAMA PERSIS) ...
    def build(self):
        # Router
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # Switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3') # <-- Pastikan 's3' ada (bukan typo 's3B')

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


# ---------------------- RANDOM TRAFFIC (SAMA) ----------------------
# (Fungsi _log_iperf dan generate_client_traffic SAMA PERSIS dari v7.1)
def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    """Helper fungsi logging."""
    if not output: # Cek kalo output None (karena safe_cmd error)
        # info(f"  [iperf Log] No output received for {client_name} -> {server_ip}\n") # Kurangi spam
        return
    try:
        # Ambil baris CSV (hati-hati kalo ada error di output)
        lines = output.strip().split('\n')
        csv_line = None
        for line in reversed(lines): # Cari dari bawah
             if ',' in line and len(line.split(',')) > 7: # Cek minimal ada 8 kolom CSV
                  csv_line = line
                  break # Ketemu baris CSV valid

        if csv_line:
            parts = csv_line.split(',')
            actual_bytes = int(parts[7])
            info(f"CLIENT LOG: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes in {burst_time_str}s (Target BW: {bw_str}ps)\n")
        else:
             # Kalo nggak nemu CSV (misal cuma error "refused")
             info(f"Could not find valid CSV in iperf output for {client_name}:\nOutput was:\n{output}\n")

    except Exception as e:
        info(f"Could not parse iperf output for {client_name}: {e}\nOutput was:\n{output}\n")


def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed):
    """Generates random UDP traffic bursts using a DEDICATED random generator."""
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
            # Log outputnya (udah dihandle kalo None)
            _log_iperf(client.name, server_ip, output, burst_time_str, bw_str)

            # Cek stop_event SEBELUM tidur (biar lebih responsif)
            if stop_event.is_set(): break

            pause_duration = rng.uniform(0.5, 2.0)
            stop_event.wait(pause_duration)
        except Exception as e:
            if stop_event.is_set(): break
            # Kurangi spam error iperf
            # info(f"Error in traffic loop for {client.name}: {e}\n") # Terlalu spam kalo iperf sering error
            stop_event.wait(1) # Tunggu lebih singkat kalo error
    # info(f"Traffic thread for {client.name} stopped.") # Log tambahan


# ---------------------- START TRAFFIC (SAMA, + PINGALL) ----------------------
def start_traffic(net):
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h7 = net.get('h4', 'h5', 'h7') # h6 nggak dipake server

    info("\n*** Starting iperf servers (Simulating services)\n")
    safe_cmd(h4, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h5, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(h7, "iperf -s -u -p 1935 -i 1 &")
    time.sleep(1)

    # --- PENTING: PINGALL (Warm-up) ---
    info("\n*** Warming up network with pingAll...\n")
    try:
         # Tambah net.waitConnected() sebelum pingAll
         info("Waiting for switch <-> controller connection...")
         net.waitConnected()
         info("Connection established. Starting pingAll...")
         net.pingAll(timeout='1') # Timeout 1 detik per ping
         info("*** Warm-up complete.\n")
    except Exception as e:
         info(f"*** Warning: pingAll or waitConnected failed: {e}\n")
    # --- END PINGALL ---

    info("-----------------------------------------------------------\n")
    info("💡 TELEMETRI LIVE (dari server iperf) akan muncul di bawah ini:\n")
    info("-----------------------------------------------------------\n")

    info("\n*** Starting client traffic threads (Simulating users)\n")
    base_range_min = 0.5
    base_range_max = 5.0
    threads = [
        # Set daemon=False biar bisa di-join
        threading.Thread(target=generate_client_traffic, args=(h1, '10.0.1.1', 443, base_range_min, base_range_max, 12345), daemon=False),
        threading.Thread(target=generate_client_traffic, args=(h2, '10.0.1.2', 443, base_range_min, base_range_max, 67890), daemon=False),
        threading.Thread(target=generate_client_traffic, args=(h3, '10.0.2.1', 1935, base_range_min, base_range_max, 98765), daemon=False)
    ]
    for t in threads:
        t.start()

    return threads

# ---------------------- [BARU] FUNGSI KOLEKTOR ----------------------
def get_host_interface_bytes(host):
    """Mendapatkan total TX bytes dari interface eth0 host."""
    try:
        # Jalankan 'ip -s link show eth0' di host
        cmd_output = host.cmd('ip -s link show eth0')

        if cmd_output is None: # Handle jika safe_cmd gagal
             # info(f"  [Collector] Gagal menjalankan 'ip -s link' di {host.name}\n") # Kurangi spam
             return None

        # Cari baris TX bytes pake regex
        match = re.search(r'TX:.*?bytes\s+packets\s+errors.*?(\d+)\s+(\d+)\s+(\d+)', cmd_output, re.DOTALL)
        if match:
            tx_bytes = int(match.group(1))
            return tx_bytes
        else:
            # info(f"  [Collector] Gagal parsing output 'ip -s link' (TX) untuk {host.name}\nOutput:\n{cmd_output}\n") # Kurangi spam
            return None
    except Exception as e:
        # info(f"  [Collector] Exception get bytes for {host.name}: {e}\n") # Kurangi spam
        return None

def run_collector(net):
    """Loop utama collector, jalan tiap COLLECT_INTERVAL."""
    global last_host_bytes

    # Ambil objek host h1, h2, h3
    hosts_to_monitor = [net.get(f'h{i}') for i in range(1, 4)]

    info("\n*** Collector (via Host TX Interface) Dimulai ***\n")

    while not stop_event.is_set():
        ts = datetime.now()
        rows_to_insert = []
        has_delta = False

        for host in hosts_to_monitor:
            host_name = host.name

            # 1. Ambil total byte saat ini dari interface host (TX)
            current_total_bytes = get_host_interface_bytes(host)

            if current_total_bytes is None:
                continue # Gagal ambil data, skip host ini

            # 2. Ambil total byte sebelumnya dari memori
            last_total_bytes = last_host_bytes[host_name]

            # 3. Hitung DELTA
            delta_bytes = current_total_bytes - last_total_bytes

            # Handle reset counter (jarang terjadi di host interface)
            if delta_bytes < 0:
                delta_bytes = current_total_bytes

            # 4. Update memori
            last_host_bytes[host_name] = current_total_bytes

            # 5. Siapkan data DB jika ada delta
            if delta_bytes > 0:
                has_delta = True
                app_name = HOST_TO_APP_MAP.get(host_name, "unknown")
                category = APP_TO_CATEGORY.get(app_name, "data")
                # Kita nggak punya data latency/loss asli, pakai dummy aja
                latency = random.uniform(10, 50)
                loss = random.uniform(0, 1)

                info(f"  [Collector] Host: {host_name}, App: {app_name}, Delta TX Bytes: {delta_bytes}")

                rows_to_insert.append((
                    ts, 1, host.IP(), app_name, "udp", # dpid=1 (s1), host=IP host
                    host.IP(), "server_ip_dummy", host.MAC(), "mac_dummy", # src=host, dst=dummy
                    delta_bytes, delta_bytes, # tx/rx diisi delta_bytes
                    0, 0,                     # pkts_tx/rx diisi 0
                    latency, category
                 ))

        # 6. Masukkan data ke DB
        if has_delta and rows_to_insert:
            inserted = insert_pg(rows_to_insert)
            # info(f"  [Collector] {inserted} baris DELTA (HOST STATS) masuk DB.\n") # Kurangi spam log
        # else:
            # info("  [Collector] Tidak ada delta baru (host stats tidak berubah).\n") # Kurangi spam log

        # 7. Tunggu sebelum poll berikutnya
        # Cek stop event lebih sering
        interrupted = stop_event.wait(COLLECT_INTERVAL)
        if interrupted: # Jika dibangunkan oleh stop_event.set()
             break

    info("\n*** Collector Berhenti ***\n")


# ---------------------- FUNGSI INSERT DB (SAMA) ----------------------
def insert_pg(rows):
    """Memasukkan data agregasi ke PostgreSQL."""
    try:
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        for r in rows:
            cur.execute("""
                INSERT INTO traffic.flow_stats(
                    timestamp, dpid, host, app, proto,
                    src_ip, dst_ip, src_mac, dst_mac,
                    bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms, category
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, r)
        conn.commit()
        cur.close()
        conn.close()
        return len(rows)
    except Exception as e:
        info(f"  [DB Error] {e}\n") # Kasih prefix biar jelas
        return 0

# ---------------------- MAIN (MODIFIKASI) ----------------------
if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)

    info("*** Memulai Jaringan Mininet...\n")
    net.start()

    # Mulai thread traffic generator
    # [FIX] Set daemon=False biar bisa di-join
    traffic_threads = start_traffic(net)

    # [FIX] Mulai thread collector, set daemon=False
    collector_thread = threading.Thread(target=run_collector, args=(net,), daemon=False)
    collector_thread.start()

    # Loop utama (biar skrip nggak langsung mati)
    info("\n*** Skrip Utama & Collector berjalan.")
    info("*** Tekan Ctrl+C untuk berhenti kapan saja.\n")
    try:
        # Tunggu collector thread selesai (dia akan berhenti kalo stop_event diset)
        collector_thread.join() # Tunggu tanpa timeout sampai collector berhenti

    except KeyboardInterrupt:
        info("\n\n*** Ctrl+C diterima. Menghentikan semua proses...\n")

    finally: # Blok ini akan selalu jalan, baik ada Ctrl+C atau tidak
        # Cleanup
        info("*** Mengirim sinyal stop ke semua thread...\n")
        stop_event.set()

        # Tunggu collector thread benar-benar berhenti
        info("*** Menunggu Collector thread berhenti...\n")
        collector_thread.join(timeout=COLLECT_INTERVAL + 2) # Kasih sedikit waktu ekstra
        if collector_thread.is_alive():
             info("*** Peringatan: Collector thread tidak berhenti dengan benar.\n")

        # Tunggu traffic threads berhenti
        info("*** Menunggu Traffic threads berhenti...\n")
        for t in traffic_threads:
            t.join(timeout=5) # Kasih waktu 5 detik per thread
            if t.is_alive():
                 info(f"*** Peringatan: Traffic thread {t.name} tidak berhenti dengan benar.\n")

        # BERSIHKAN SEMUA PROSES iperf DI HOST (PENTING!)
        info("*** Membersihkan proses iperf yang mungkin tersisa...\n")
        for i in range(1, 8): # Loop h1 sampai h7
            host = net.get(f'h{i}')
            if host:
                 host.cmd("killall -9 iperf") # Kill paksa iperf

        # Baru panggil net.stop()
        info("*** Menghentikan jaringan Mininet...\n")
        try:
            net.stop()
            info("*** Mininet berhenti.\n")
        except Exception as e:
             info(f"*** ERROR saat net.stop(): {e}. Coba cleanup manual 'sudo mn -c'.\n")

