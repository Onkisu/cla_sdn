#!/usr/bin/python3
"""
[COLLECTION DEBUG MODE]
Script ini akan:
1. Mencari nama interface yang BENAR (misal: h1-eth0).
2. Memastikan data IP/MAC ada sebelum dikirim.
3. Menampilkan apa yang akan dikirim ke DB di terminal.
"""

import psycopg2
import subprocess
import time
import sys
from datetime import datetime

# --- KONFIGURASI DATABASE ---
DB_CONFIG = {
    'dbname': 'development', 
    'user': 'dev_one', 
    'password': 'hijack332.', 
    'host': '103.181.142.165'
}

# --- DEFINISI FLOW ---
MONITOR_FLOWS = [
    {
        'label_base': 'VoIP',
        'src_host': 'h1', 'dst_host': 'h3',
        'src_ip': '10.0.0.1', 'dst_ip': '10.0.0.3',
        'src_mac': '00:00:00:00:00:01', 'dst_mac': '00:00:00:00:00:03',
        'proto': 17, 'tp_src': 0, 'tp_dst': 5060 
    },
    {
        'label_base': 'Background',
        'src_host': 'h2', 'dst_host': 'h4',
        'src_ip': '10.0.0.2', 'dst_ip': '10.0.0.4',
        'src_mac': '00:00:00:00:00:02', 'dst_mac': '00:00:00:00:00:04',
        'proto': 6, 'tp_src': 0, 'tp_dst': 5001 
    }
]

# Cache nama interface agar tidak berat
IFACE_CACHE = {}

def get_real_interface(host_name):
    """Mencari nama interface asli di dalam namespace host (misal: h1-eth0)"""
    if host_name in IFACE_CACHE:
        return IFACE_CACHE[host_name]
    
    try:
        # List folder di /sys/class/net/ untuk melihat interface apa yang ada
        cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ls', '/sys/class/net/']
        res = subprocess.run(cmd, capture_output=True, text=True)
        interfaces = res.stdout.strip().split()
        
        # Ambil interface yang bukan 'lo' (loopback)
        # Biasanya: ['h1-eth0', 'lo'] -> kita ambil 'h1-eth0'
        real_iface = next((x for x in interfaces if x != 'lo'), None)
        
        if not real_iface:
            print(f"[WARNING] Tidak ditemukan interface di {host_name}, memaksa 'eth0'")
            real_iface = 'eth0'
            
        print(f"[INFO] Host {host_name} menggunakan interface: {real_iface}")
        IFACE_CACHE[host_name] = real_iface
        return real_iface
    except Exception as e:
        print(f"[ERROR] Gagal deteksi interface {host_name}: {e}")
        return 'eth0'

def get_host_stats_kernel(host_name):
    # Gunakan nama interface yang sudah dideteksi otomatis
    iface = get_real_interface(host_name)
    
    try:
        def read_file(metric):
            # Path dinamis sesuai nama interface
            cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'cat', f'/sys/class/net/{iface}/statistics/{metric}']
            res = subprocess.run(cmd, capture_output=True, text=True)
            val = res.stdout.strip()
            return int(val) if val.isdigit() else 0

        tx_bytes = read_file('tx_bytes')
        tx_pkts  = read_file('tx_packets')
        rx_bytes = read_file('rx_bytes')
        rx_pkts  = read_file('rx_packets')
        
        return tx_bytes, tx_pkts, rx_bytes, rx_pkts

    except Exception as e:
        print(f"Error reading stats: {e}")
        return 0, 0, 0, 0

def main():
    print("--- MEMULAI MONITORING DEBUG MODE ---")
    
    # Tes Koneksi DB Dulu
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("[OK] Koneksi Database Berhasil.")
        conn.close()
    except Exception as e:
        print(f"[FATAL] Gagal konek database: {e}")
        sys.exit(1)

    prev_stats = {}

    while True:
        start_loop = time.time()
        timestamp = datetime.now()
        batch_data = []

        print(f"\n--- WAKTU: {timestamp.strftime('%H:%M:%S')} ---")

        for flow in MONITOR_FLOWS:
            # Ambil Stats
            tx_b_curr, tx_p_curr, _, _ = get_host_stats_kernel(flow['src_host'])
            _, _, rx_b_curr, rx_p_curr = get_host_stats_kernel(flow['dst_host'])

            flow_key = flow['label_base']

            if flow_key in prev_stats:
                prev = prev_stats[flow_key]
                time_diff = start_loop - prev['ts']
                
                if time_diff > 0:
                    bytes_tx_diff = tx_b_curr - prev['tx_b']
                    bytes_rx_diff = rx_b_curr - prev['rx_b']
                    pkts_tx_diff  = tx_p_curr - prev['tx_p']
                    pkts_rx_diff  = rx_p_curr - prev['rx_p']
                    
                    throughput_mbps = (bytes_rx_diff * 8) / 1000000.0 / time_diff
                    throughput_mbps = max(0.0, throughput_mbps)

                    # --- DEBUG DISPLAY (Lihat ini di terminal!) ---
                    print(f"CAPTURED -> Label: {flow['label_base']} | SRC_IP: {flow['src_ip']} | DST_IP: {flow['dst_ip']} | TX Bytes: {bytes_tx_diff}")
                    
                    # Data Tuple
                    row = (
                        timestamp, "0000000000000001", 
                        flow['src_ip'], flow['dst_ip'],
                        flow['src_mac'], flow['dst_mac'],
                        flow['proto'], flow['tp_src'], flow['tp_dst'],
                        bytes_tx_diff, bytes_rx_diff,
                        pkts_tx_diff, pkts_rx_diff,
                        time_diff, throughput_mbps, flow['label_base']
                    )
                    batch_data.append(row)

            prev_stats[flow_key] = {
                'tx_b': tx_b_curr, 'rx_b': rx_b_curr,
                'tx_p': tx_p_curr, 'rx_p': rx_p_curr,
                'ts': start_loop
            }

        # Simpan ke DB
        if batch_data:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                
                # Query INSERT standar
                sql = """INSERT INTO design_data_ta
                         (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                          ip_proto, tp_src, tp_dst, 
                          bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                          duration_sec, throughput_mbps, traffic_label) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                
                cur.executemany(sql, batch_data)
                conn.commit()
                conn.close()
                print("[SUCCESS] Data terkirim ke DB.")
            except Exception as e:
                print(f"[DB ERROR] Gagal Insert: {e}")
                print("Cek apakah jumlah kolom di tabel 'design_data_ta' sesuai dengan 16 parameter script!")

        # Sleep
        elapsed = time.time() - start_loop
        time.sleep(max(0, 1.0 - elapsed))

if __name__ == '__main__':
    main()