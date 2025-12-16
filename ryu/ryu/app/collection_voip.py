#!/usr/bin/python3
"""
[COLLECTION FINAL] Sesuai Format Data Proposal (PDF)
Fitur:
- Memisahkan TX (Sender) dan RX (Receiver)
- Menghitung Throughput dalam Mbps
- Mencatat Protocol sebagai Integer (UDP=17, TCP=6)
- Mendeteksi Label berdasarkan Threshold Throughput
"""

import psycopg2
import subprocess
import time
import re
from datetime import datetime
from collections import defaultdict

# --- KONFIGURASI DATABASE ---
# Sesuaikan password jika perlu
DB_CONFIG = {
    'dbname': 'development', 
    'user': 'dev_one', 
    'password': 'hijack332.', 
    'host': '103.181.142.165'
}

# --- KONFIGURASI TARGET MONITORING ---
# Harus COCOK dengan simulation_main.py & background_traffic.py
MONITOR_FLOWS = [
    {
        # Flow 1: VoIP (Stabil)
        'label_base': 'VoIP',
        'src_host': 'h1', 'dst_host': 'h3',
        'src_ip': '10.0.0.1', 'dst_ip': '10.0.0.3',
        'src_mac': '00:00:00:00:00:01', 'dst_mac': '00:00:00:00:00:03',
        'tp_src': 3000, 'tp_dst': 5060,
        'ip_proto': 17, # UDP
        'dpid': '0000000000000011' # Leaf 1
    },
    {
        # Flow 2: Background (Dinamis/Serangan)
        'label_base': 'Background',
        'src_host': 'h2', 'dst_host': 'h4',
        'src_ip': '10.0.0.2', 'dst_ip': '10.0.0.4',
        'src_mac': '00:00:00:00:00:02', 'dst_mac': '00:00:00:00:00:04',
        'tp_src': 4000, 'tp_dst': 80,
        'ip_proto': 6, # TCP
        'dpid': '0000000000000011' # Leaf 1
    }
]

# Variabel untuk menyimpan state sebelumnya (untuk hitung delta per detik)
# Format: key -> {'tx_b': 0, 'rx_b': 0, 'tx_p': 0, 'rx_p': 0, 'ts': 0}
prev_stats = defaultdict(lambda: {'tx_b': 0, 'rx_b': 0, 'tx_p': 0, 'rx_p': 0, 'ts': 0})

def init_db():
    """Membuat Tabel Database Sesuai PDF"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Hapus tabel lama agar struktur baru (sesuai PDF) terbentuk
    # Hati-hati: Data lama akan hilang. Uncomment jika ingin reset total.
    # cur.execute("DROP TABLE IF EXISTS design_data_ta") 
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_flow_stats_ (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            dpid TEXT,
            src_ip TEXT,
            dst_ip TEXT,
            src_mac TEXT,
            dst_mac TEXT,
            ip_proto INTEGER,
            tp_src INTEGER,
            tp_dst INTEGER,
            bytes_tx BIGINT,
            bytes_rx BIGINT,
            pkts_tx BIGINT,
            pkts_rx BIGINT,
            duration_sec FLOAT,
            throughput_mbps FLOAT,
            traffic_label TEXT
        );
    """)
    conn.commit()
    conn.close()
    print("[DB] Tabel 'design_data_ta' siap (Struktur Sesuai PDF).")

def get_host_stats(host_name):
    """
    Mengambil statistik Interface langsung dari Host Mininet (Lebih Akurat untuk TX/RX End-to-End)
    Return: tx_bytes, tx_packets, rx_bytes, rx_packets
    """
    cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ip', '-s', 'link', 'show', 'eth0']
    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        output = res.stdout
        
        # Regex untuk menangkap baris statistik IP Link
        rx_match = re.search(r'RX:.*?\n\s+(\d+)\s+(\d+)', output, re.MULTILINE)
        tx_match = re.search(r'TX:.*?\n\s+(\d+)\s+(\d+)', output, re.MULTILINE)
        
        rx_bytes, rx_pkts = 0, 0
        tx_bytes, tx_pkts = 0, 0
        
        if rx_match:
            rx_bytes = int(rx_match.group(1))
            rx_pkts = int(rx_match.group(2))
        if tx_match:
            tx_bytes = int(tx_match.group(1))
            tx_pkts = int(tx_match.group(2))
            
        return tx_bytes, tx_pkts, rx_bytes, rx_pkts
    except Exception as e:
        print(f"Error reading stats form {host_name}: {e}")
        return 0, 0, 0, 0

def collection_loop():
    print("*** Memulai Monitoring Trafik Real-time ***")
    print("Mencatat ke DB: design_data_ta")
    
    while True:
        start_loop = time.time()
        timestamp_now = datetime.now()
        batch_data = []
        
        print(f"\n--- {timestamp_now.strftime('%H:%M:%S')} ---")
        
        for f in MONITOR_FLOWS:
            # Identifier Unik untuk Flow ini
            flow_key = f"{f['src_ip']}->{f['dst_ip']}"
            
            # 1. Ambil Data TX dari Pengirim (Source)
            tx_b, tx_p, _, _ = get_host_stats(f['src_host'])
            
            # 2. Ambil Data RX dari Penerima (Destination)
            _, _, rx_b, rx_p = get_host_stats(f['dst_host'])
            
            # 3. Hitung Selisih (Delta) dari detik sebelumnya
            prev = prev_stats[flow_key]
            delta_time = start_loop - prev['ts']
            
            # Mencegah nilai negatif saat awal mulai
            d_tx_b = max(0, tx_b - prev['tx_b'])
            d_rx_b = max(0, rx_b - prev['rx_b'])
            d_tx_p = max(0, tx_p - prev['tx_p'])
            d_rx_p = max(0, rx_p - prev['rx_p'])
            
            throughput_mbps = 0.0
            final_label = f['label_base']
            
            # Pastikan bukan iterasi pertama & waktu valid
            if prev['ts'] > 0 and delta_time > 0:
                
                # Rumus Mbps: (Bytes * 8 bit) / (Waktu * 1 Juta)
                throughput_mbps = (d_tx_b * 8) / (delta_time * 1000000)
                
                # --- LOGIKA LABEL DINAMIS (PENTING UNTUK ML) ---
                # Jika trafik Background melonjak tinggi, labeli sebagai MicroBurst
                if f['label_base'] == 'Background':
                    if throughput_mbps > 8.0: # Ambang batas burst (bisa disesuaikan)
                        final_label = "MicroBurst"
                    else:
                        final_label = "Background_Normal"
                
                # Tampilkan di Layar
                print(f"[{final_label:^15}] {f['src_ip']}->{f['dst_ip']} | "
                      f"Load: {throughput_mbps:.2f} Mbps | TX: {d_tx_p} pkts")
                
                # Siapkan data untuk DB (Sesuai Urutan Kolom PDF)
                batch_data.append((
                    timestamp_now,
                    f['dpid'],
                    f['src_ip'], f['dst_ip'],
                    f['src_mac'], f['dst_mac'],
                    f['ip_proto'],
                    f['tp_src'], f['tp_dst'],
                    d_tx_b, d_rx_b,    # bytes_tx, bytes_rx
                    d_tx_p, d_rx_p,    # pkts_tx, pkts_rx
                    delta_time,        # duration_sec
                    throughput_mbps,
                    final_label
                ))
            
            # Simpan state sekarang untuk iterasi berikutnya
            prev_stats[flow_key] = {
                'tx_b': tx_b, 'rx_b': rx_b,
                'tx_p': tx_p, 'rx_p': rx_p,
                'ts': start_loop
            }

        # 4. Simpan ke Database
        if batch_data:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                sql = """INSERT INTO traffic_flow_stats_ 
                         (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                          ip_proto, tp_src, tp_dst, 
                          bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                          duration_sec, throughput_mbps, traffic_label) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(sql, batch_data)
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Database Error: {e}")
        
        # Sleep sisa waktu agar loop berjalan tiap 1 detik
        elapsed = time.time() - start_loop
        sleep_time = max(0, 1.0 - elapsed)
        time.sleep(sleep_time)

if __name__ == '__main__':
    # Tunggu sebentar agar Mininet siap sepenuhnya
    time.sleep(1)
    
    try:
        init_db()
        collection_loop()
    except KeyboardInterrupt:
        print("\n[STOP] Monitoring dihentikan user.")