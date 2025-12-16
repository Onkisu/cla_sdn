#!/usr/bin/python3
"""
[COLLECTION FIX] Sesuai Format Data Proposal (PDF)
Mengumpulkan data TX/RX terpisah, DPID, dan Throughput Mbps.
"""

import psycopg2
import subprocess
import time
import re
from datetime import datetime
from collections import defaultdict

# --- DB CONFIG ---
DB_CONFIG = {
    'dbname': 'development', 'user': 'dev_one', 'password': 'hijack332.', 'host': '103.181.142.165'
}

# --- DEFINISI TARGET MONITORING (Sesuai Simulation) ---
MONITOR_FLOWS = [
    {
        # VoIP Flow
        'src_host': 'h1', 'dst_host': 'h3',
        'src_ip': '10.0.0.1', 'dst_ip': '10.0.0.3',
        'src_mac': '00:00:00:00:00:01', 'dst_mac': '00:00:00:00:00:03',
        'tp_src': 3000, 'tp_dst': 5060,
        'ip_proto': 17, # 17 = UDP (Integer sesuai PDF)
        'dpid': '0000000000000011', # Leaf 1 (Tempat H1 terhubung)
        'label': 'VoIP'
    },
    {
        # Background Flow
        'src_host': 'h2', 'dst_host': 'h4',
        'src_ip': '10.0.0.2', 'dst_ip': '10.0.0.4',
        'src_mac': '00:00:00:00:00:02', 'dst_mac': '00:00:00:00:00:04',
        'tp_src': 4000, 'tp_dst': 80,
        'ip_proto': 6, # 6 = TCP (Integer sesuai PDF)
        'dpid': '0000000000000011', # Leaf 1 (Tempat H2 terhubung)
        'label': 'Background'
    }
]

# State Tracking
prev_stats = defaultdict(lambda: {'tx_b': 0, 'rx_b': 0, 'tx_p': 0, 'rx_p': 0, 'ts': 0})

def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Hapus tabel jika ada agar struktur baru bisa dibuat
    # cur.execute("DROP TABLE IF EXISTS design_data_ta") 
    
    # MEMBUAT TABEL SESUAI PDF 'format_data(design data).pdf'
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic.flow_stats_ (
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
    print("[DB] Tabel 'design_data_ta' siap sesuai PDF.")

def get_host_stats(host_name):
    """
    Mengambil data TX dari Host Pengirim dan RX dari Host Penerima
    menggunakan perintah 'ip -s link'.
    Ini memberikan data 'pkts_tx' dan 'pkts_rx' yang akurat.
    """
    cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ip', '-s', 'link', 'show', 'eth0']
    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        out = res.stdout
        # Parsing Output IP Link
        # RX: bytes  packets ...
        # TX: bytes  packets ...
        rx_match = re.search(r'RX:.*?\n\s+(\d+)\s+(\d+)', out, re.MULTILINE)
        tx_match = re.search(r'TX:.*?\n\s+(\d+)\s+(\d+)', out, re.MULTILINE)
        
        rx_bytes, rx_pkts = 0, 0
        tx_bytes, tx_pkts = 0, 0
        
        if rx_match:
            rx_bytes = int(rx_match.group(1))
            rx_pkts = int(rx_match.group(2))
        if tx_match:
            tx_bytes = int(tx_match.group(1))
            tx_pkts = int(tx_match.group(2))
            
        return tx_bytes, tx_pkts, rx_bytes, rx_pkts
    except:
        return 0, 0, 0, 0

def collection_loop():
    print("*** Memulai Koleksi Data (Format PDF)...")
    
    while True:
        start_ts = time.time()
        now = datetime.now()
        batch = []
        
        print(f"\n--- {now.strftime('%H:%M:%S')} ---")
        
        for f in MONITOR_FLOWS:
            flow_key = f"{f['src_ip']}->{f['dst_ip']}"
            
            # 1. Ambil Stats dari Source Host (Untuk TX)
            tx_b_src, tx_p_src, _, _ = get_host_stats(f['src_host'])
            
            # 2. Ambil Stats dari Dest Host (Untuk RX)
            _, _, rx_b_dst, rx_p_dst = get_host_stats(f['dst_host'])
            
            # 3. Hitung Delta (Perubahan detik ini)
            prev = prev_stats[flow_key]
            delta_t = start_ts - prev['ts']
            
            d_tx_b = max(0, tx_b_src - prev['tx_b'])
            d_rx_b = max(0, rx_b_dst - prev['rx_b'])
            d_tx_p = max(0, tx_p_src - prev['tx_p'])
            d_rx_p = max(0, rx_p_dst - prev['rx_p'])
            
            throughput_mbps = 0.0
            
            if prev['ts'] > 0 and delta_t > 0:
                # Rumus Throughput (Mbps) = (Bytes * 8) / (Time * 1,000,000)
                throughput_mbps = (d_tx_b * 8) / (delta_t * 1000000)
                
                # Logika Label (Sesuai Proposal: Burst Detection)
                label = f['label']
                if f['label'] == 'Background' and throughput_mbps > 5.0:
                    label = "MicroBurst"
                
                print(f"[{label}] {f['src_ip']}->{f['dst_ip']} | TX: {d_tx_b} B | RX: {d_rx_b} B | Tput: {throughput_mbps:.2f} Mbps")
                
                # Masukkan ke list Batch
                batch.append((
                    now,
                    f['dpid'],
                    f['src_ip'], f['dst_ip'],
                    f['src_mac'], f['dst_mac'],
                    f['ip_proto'],
                    f['tp_src'], f['tp_dst'],
                    d_tx_b, d_rx_b,    # bytes_tx, bytes_rx
                    d_tx_p, d_rx_p,    # pkts_tx, pkts_rx
                    delta_t,           # duration_sec
                    throughput_mbps,
                    label
                ))

            # Update State
            prev_stats[flow_key] = {
                'tx_b': tx_b_src, 'rx_b': rx_b_dst,
                'tx_p': tx_p_src, 'rx_p': rx_p_dst,
                'ts': start_ts
            }

        # 4. Insert Batch ke DB
        if batch:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                sql = """INSERT INTO traffic.flow_stats_
                         (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                          ip_proto, tp_src, tp_dst, 
                          bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                          duration_sec, throughput_mbps, traffic_label)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(sql, batch)
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"DB Error: {e}")
        
        # Jaga interval 1 detik
        time.sleep(1.0)

if __name__ == '__main__':
    init_db()
    try:
        collection_loop()
    except KeyboardInterrupt:
        print("\nStop.")