#!/usr/bin/python3
import psycopg2
import re
import time
import subprocess
import sys
import os
from collections import defaultdict
from datetime import datetime
import threading

# Import konfigurasi
try:
    from shared_config import DB_CONN, COLLECT_INTERVAL, HOSTS_TO_MONITOR, HOST_INFO, APP_TO_CATEGORY
except ImportError:
    print("[CRITICAL] File shared_config.py tidak ditemukan!")
    sys.exit(1)

# ---------------------- STATE GLOBAL ----------------------
last_tx_bytes = defaultdict(int)
last_rx_bytes = defaultdict(int)
last_tx_pkts  = defaultdict(int)
last_rx_pkts  = defaultdict(int)

stop_event = threading.Event()

# ---------------------- UTILITY ----------------------
def run_ns_cmd(host, cmd, timeout=4):
    """Menjalankan perintah di dalam namespace host dengan sudo"""
    try:
        full = ['sudo', 'ip', 'netns', 'exec', host] + cmd
        out = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return out.stdout
    except Exception as e:
        return ""

def get_iface_stats(host):
    """
    Mencoba membaca statistik interface.
    Strategi: Coba 'eth0' dulu. Jika gagal, coba '{host}-eth0'.
    """
    # 1. Coba interface standar 'eth0'
    target_iface = 'eth0'
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', target_iface])
    
    # 2. Jika output kosong atau error, coba nama spesifik '{host}-eth0'
    #    (Contoh: voip1-eth0 sesuai hasil debug Anda)
    if "does not exist" in out or "Cannot find device" in out or not out.strip():
        target_iface = f"{host}-eth0"
        out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', target_iface])

    # 3. Parsing Output
    if not out: return None

    # Regex disesuaikan dengan output 'ip -s link' Anda:
    # Baris 1: ... state UP ...
    # Baris 2: ... link/ether ...
    # Baris 3: RX: bytes packets ...
    # Baris 4: <spasi> 10962 181 ...
    
    rx_match = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx_match = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)

    if rx_match and tx_match:
        rx_bytes = int(rx_match.group(1))
        rx_pkts  = int(rx_match.group(2))
        tx_bytes = int(tx_match.group(1))
        tx_pkts  = int(tx_match.group(2))
        return (tx_bytes, tx_pkts, rx_bytes, rx_pkts)
    
    return None

def insert_db(rows, conn):
    """Memasukkan data ke DB"""
    if not rows: return conn
    try:
        if conn is None or conn.closed:
            conn = psycopg2.connect(DB_CONN)
        
        cur = conn.cursor()
        query = """
            INSERT INTO network_traffic 
            (timestamp, tenant_id, source_host, app_name, protocol, 
             source_ip, dest_ip, source_mac, dest_mac,
             tx_bytes, rx_bytes, tx_pkts, rx_pkts, latency, category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(query, rows)
        conn.commit()
        cur.close()
        return conn
    except Exception as e:
        print(f"[DB ERROR] {e}")
        return None

def collector_loop():
    print(f"\n*** Collector ON - Monitoring: {HOSTS_TO_MONITOR} ***")
    conn = None
    
    while not stop_event.is_set():
        now = datetime.now()
        rows = []
        
        for host in HOSTS_TO_MONITOR:
            stats = get_iface_stats(host)
            
            if stats is None:
                # Diam saja agar tidak spam error, mungkin namespace belum siap
                continue 

            cur_tx_bytes, cur_tx_pkts, cur_rx_bytes, cur_rx_pkts = stats
            
            # Hitung Delta
            d_tx_b = cur_tx_bytes - last_tx_bytes[host]
            d_rx_b = cur_rx_bytes - last_rx_bytes[host]
            d_tx_p = cur_tx_pkts  - last_tx_pkts[host]
            d_rx_p = cur_rx_pkts  - last_rx_pkts[host]

            # Update nilai terakhir
            last_tx_bytes[host] = cur_tx_bytes
            last_rx_bytes[host] = cur_rx_bytes
            last_tx_pkts[host]  = cur_tx_pkts
            last_rx_pkts[host]  = cur_rx_pkts

            # Ambil Metadata
            meta = HOST_INFO.get(host, {})
            app = meta.get('app', 'unknown')
            category = APP_TO_CATEGORY.get(app, 'other')

            # Logika Insert: Simpan jika ada trafik ATAU jika ini VoIP (keepalive monitoring)
            # Kita set latency dummy 0.5ms karena kita pakai netns stats (bukan ping)
            if d_tx_b > 0 or d_rx_b > 0 or category == 'voip':
                
                print(f"[{host}] {category.upper()} TX:{d_tx_b} B") 

                rows.append((
                    now, 1, host, app, meta.get('proto', 'tcp'),
                    meta.get('ip', ''), meta.get('server_ip', ''), 
                    meta.get('mac', ''), meta.get('server_mac', ''),
                    d_tx_b, d_rx_b, d_tx_p, d_rx_p,
                    0.5, category
                ))

        if rows:
            conn = insert_db(rows, conn)
        
        time.sleep(COLLECT_INTERVAL)

    if conn: conn.close()
    print("\n*** Collector STOPPED ***")

if __name__ == "__main__":
    if os.geteuid() != 0:
        print("WAJIB jalankan dengan SUDO!")
        sys.exit(1)
    try:
        collector_loop()
    except KeyboardInterrupt:
        stop_event.set()