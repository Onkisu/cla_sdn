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
def run_ns_cmd(host, cmd, timeout=2):
    """Menjalankan perintah di dalam namespace host dengan sudo"""
    try:
        # Cek apakah file netns ada dulu untuk menghindari spam error sudo
        if not os.path.exists(f"/var/run/netns/{host}"):
            return None
            
        full = ['sudo', 'ip', 'netns', 'exec', host] + cmd
        out = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return out.stdout
    except Exception:
        return None

def get_iface_stats(host):
    """
    Mencoba membaca statistik interface eth0 di dalam namespace host.
    """
    # Di Mininet, interface di dalam host biasanya bernama 'eth0'
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', 'eth0'])
    
    if not out:
        # Fallback: kadang namanya hX-eth0 (jarang terjadi kalau masuk namespace, tapi antisipasi)
        out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', f"{host}-eth0"])
    
    if not out: 
        return None

    # Parsing Output ip -s link
    rx_match = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx_match = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)

    if rx_match and tx_match:
        rx_bytes = int(rx_match.group(1))
        rx_pkts  = int(rx_match.group(2))
        tx_bytes = int(tx_match.group(1))
        tx_pkts  = int(tx_match.group(2))
        return (tx_bytes, tx_pkts, rx_bytes, rx_pkts)
    
    return None

def get_db_connection():
    try:
        conn = psycopg2.connect(DB_CONN)
        return conn
    except Exception as e:
        print(f"[DB CONNECT ERROR] {e}")
        return None

def insert_db(rows, conn):
    """Memasukkan data ke DB dengan proteksi koneksi"""
    if not rows: return conn
    
    # Reconnect logic
    if conn is None or conn.closed:
        conn = get_db_connection()
        if not conn: return None # Gagal connect, skip cycle ini

    try:
        cur = conn.cursor()
        query = """
            INSERT INTO traffic.flow_stats 
            (timestamp, dpid, host, app, proto, 
             src_ip, dst_ip, src_mac, dst_mac,
             bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms, category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(query, rows)
        conn.commit()
        cur.close()
        return conn
    except Exception as e:
        print(f"[DB INSERT ERROR] {e}")
        # Jika error, coba tutup koneksi biar direconnect cycle depan
        try: conn.close()
        except: pass
        return None

def collector_loop():
    print(f"\n*** Collector ON - Monitoring {len(HOSTS_TO_MONITOR)} Hosts ***")
    conn = get_db_connection()
    
    # Inisialisasi awal (supaya tidak spike di detik pertama)
    print("--- Initializing Baseline Stats ---")
    for host in HOSTS_TO_MONITOR:
        stats = get_iface_stats(host)
        if stats:
            last_tx_bytes[host], last_tx_pkts[host], last_rx_bytes[host], last_rx_pkts[host] = stats

    while not stop_event.is_set():
        start_time = time.time()
        now = datetime.now()
        rows = []
        
        for host in HOSTS_TO_MONITOR:
            stats = get_iface_stats(host)
            
            if stats is None:
                continue 

            cur_tx_bytes, cur_tx_pkts, cur_rx_bytes, cur_rx_pkts = stats
            
            # Hitung Delta (Traffic yang lewat selama Interval)
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

            # Filter Noise: Hanya simpan jika ada TX/RX atau ini VoIP (penting buat keepalive)
            # Menghindari log database penuh angka 0
            if d_tx_b > 0 or d_rx_b > 0 or category == 'voip':
                
                # Visualisasi simple di terminal
                if d_tx_b > 1000:
                    print(f"[{host}] {category.upper()} -> TX: {d_tx_b} Bytes")

                rows.append((
                    now, 1, host, app, meta.get('proto', 'tcp'),
                    meta.get('ip', ''), meta.get('server_ip', ''), 
                    meta.get('mac', ''), meta.get('server_mac', ''),
                    d_tx_b, d_rx_b, d_tx_p, d_rx_p,
                    random_latency(category), category
                ))

        if rows:
            conn = insert_db(rows, conn)
        
        # Sleep presisi
        elapsed = time.time() - start_time
        sleep_time = max(0, COLLECT_INTERVAL - elapsed)
        time.sleep(sleep_time)

    if conn: conn.close()
    print("\n*** Collector STOPPED ***")

def random_latency(category):
    import random
    if category == 'voip': return round(random.uniform(20, 40), 2)
    if category == 'db': return round(random.uniform(0.5, 2.0), 2)
    return round(random.uniform(10, 50), 2)

if __name__ == "__main__":
    if os.geteuid() != 0:
        print("WAJIB jalankan dengan SUDO!")
        sys.exit(1)
    try:
        collector_loop()
    except KeyboardInterrupt:
        stop_event.set()