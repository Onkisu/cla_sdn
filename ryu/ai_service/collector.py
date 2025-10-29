#!/usr/bin/python3
import requests
import psycopg2
import time
import sys
import socket
import yaml
import json
import os
import ipaddress
import random
from datetime import datetime
from collections import defaultdict

# ---------------------- KONFIGURASI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPID_S1 = 1 # DPID 's1'

print("Memulai collector_FINAL.py (Delta per PORT RX, 1 poll / 5 detik)...")

# [WAJIB] Penyimpanan global untuk TOTAL KUMULATIF PORT terakhir
# Kunci: (dpid, port_no)
last_port_stats = defaultdict(lambda: {"tx_bytes": 0, "rx_bytes": 0})

# Mapping APLIKASI ke PORT (dari topo.py)
# h1 -> s1-eth1 (Port 1)
# h2 -> s1-eth2 (Port 2)
# h3 -> s1-eth3 (Port 3)
APP_TO_PORT_MAP = {
    'youtube': 1,
    'netflix': 2,
    'twitch': 3
}

# ... (Kode apps.yaml, caching, dan metrics SAMA PERSIS) ...
try:
    with open("apps.yaml") as f:
        apps_conf = yaml.safe_load(f)
except FileNotFoundError:
    print("ERROR: File apps.yaml tidak ditemukan!", file=sys.stderr)
    sys.exit(1)

app_category_map = {}
for cat, catconf in apps_conf.get("categories", {}).items():
    for app in catconf.get("apps", []):
        app_category_map[app] = cat

CACHE_FILE = "ip_cache.json"
CACHE_EXPIRE = 3600
app_latency_ranges = {
    "youtube": (20, 60), "netflix": (25, 70), "twitch": (15, 50),
    "zoom": (10, 30), "skype": (10, 30), "unknown": (30, 100)
}
app_loss_ranges = {
    "youtube": (0, 3), "netflix": (0, 5), "twitch": (0, 2),
    "zoom": (0, 1), "skype": (0, 1), "unknown": (0, 10)
}

# ---------------------- FUNGSI PEMBANTU (SAMA) ----------------------
def get_synthetic_metrics(app):
    lat_range = app_latency_ranges.get(app, app_latency_ranges["unknown"])
    loss_range = app_loss_ranges.get(app, app_loss_ranges["unknown"])
    return random.uniform(*lat_range), random.uniform(*loss_range)

# ---------------------- FUNGSI UTAMA KOLEKSI (FIXED) ----------------------
def collect_current_traffic():
    """
    Mengumpulkan lalu lintas PORT, menghitung DELTA (perubahan) per PORT,
    dan memasukkannya ke DB.
    """
    global last_port_stats
    
    rows_to_insert = []
    has_delta = False
    
    try:
        # 1. Ambil data port stats dari dpid 1 (s1)
        res = requests.get(f"{RYU_REST}/stats/port/{DPID_S1}", timeout=5).json()
    except Exception as e:
        print(f"Error fetch dpid {DPID_S1}: {e}", file=sys.stderr)
        return False, []
        
    if str(DPID_S1) not in res:
        return False, []

    ts = datetime.now()

    # 2. Iterasi data port stats
    for port_data in res[str(DPID_S1)]:
        port_no = port_data.get("port_no")
        
        # 3. Cari app_name berdasarkan port_no
        app_name = None
        for app, port in APP_TO_PORT_MAP.items():
            if port == port_no:
                app_name = app
                break
        
        if not app_name:
            continue
            
        # --- [FIX SAPU JAGAT] ---
        # Kita ambil 'rx_bytes' (Received by Switch)
        # Ini adalah data UPLOAD dari host (h1, h2, h3)
        current_total_bytes = port_data.get("rx_bytes", 0)
        
        port_key = (DPID_S1, port_no)
        
        # Ambil 'rx_bytes' dari memori
        last_total_bytes = last_port_stats[port_key]["rx_bytes"]
        
        delta_bytes = current_total_bytes - last_total_bytes
        
        if delta_bytes < 0: 
            delta_bytes = current_total_bytes
            
        # Simpan 'rx_bytes' ke memori
        last_port_stats[port_key]["rx_bytes"] = current_total_bytes
        
        # --- SELESAI FIX ---
        
        if delta_bytes > 0:
            has_delta = True

        category = app_category_map.get(app_name, "data")
        latency, loss = get_synthetic_metrics(app_name)
        host = f"10.0.0.{port_no}" # h1, h2, h3
        
        print(f"[PORT SNAPSHOT] app={app_name} (Port {port_no}), delta_RX_bytes={delta_bytes}")
        
        rows_to_insert.append((
            ts, DPID_S1, host, app_name, "udp", 
            host, "server_ip_dummy", "mac_dummy", "mac_dummy",
            delta_bytes, delta_bytes, # tx_bytes dan rx_bytes diisi delta
            0, 0, # pkts (kita nggak dapet dari /stats/port, isi 0)
            latency, category
        ))

    return has_delta, rows_to_insert

# ---------------------- FUNGSI INSERT (SAMA) ----------------------
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
        print(f"DB error: {e}", file=sys.stderr)
        return 0
        
# ---------------------- MAIN LOOP (FIXED) ----------------------

if __name__ == "__main__":
    
    while True:
        
        has_traffic_delta, rows = collect_current_traffic()
        
        if has_traffic_delta and rows:
            inserted = insert_pg(rows)
            print(f"{inserted} baris DELTA (PORT STATS) masuk DB.", file=sys.stderr)
        else:
            print("Tidak ada delta baru (port tidak berubah).", file=sys.stderr)
            
        time.sleep(5)
