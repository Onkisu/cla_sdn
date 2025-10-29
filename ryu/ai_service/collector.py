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

print("Memulai collector_AGG_DELTA.py (Delta dari Agregasi Flow per App, 1 poll / 5 detik)...")

# [WAJIB] Penyimpanan global untuk TOTAL KUMULATIF BYTE per APP terakhir
# Kunci: app_name ('youtube', 'netflix', 'twitch')
last_cumulative_bytes_per_app = defaultdict(int)

# [WAJIB] Penyimpanan global untuk TOTAL KUMULATIF PACKET per APP terakhir
# Kunci: app_name
last_cumulative_pkts_per_app = defaultdict(int)

# Mapping APLIKASI ke IP Client (dari topo.py)
APP_TO_HOST_IP = {
    'youtube': '10.0.0.1',
    'netflix': '10.0.0.2',
    'twitch': '10.0.0.3'
}
HOST_IP_TO_APP = {v: k for k, v in APP_TO_HOST_IP.items()}


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

# ---------------------- FUNGSI UTAMA KOLEKSI (AGG DELTA) ----------------------
def collect_current_traffic():
    """
    Mengumpulkan SEMUA flow, meng-AGREGASI total bytes/pkts per APP,
    menghitung DELTA dari agregasi sebelumnya, dan menyimpan DELTA ke DB.
    """
    global last_cumulative_bytes_per_app, last_cumulative_pkts_per_app

    rows_to_insert = []
    has_delta = False

    # [BARU] Wadah sementara untuk total KUMULATIF SAAT INI per App
    current_cumulative_bytes = defaultdict(int)
    current_cumulative_pkts = defaultdict(int)
    # [BARU] Wadah untuk nyimpen detail flow TERAKHIR per App
    latest_flow_details_per_app = {}

    try:
        res_flow = requests.get(f"{RYU_REST}/stats/flow/{DPID_S1}", timeout=5).json()
    except Exception as e:
        print(f"Error fetch FLOW dpid {DPID_S1}: {e}", file=sys.stderr)
        return False, [] # Gagal total kalo nggak bisa ambil flow

    if str(DPID_S1) not in res_flow:
        return False, []

    # === Tahap 1: Iterasi SEMUA flow, AGREGASI per App ===
    for flow in res_flow[str(DPID_S1)]:
        match = flow.get("match", {})
        src_ip = match.get("ipv4_src") or match.get("nw_src")
        
        # Cari tau ini flow app apa (berdasarkan IP client)
        app_name = HOST_IP_TO_APP.get(src_ip)
        
        # Kalo bukan dari h1/h2/h3, skip
        if not app_name:
            continue

        # Agregasi byte & packet
        current_bytes = flow.get("byte_count", 0)
        current_pkts = flow.get("packet_count", 0)
        current_cumulative_bytes[app_name] += current_bytes
        current_cumulative_pkts[app_name] += current_pkts

        # Simpan/Update detail flow terakhir untuk app ini
        dst_ip = match.get("ipv4_dst") or match.get("nw_dst")
        tp_src = int(match.get("tcp_src") or match.get("udp_src") or 0)
        tp_dst = int(match.get("tcp_dst") or match.get("udp_dst") or 0)
        proto = {6:"tcp", 17:"udp"}.get(match.get("ip_proto", 0), "any")
        src_mac = match.get("eth_src")
        dst_mac = match.get("eth_dst")
        
        latest_flow_details_per_app[app_name] = {
             "src_ip": src_ip, "dst_ip": dst_ip, "src_mac": src_mac,
             "dst_mac": dst_mac, "proto": proto, "tp_src": tp_src, "tp_dst": tp_dst
        }

    # === Tahap 2: Hitung DELTA untuk tiap App ===
    ts = datetime.now()
    for app_name in APP_TO_HOST_IP.keys(): # Loop youtube, netflix, twitch
        # Ambil total kumulatif saat ini & sebelumnya
        current_total_bytes = current_cumulative_bytes[app_name]
        last_total_bytes = last_cumulative_bytes_per_app[app_name]
        current_total_pkts = current_cumulative_pkts[app_name]
        last_total_pkts = last_cumulative_pkts_per_app[app_name]

        # Hitung Delta
        delta_bytes = current_total_bytes - last_total_bytes
        delta_pkts = current_total_pkts - last_total_pkts

        # Handle reset counter
        if delta_bytes < 0: delta_bytes = current_total_bytes
        if delta_pkts < 0: delta_pkts = current_total_pkts

        # Update "memori" untuk poll berikutnya
        last_cumulative_bytes_per_app[app_name] = current_total_bytes
        last_cumulative_pkts_per_app[app_name] = current_total_pkts

        # Jika ada perubahan, siapkan data DB
        if delta_bytes > 0 or delta_pkts > 0:
            has_delta = True
            details = latest_flow_details_per_app.get(app_name)
            
            if details:
                 category = app_category_map.get(app_name, "data")
                 latency, loss = get_synthetic_metrics(app_name)
                 host = APP_TO_HOST_IP.get(app_name)
                 pkts_rx = max(0, int(delta_pkts * (1 - loss/100))) 
                 
                 print(f"[AGG DELTA] app={app_name}, delta_bytes={delta_bytes}, delta_pkts={delta_pkts}")

                 rows_to_insert.append((
                    ts, DPID_S1, host, app_name, details["proto"], 
                    details["src_ip"], details["dst_ip"], details["src_mac"], details["dst_mac"],
                    delta_bytes, delta_bytes, # tx/rx diisi delta BYTE
                    delta_pkts, pkts_rx,     # tx diisi delta PKT, rx dihitung
                    latency, category
                 ))
            else:
                 # Harusnya nggak kejadian kalo traffic jalan
                 print(f"[WARNING] No flow details found for {app_name}, skipping DB insert.")


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

# ---------------------- MAIN LOOP (SAMA) ----------------------
if __name__ == "__main__":
    
    while True:
        
        has_traffic_delta, rows = collect_current_traffic()
        
        if has_traffic_delta and rows:
            inserted = insert_pg(rows)
            print(f"{inserted} baris DELTA (AGG DELTA) masuk DB.", file=sys.stderr)
        else:
            print("Tidak ada delta baru (agregasi flow tidak berubah).", file=sys.stderr)
            
        time.sleep(5)
