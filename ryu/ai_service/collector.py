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

print("Memulai collector_HYBRID.py (Delta Port + Detail Flow, 1 poll / 5 detik)...")

# [WAJIB] Penyimpanan global untuk TOTAL KUMULATIF PORT terakhir
# Kunci: (dpid, port_no)
last_port_stats = defaultdict(lambda: {"rx_bytes": 0})

# [WAJIB] Penyimpanan global untuk TOTAL KUMULATIF FLOW terakhir
# Kunci: (dpid, src_ip, dst_ip, tp_src, tp_dst, proto)
last_flow_stats = defaultdict(lambda: {"bytes": 0, "pkts": 0})


# Mapping APLIKASI ke PORT (dari topo.py)
APP_TO_PORT_MAP = {
    'youtube': 1,
    'netflix': 2,
    'twitch': 3
}
PORT_TO_APP_MAP = {v: k for k, v in APP_TO_PORT_MAP.items()} # Buat lookup sebaliknya
PORT_TO_HOST_IP = {
    1: '10.0.0.1',
    2: '10.0.0.2',
    3: '10.0.0.3',
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

# ---------------------- FUNGSI UTAMA KOLEKSI (HYBRID) ----------------------
def collect_current_traffic():
    """
    Mengumpulkan PORT stats (untuk delta bytes) dan FLOW stats (untuk detail).
    Menggabungkan keduanya untuk data yang lengkap.
    """
    global last_port_stats, last_flow_stats
    
    rows_to_insert = []
    has_delta = False
    port_deltas = {} # Simpan delta bytes per port di sini

    # === Tahap 1: Hitung Delta Bytes dari Port Stats ===
    try:
        res_port = requests.get(f"{RYU_REST}/stats/port/{DPID_S1}", timeout=5).json()
    except Exception as e:
        print(f"Error fetch PORT dpid {DPID_S1}: {e}", file=sys.stderr)
        return False, []
        
    if str(DPID_S1) not in res_port:
        return False, []

    for port_data in res_port[str(DPID_S1)]:
        port_no = port_data.get("port_no")
        if port_no not in PORT_TO_APP_MAP: # Hanya proses port 1, 2, 3
            continue
            
        current_total_bytes = port_data.get("rx_bytes", 0)
        port_key = (DPID_S1, port_no)
        last_total_bytes = last_port_stats[port_key]["rx_bytes"]
        delta_bytes = current_total_bytes - last_total_bytes
        
        if delta_bytes < 0: 
            delta_bytes = current_total_bytes
            
        last_port_stats[port_key]["rx_bytes"] = current_total_bytes
        
        if delta_bytes > 0:
            has_delta = True
            port_deltas[port_no] = delta_bytes # Simpan delta byte port ini
            app_name = PORT_TO_APP_MAP.get(port_no, "unknown")
            print(f"[PORT DELTA] app={app_name} (Port {port_no}), delta_RX_bytes={delta_bytes}")


    # === Tahap 2: Ambil Detail dari Flow Stats ===
    try:
        res_flow = requests.get(f"{RYU_REST}/stats/flow/{DPID_S1}", timeout=5).json()
    except Exception as e:
        print(f"Error fetch FLOW dpid {DPID_S1}: {e}", file=sys.stderr)
        # Kita masih bisa lanjut pake delta bytes aja kalo flow gagal
        res_flow = {} 
        
    flow_details = {} # Simpan detail flow terakhir per port di sini

    if str(DPID_S1) in res_flow:
        # Cari flow yang relevan (dari h1/h2/h3)
        relevant_flows = {}
        for flow in res_flow[str(DPID_S1)]:
            match = flow.get("match", {})
            in_port = match.get("in_port") # Port masuk ke switch
            src_ip = match.get("ipv4_src") or match.get("nw_src")
            
            # Kita hanya peduli flow yang MASUK dari port 1, 2, atau 3
            if in_port in PORT_TO_APP_MAP and src_ip == PORT_TO_HOST_IP.get(in_port):
                 # Jika ada >1 flow di port yg sama, ambil yg byte_count terbesar (paling aktif)
                 if in_port not in relevant_flows or flow.get("byte_count", 0) > relevant_flows[in_port].get("byte_count", 0):
                      relevant_flows[in_port] = flow

        # Proses flow yang relevan untuk hitung delta packet & ambil detail
        for port_no, flow in relevant_flows.items():
             match = flow.get("match", {})
             src_ip = match.get("ipv4_src") or match.get("nw_src")
             dst_ip = match.get("ipv4_dst") or match.get("nw_dst")
             tp_src = int(match.get("tcp_src") or match.get("udp_src") or 0)
             tp_dst = int(match.get("tcp_dst") or match.get("udp_dst") or 0)
             proto = {6:"tcp", 17:"udp"}.get(match.get("ip_proto", 0), "any")
             src_mac = match.get("eth_src")
             dst_mac = match.get("eth_dst")

             # Hitung Delta Packet
             current_total_pkts = flow.get("packet_count", 0)
             flow_key = (DPID_S1, src_ip, dst_ip, tp_src, tp_dst, proto) # Kunci memori flow
             last_total_pkts = last_flow_stats[flow_key]["pkts"]
             delta_pkts = current_total_pkts - last_total_pkts
             if delta_pkts < 0: delta_pkts = current_total_pkts
             last_flow_stats[flow_key]["pkts"] = current_total_pkts

             # Simpan detail flow
             flow_details[port_no] = {
                 "src_ip": src_ip,
                 "dst_ip": dst_ip,
                 "src_mac": src_mac,
                 "dst_mac": dst_mac,
                 "proto": proto,
                 "delta_pkts": delta_pkts,
                 "tp_src": tp_src, # Simpan port utk debug
                 "tp_dst": tp_dst
             }
             print(f"[FLOW DETAIL] Port {port_no}: delta_pkts={delta_pkts}, dst={dst_ip}, src_mac={src_mac}")


    # === Tahap 3: Gabungkan Data & Siapkan untuk DB ===
    ts = datetime.now()
    for port_no, delta_bytes in port_deltas.items():
        app_name = PORT_TO_APP_MAP.get(port_no, "unknown")
        category = app_category_map.get(app_name, "data")
        latency, loss = get_synthetic_metrics(app_name)
        host = PORT_TO_HOST_IP.get(port_no)
        
        # Ambil detail flow jika ada
        details = flow_details.get(port_no)
        if details:
            delta_pkts = details["delta_pkts"]
            pkts_rx = max(0, int(delta_pkts * (1 - loss/100))) # Hitung pkts_rx pake loss sintetis
            
            rows_to_insert.append((
                ts, DPID_S1, host, app_name, details["proto"], 
                details["src_ip"], details["dst_ip"], details["src_mac"], details["dst_mac"],
                delta_bytes, delta_bytes, # tx_bytes dan rx_bytes diisi delta BYTE (dari port)
                delta_pkts, pkts_rx,     # pkts_tx diisi delta PACKET (dari flow), pkts_rx dihitung
                latency, category
            ))
        else:
            # Fallback jika detail flow tidak ditemukan (isi dummy)
             print(f"[WARNING] Flow detail not found for port {port_no}, using dummy values.")
             rows_to_insert.append((
                ts, DPID_S1, host, app_name, "udp", 
                host, "server_ip_dummy", "mac_dummy", "mac_dummy",
                delta_bytes, delta_bytes, 
                0, 0, # pkts = 0
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
            print(f"{inserted} baris DELTA (HYBRID) masuk DB.", file=sys.stderr)
        else:
            print("Tidak ada delta baru (port/flow tidak berubah).", file=sys.stderr)
            
        time.sleep(5)
