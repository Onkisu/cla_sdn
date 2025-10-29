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
DPIDS = [1, 2, 3]

print("Memulai collector_FIXED_v2.py (Hardcoded IP Match, 1 poll / 5 detik)...")


# [WAJIB] Penyimpanan global untuk TOTAL KUMULATIF terakhir
# Kunci: (dpid, src_ip, dst_ip, tp_src, tp_dst, proto)
last_flow_stats = defaultdict(lambda: {"bytes": 0, "pkts": 0})

# Penyimpanan global untuk agregasi DELTA (perubahan)
# Kunci: (dpid, app_name, category)
traffic_aggregator = defaultdict(lambda: {
    "bytes_tx": 0,
    "bytes_rx": 0, 
    "pkts_tx": 0,
    "pkts_rx": 0,
    "latency_sum": 0,
    "loss_sum": 0,
    "count": 0,
    "src_ip": "",
    "dst_ip": "",
    "src_mac": "",
    "dst_mac": "",
    "proto": "any"
})

# Peta untuk resolusi IP/MAC dan Aplikasi
ip_mac_map = {}
# [FIX] Kita udah nggak pake 'match_app', jadi 2 baris ini bisa dihapus/diabaikan
ip_app_map = {}   
port_app_map = {} 

# ... (Kode apps.yaml, caching, dan metrics SAMA PERSIS) ...
try:
    with open("apps.yaml") as f:
        apps_conf = yaml.safe_load(f)
except FileNotFoundError:
    print("ERROR: File apps.yaml tidak ditemukan!", file=sys.stderr)
    sys.exit(1)

# [FIX] Kita tetap butuh 'app_category_map'
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
def get_controller_mappings():
    try:
        response = requests.get(f"{RYU_REST}/ip_mac_map", timeout=3)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Gagal mendapatkan pemetaan kontroler: {e}", file=sys.stderr)
    return {}

def load_cache():
    global ip_app_map, port_app_map
    if not os.path.exists(CACHE_FILE): return False
    try:
        with open(CACHE_FILE, "r") as f: data = json.load(f)
        if time.time() - data.get("_timestamp", 0) > CACHE_EXPIRE: return False
        ip_app_map.update(data.get("ip_app_map", {}))
        port_app_map.update({int(k): v for k, v in data.get("port_app_map", {}).items()})
        return True
    except: return False

def save_cache():
    try:
        data = {"_timestamp": time.time(), "ip_app_map": ip_app_map, "port_app_map": port_app_map}
        with open(CACHE_FILE, "w") as f: json.dump(data, f, indent=2)
    except: pass

def refresh_ip_mapping():
    global ip_app_map, port_app_map
    ip_app_map = {}
    port_app_map = {}
    for app_name, conf in apps_conf.get("apps", {}).items():
        for item in conf.get("cidrs", []): ip_app_map[item] = app_name
        for port in conf.get("ports", []): port_app_map[int(port)] = app_name
    save_cache()

# [FIX] Fungsi 'match_app' udah nggak relevan, tapi kita biarin aja
def match_app(src_ip, dst_ip, tp_src, tp_dst):
    def clean_ip(ip): return ip.split('/')[0] if ip and '/' in ip else ip
    s, d = clean_ip(src_ip), clean_ip(dst_ip)
    for net, app_name in ip_app_map.items():
        try:
            if s and ipaddress.ip_address(s) in ipaddress.ip_network(net): return app_name
            if d and ipaddress.ip_address(d) in ipaddress.ip_network(net): return app_name
        except: continue
    if tp_src in port_app_map: return port_app_map[tp_src]
    if tp_dst in port_app_map: return port_app_map[tp_dst]
    return "unknown"

def get_synthetic_metrics(app):
    lat_range = app_latency_ranges.get(app, app_latency_ranges["unknown"])
    loss_range = app_loss_ranges.get(app, app_loss_ranges["unknown"])
    return random.uniform(*lat_range), random.uniform(*loss_range)

# ---------------------- FUNGSI UTAMA KOLEKSI (FIXED) ----------------------
def collect_current_traffic():
    """
    Mengumpulkan lalu lintas, menghitung DELTA (perubahan) per FLOW,
    dan meng-agregasi DELTA tersebut per APLIKASI.
    """
    global ip_mac_map, traffic_aggregator, last_flow_stats
    ip_mac_map.update(get_controller_mappings())
    
    current_bytes = 0
    current_packets = 0

    for dpid in DPIDS:
        try:
            res = requests.get(f"{RYU_REST}/stats/flow/{dpid}", timeout=5).json()
        except Exception as e:
            print(f"Error fetch dpid {dpid}: {e}", file=sys.stderr)
            continue
            
        if str(dpid) not in res:
            continue

        for flow in res[str(dpid)]:
            match = flow.get("match", {})
            src_ip = match.get("ipv4_src") or match.get("nw_src")
            dst_ip = match.get("ipv4_dst") or match.get("nw_dst")
            if not src_ip or not dst_ip:
                continue
            
            tp_src = int(match.get("tcp_src") or match.get("udp_src") or 0)
            tp_dst = int(match.get("tcp_dst") or match.get("udp_dst") or 0)
            proto = {6:"tcp", 17:"udp"}.get(match.get("ip_proto", 0), "any")

            
            # [FIX] HAPUS PANGGILAN 'match_app'
            # app_name = match_app(src_ip, dst_ip, tp_src, tp_dst)
            
            # [FIX] Ganti pakai 'hardcoded' IP (dari topo)
            app_name = "unknown"
            # Cek traffic DARI client (h1, h2, h3)
            if src_ip == '10.0.0.1':
                app_name = 'youtube'
            elif src_ip == '10.0.0.2':
                app_name = 'netflix'
            elif src_ip == '10.0.0.3':
                app_name = 'twitch'
            # Cek traffic KE client (balasan dari server)
            elif dst_ip == '10.0.0.1':
                 app_name = 'youtube'
            elif dst_ip == '10.0.0.2':
                 app_name = 'netflix'
            elif dst_ip == '10.0.0.3':
                 app_name = 'twitch'
            
            # Skip traffic 'unknown' (kayak ARP, routing, dll)
            if app_name == "unknown":
                continue
            
            category = app_category_map.get(app_name, "data")
            
            # --- LOGIKA PENGHITUNGAN DELTA (FIXED) ---
            current_total_bytes = flow.get("byte_count", 0)
            current_total_pkts = flow.get("packet_count", 0)
            
            # [FIX] Kunci "memori" HARUS spesifik per flow
            flow_key = (dpid, src_ip, dst_ip, tp_src, tp_dst, proto)
            
            last_total_bytes = last_flow_stats[flow_key]["bytes"]
            last_total_pkts = last_flow_stats[flow_key]["pkts"]
            
            delta_bytes = current_total_bytes - last_total_bytes
            delta_pkts = current_total_pkts - last_total_pkts
            
            # Cek jika flow di-reset (counter < 0) atau flow baru
            if delta_bytes < 0: 
                delta_bytes = current_total_bytes
            if delta_pkts < 0: 
                delta_pkts = current_total_pkts
                
            # [FIX] Simpan total kumulatif ke "memori" spesifik flow
            last_flow_stats[flow_key]["bytes"] = current_total_bytes
            last_flow_stats[flow_key]["pkts"] = current_total_pkts
            
            # --- SELESAI LOGIKA DELTA ---

            # Metrik Sintetik (SAMA)
            latency, loss = get_synthetic_metrics(app_name)
            pkts_rx = max(0, int(delta_pkts * (1 - loss/100)))
            
            # [FIX] Agregasi DELTA berdasarkan APLIKASI
            # Kunci agregator (tetap per app)
            agg_key = (dpid, app_name, category)
            
            traffic_aggregator[agg_key]["bytes_tx"] += delta_bytes
            traffic_aggregator[agg_key]["bytes_rx"] += delta_bytes # Simulasi rx=tx
            traffic_aggregator[agg_key]["pkts_tx"] += delta_pkts
            traffic_aggregator[agg_key]["pkts_rx"] += pkts_rx
            traffic_aggregator[agg_key]["latency_sum"] += latency
            traffic_aggregator[agg_key]["loss_sum"] += loss
            traffic_aggregator[agg_key]["count"] += 1
            traffic_aggregator[agg_key]["src_ip"] = src_ip
            traffic_aggregator[agg_key]["dst_ip"] = dst_ip
            traffic_aggregator[agg_key]["src_mac"] = match.get("eth_src") or ip_mac_map.get(src_ip, "")
            traffic_aggregator[agg_key]["dst_mac"] = match.get("eth_dst") or ip_mac_map.get(dst_ip, "")
            traffic_aggregator[agg_key]["proto"] = proto
            
            current_bytes += delta_bytes
            current_packets += delta_pkts

    return current_bytes > 0 or current_packets > 0

def get_aggregated_traffic():
    """Mengembalikan data yang diagregasi dan me-reset agregator."""
    global traffic_aggregator
    
    if not traffic_aggregator:
        return []
    
    ts = datetime.now()
    rows = []
    
    for (dpid, app, category), data in traffic_aggregator.items():
        if data["count"] == 0:
            continue
            
        avg_latency = data["latency_sum"] / data["count"] if data["count"] > 0 else 0
        avg_loss = data["loss_sum"] / data["count"] if data["count"] > 0 else 0
        host = data["src_ip"] if data["src_ip"].startswith('10.0.0.') else data["dst_ip"]
        
        print(f"[SNAPSHOT] app={app}, bytes={data['bytes_tx']}, pkts={data['pkts_tx']}, lat={avg_latency:.2f}ms, loss={avg_loss:.2f}%")
        
        rows.append((
            ts, dpid, host, app, data["proto"], 
            data["src_ip"], data["dst_ip"], data["src_mac"], data["dst_mac"],
            data["bytes_tx"], data["bytes_rx"], 
            data["pkts_tx"], data["pkts_rx"], 
            avg_latency, category
        ))
    
    traffic_aggregator.clear()
    
    return rows

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
    if not load_cache():
        refresh_ip_mapping()
    last_refresh = time.time()
    
    while True:
        if time.time() - last_refresh > 600:
            refresh_ip_mapping()
            last_refresh = time.time()
        
        has_traffic_delta = collect_current_traffic()
        
        if has_traffic_delta:
            aggregated_rows = get_aggregated_traffic()
            if aggregated_rows:
                inserted = insert_pg(aggregated_rows)
                print(f"{inserted} baris DELTA (FIXED) masuk DB.", file=sys.stderr)
            else:
                print("Tidak ada data baru (agregator kosong).", file=sys.stderr)
        else:
            print("Tidak ada delta baru (flow tidak berubah).", file=sys.stderr)
            
        time.sleep(5)

