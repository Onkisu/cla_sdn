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

DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1, 2, 3]

last_bytes = {}
last_pkts = {}
ip_mac_map = {}

ip_app_map = {}   # { "10.0.0.2/32": "youtube" }
port_app_map = {} # { 443: "youtube" }

with open("apps.yaml") as f:
    apps_conf = yaml.safe_load(f)

CACHE_FILE = "ip_cache.json"
CACHE_EXPIRE = 3600  # 1 jam

# For synthetic data generation (remove in production)
app_latency_ranges = {
    "youtube": (20, 60),
    "netflix": (25, 70), 
    "twitch": (15, 50),
    "unknown": (30, 100)
}

app_loss_ranges = {
    "youtube": (0, 3),
    "netflix": (0, 5),
    "twitch": (0, 2),
    "unknown": (0, 10)
}

def load_cache():
    global ip_app_map, port_app_map
    if not os.path.exists(CACHE_FILE):
        return False
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        ts_saved = data.get("_timestamp", 0)
        age = time.time() - ts_saved
        if age > CACHE_EXPIRE:
            print(f"[INFO] Cache expired ({int(age)}s), refresh ulang.")
            return False
        ip_app_map.update(data.get("ip_app_map", {}))
        port_app_map.update({int(k): v for k, v in data.get("port_app_map", {}).items()})
        print(f"[INFO] Cache diload: {len(ip_app_map)} IP, {len(port_app_map)} ports.")
        return True
    except Exception as e:
        print(f"[WARN] Gagal load cache: {e}", file=sys.stderr)
        return False

def save_cache():
    try:
        data = {
            "_timestamp": time.time(),
            "ip_app_map": ip_app_map,
            "port_app_map": port_app_map
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)
        print(f"[INFO] Cache disimpan.")
    except Exception as e:
        print(f"[WARN] Gagal simpan cache: {e}", file=sys.stderr)

def refresh_ip_mapping():
    global ip_app_map, port_app_map
    ip_app_map = {}
    port_app_map = {}

    for app_name, conf in apps_conf.get("apps", {}).items():
        for item in conf.get("cidrs", []):
            if "/" in item or item.replace(".", "").isdigit():
                ip_app_map[item] = app_name
            else:
                try:
                    ips = socket.gethostbyname_ex(item)[2]
                    for ip in ips:
                        ip_app_map[f"{ip}/32"] = app_name
                except Exception as e:
                    print(f"[WARN] DNS gagal {item}: {e}", file=sys.stderr)

        for port in conf.get("ports", []):
            try:
                port_app_map[int(port)] = app_name
            except Exception:
                continue

    save_cache()
    print(f"[INFO] Mapping di-refresh: {len(ip_app_map)} IP/CIDR, {len(port_app_map)} ports.")

def match_app(src_ip, dst_ip, tp_src, tp_dst):
    app = "unknown"
    
    # Clean IP addresses (remove subnet mask if present)
    def clean_ip(ip):
        if ip and '/' in ip:
            return ip.split('/')[0]
        return ip
    
    clean_src_ip = clean_ip(src_ip)
    clean_dst_ip = clean_ip(dst_ip)
    
    # Check IP mapping
    for net, app_name in ip_app_map.items():
        try:
            if clean_src_ip and ipaddress.ip_address(clean_src_ip) in ipaddress.ip_network(net):
                return app_name
            if clean_dst_ip and ipaddress.ip_address(clean_dst_ip) in ipaddress.ip_network(net):
                return app_name
        except Exception:
            continue

    # Check port mapping
    if tp_src in port_app_map:
        return port_app_map[tp_src]
    if tp_dst in port_app_map:
        return port_app_map[tp_dst]

    return app

def get_synthetic_metrics(app):
    """Generate synthetic latency and loss metrics for testing"""
    latency_range = app_latency_ranges.get(app, app_latency_ranges["unknown"])
    loss_range = app_loss_ranges.get(app, app_loss_ranges["unknown"])
    
    latency_ms = random.uniform(latency_range[0], latency_range[1])
    loss_percent = random.uniform(loss_range[0], loss_range[1])
    
    return latency_ms, loss_percent

def collect_flows():
    rows = []
    ts = datetime.now()

    for dpid in DPIDS:
        try:
            t0 = time.time()
            res = requests.get(f"{RYU_REST}/stats/flow/{dpid}", timeout=5).json()
            api_latency_ms = round((time.time() - t0) * 1000, 2)
        except Exception as e:
            print(f"Error fetch dpid {dpid}: {e}", file=sys.stderr)
            continue

        if str(dpid) not in res:
            continue

        for flow in res.get(str(dpid), []):
            match = flow.get("match", {})

            src_ip = match.get("ipv4_src") or match.get("nw_src")
            dst_ip = match.get("ipv4_dst") or match.get("nw_dst")
            src_mac = match.get("eth_src")
            dst_mac = match.get("eth_dst")
            tp_src = int(match.get("tcp_src") or match.get("udp_src") or 0)
            tp_dst = int(match.get("tcp_dst") or match.get("udp_dst") or 0)

            if not src_ip and not dst_ip:
                continue

            if src_ip and src_mac:
                ip_mac_map[src_ip] = src_mac
            if dst_ip and dst_mac:
                ip_mac_map[dst_ip] = dst_mac
            if not src_mac and src_ip in ip_mac_map:
                src_mac = ip_mac_map[src_ip]
            if not dst_mac and dst_ip in ip_mac_map:
                dst_mac = ip_mac_map[dst_ip]

            host = src_ip or dst_ip or "unknown"
            app_name = match_app(src_ip, dst_ip, tp_src, tp_dst)

            ip_proto = match.get("ip_proto", 0)
            proto = {6: "tcp", 17: "udp"}.get(ip_proto, "any")

            bytes_count = flow.get("byte_count", 0)
            total_pkts = flow.get("packet_count", 0)
            
            # Generate synthetic metrics for forecast system
            network_latency_ms, loss_percent = get_synthetic_metrics(app_name)
            
            # Calculate packet loss based on loss percentage
            pkts_tx = total_pkts
            pkts_rx = max(0, int(total_pkts * (1 - loss_percent / 100)))
            
            key = (dpid, src_ip, dst_ip, proto)
            delta_bytes = bytes_count - last_bytes.get(key, 0)
            delta_pkts_tx = pkts_tx - last_pkts.get(key, (0, 0))[0]
            delta_pkts_rx = pkts_rx - last_pkts.get(key, (0, 0))[1]
            
            last_bytes[key] = bytes_count
            last_pkts[key] = (pkts_tx, pkts_rx)

            print(f"[DBG] dpid={dpid}, src={src_ip}:{tp_src}, dst={dst_ip}:{tp_dst}, app={app_name}, "
                  f"latency={network_latency_ms:.2f}ms, loss={loss_percent:.2f}%")

            if delta_bytes > 0 or delta_pkts_tx > 0 or delta_pkts_rx > 0:
                rows.append((
                    ts, dpid, host, app_name, proto,
                    src_ip, dst_ip, src_mac, dst_mac,
                    delta_bytes, delta_bytes,  # bytes_tx, bytes_rx
                    delta_pkts_tx, delta_pkts_rx,  # pkts_tx, pkts_rx
                    network_latency_ms  # network latency for forecast
                ))

    return rows

def insert_pg(rows):
    try:
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        for r in rows:
            cur.execute("""
            INSERT INTO traffic.flow_stats(
                timestamp, dpid, host, app, proto,
                src_ip, dst_ip, src_mac, dst_mac,
                bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, r)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB error: {e}", file=sys.stderr)

if __name__ == "__main__":
    if not load_cache():
        refresh_ip_mapping()
    last_refresh = time.time()

    while True:
        if time.time() - last_refresh > 600:
            refresh_ip_mapping()
            last_refresh = time.time()

        rows = collect_flows()
        if rows:
            insert_pg(rows)
            print(f"{len(rows)} baris masuk DB dengan metrics untuk forecast.", file=sys.stderr)
        else:
            print("Tidak ada delta baru.", file=sys.stderr)

        time.sleep(5)