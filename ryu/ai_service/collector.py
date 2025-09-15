#!/usr/bin/python3
import requests
import psycopg2
import time
import sys
import yaml
import json
import os
import ipaddress
import random
from datetime import datetime
from collections import defaultdict

DB_CONN = "dbname=development user=dev_one password=hijack332 host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1, 2, 3]

ip_mac_map = {}
ip_app_map = {}
port_app_map = {}

with open("apps.yaml") as f:
    apps_conf = yaml.safe_load(f)

app_category_map = {}
for cat, catconf in apps_conf.get("categories", {}).items():
    for app in catconf.get("apps", []):
        app_category_map[app] = cat

CACHE_FILE = "ip_cache.json"
CACHE_EXPIRE = 3600  # 1 jam

app_latency_ranges = {
    "youtube": (20, 60),
    "netflix": (25, 70),
    "twitch": (15, 50),
    "zoom": (10, 30),
    "skype": (10, 30),
    "unknown": (30, 100)
}

app_loss_ranges = {
    "youtube": (0, 3),
    "netflix": (0, 5),
    "twitch": (0, 2),
    "zoom": (0, 1),
    "skype": (0, 1),
    "unknown": (0, 10)
}

def get_controller_mappings():
    try:
        response = requests.get(f"{RYU_REST}/ip_mac_map", timeout=3)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Failed to get controller mappings: {e}")
    return {}

def load_cache():
    global ip_app_map, port_app_map
    if not os.path.exists(CACHE_FILE):
        return False
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        ts_saved = data.get("_timestamp", 0)
        if time.time() - ts_saved > CACHE_EXPIRE:
            return False
        ip_app_map.update(data.get("ip_app_map", {}))
        port_app_map.update({int(k): v for k, v in data.get("port_app_map", {}).items()})
        return True
    except:
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
    except:
        pass

def refresh_ip_mapping():
    global ip_app_map, port_app_map
    ip_app_map = {}
    port_app_map = {}
    for app_name, conf in apps_conf.get("apps", {}).items():
        for item in conf.get("cidrs", []):
            ip_app_map[item] = app_name
        for port in conf.get("ports", []):
            port_app_map[int(port)] = app_name
    save_cache()

def match_app(src_ip, dst_ip, tp_src, tp_dst):
    def clean_ip(ip):
        return ip.split('/')[0] if ip and '/' in ip else ip
    s, d = clean_ip(src_ip), clean_ip(dst_ip)
    for net, app_name in ip_app_map.items():
        try:
            if s and ipaddress.ip_address(s) in ipaddress.ip_network(net):
                return app_name
            if d and ipaddress.ip_address(d) in ipaddress.ip_network(net):
                return app_name
        except:
            continue
    if tp_src in port_app_map:
        return port_app_map[tp_src]
    if tp_dst in port_app_map:
        return port_app_map[tp_dst]
    return "unknown"

def get_synthetic_metrics(app):
    lat_range = app_latency_ranges.get(app, app_latency_ranges["unknown"])
    loss_range = app_loss_ranges.get(app, app_loss_ranges["unknown"])
    return random.uniform(*lat_range), random.uniform(*loss_range)

def collect_flows():
    ts = datetime.now()
    global ip_mac_map
    ip_mac_map.update(get_controller_mappings())

    # agregasi per (dpid, app, category)
    agg = defaultdict(lambda: {
        "bytes_tx": 0, "bytes_rx": 0,
        "pkts_tx": 0, "pkts_rx": 0,
        "latencies": [], "losses": [],
        "src_ip": None, "dst_ip": None,
        "src_mac": None, "dst_mac": None,
        "proto": "any"
    })

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
            src_ip, dst_ip = match.get("ipv4_src"), match.get("ipv4_dst")
            if not src_ip or not dst_ip:
                continue

            tp_src = int(match.get("tcp_src") or match.get("udp_src") or 0)
            tp_dst = int(match.get("tcp_dst") or match.get("udp_dst") or 0)

            app_name = match_app(src_ip, dst_ip, tp_src, tp_dst)
            category = app_category_map.get(app_name, "data")
            proto = {6: "tcp", 17: "udp"}.get(match.get("ip_proto", 0), "any")
            bytes_count = flow.get("byte_count", 0)
            pkts_tx = flow.get("packet_count", 0)

            latency, loss = get_synthetic_metrics(app_name)
            pkts_rx = max(0, int(pkts_tx * (1 - loss/100)))

            key = (dpid, app_name, category)
            agg[key]["bytes_tx"] += bytes_count
            agg[key]["bytes_rx"] += bytes_count
            agg[key]["pkts_tx"] += pkts_tx
            agg[key]["pkts_rx"] += pkts_rx
            agg[key]["latencies"].append(latency)
            agg[key]["losses"].append(loss)
            agg[key]["src_ip"] = src_ip
            agg[key]["dst_ip"] = dst_ip
            agg[key]["src_mac"] = match.get("eth_src") or ip_mac_map.get(src_ip)
            agg[key]["dst_mac"] = match.get("eth_dst") or ip_mac_map.get(dst_ip)
            agg[key]["proto"] = proto

    rows = []
    for (dpid, app, category), v in agg.items():
        avg_lat = sum(v["latencies"]) / len(v["latencies"]) if v["latencies"] else 0
        avg_loss = sum(v["losses"]) / len(v["losses"]) if v["losses"] else 0

        host = v["src_ip"] if v["src_ip"] and v["src_ip"].startswith("10.0.0.") else v["dst_ip"]

        rows.append((
            ts, dpid, host, app, v["proto"], v["src_ip"], v["dst_ip"],
            v["src_mac"], v["dst_mac"], v["bytes_tx"], v["bytes_rx"],
            v["pkts_tx"], v["pkts_rx"], avg_lat, category
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
                    bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms, category
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            print(f"{len(rows)} baris masuk DB (agregasi total per app, tiap 5 detik).", file=sys.stderr)
        else:
            print("Tidak ada data flow.", file=sys.stderr)
        time.sleep(5)
