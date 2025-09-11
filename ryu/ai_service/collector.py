#!/usr/bin/python3
import requests
import psycopg2
import time
import sys
import socket
import yaml
import json
import os
from datetime import datetime

DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1, 2, 3]

# Cache counter terakhir → untuk delta
last_bytes = {}
last_pkts = {}

# Cache ip→mac
ip_mac_map = {}

# Global mapping
ip_app_map = {}
port_app_map = {}

# --- Load apps.yaml ---
with open("apps.yaml") as f:
    apps_conf = yaml.safe_load(f)

CACHE_FILE = "ip_cache.json"
CACHE_EXPIRE = 3600  # 1 jam


def load_cache():
    """Load IP mapping cache dari file JSON kalau masih valid."""
    global ip_app_map
    if not os.path.exists(CACHE_FILE):
        return False

    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)

        ts_saved = data.get("_timestamp", 0)
        age = time.time() - ts_saved

        if age > CACHE_EXPIRE:
            print(f"[INFO] Cache expired ({int(age)}s), akan refresh ulang.")
            return False

        ip_app_map.update(data.get("ip_app_map", {}))
        print(f"[INFO] Cache {CACHE_FILE} diload ({len(ip_app_map)} entries, age={int(age)}s).")
        return True
    except Exception as e:
        print(f"[WARN] Gagal load cache: {e}", file=sys.stderr)
        return False


def save_cache():
    """Simpan IP mapping cache ke file JSON."""
    try:
        data = {
            "_timestamp": time.time(),
            "ip_app_map": ip_app_map
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)
        print(f"[INFO] Cache {CACHE_FILE} disimpan.")
    except Exception as e:
        print(f"[WARN] Gagal simpan cache: {e}", file=sys.stderr)


def refresh_ip_mapping():
    """Resolve domain di apps.yaml ke IP address (auto-refresh + cache)."""
    global ip_app_map, port_app_map
    ip_app_map = {}
    port_app_map = {}

    for app_name, conf in apps_conf.get("apps", {}).items():
        # Resolve domain → IP list
        for domain in conf.get("cidrs", []):
            try:
                ips = socket.gethostbyname_ex(domain)[2]
                for ip in ips:
                    ip_app_map[ip] = app_name
            except Exception as e:
                print(f"[WARN] DNS resolve gagal untuk {domain}: {e}", file=sys.stderr)

        # Map ports → app
        for port in conf.get("ports", []):
            port_app_map[port] = app_name

    save_cache()
    print(f"[INFO] Mapping di-refresh. {len(ip_app_map)} IP, {len(port_app_map)} ports.")


def collect_flows():
    rows = []
    ts = datetime.now()

    for dpid in DPIDS:
        try:
            t0 = time.time()
            res = requests.get(f"{RYU_REST}/stats/flow/{dpid}", timeout=5).json()
            latency_ms = round((time.time() - t0) * 1000, 2)
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
            tp_src = match.get("tcp_src") or match.get("udp_src")
            tp_dst = match.get("tcp_dst") or match.get("udp_dst")

            # update cache ip->mac
            if src_ip and src_mac:
                ip_mac_map[src_ip] = src_mac
            if dst_ip and dst_mac:
                ip_mac_map[dst_ip] = dst_mac

            # fallback mac
            if not src_mac and src_ip in ip_mac_map:
                src_mac = ip_mac_map[src_ip]
            if not dst_mac and dst_ip in ip_mac_map:
                dst_mac = ip_mac_map[dst_ip]

            if not src_ip and not dst_ip:
                continue

            # Identifikasi host & app
            host = src_ip or dst_ip or "unknown"
            app = "unknown"

            # Cek berdasarkan IP
            if src_ip in ip_app_map:
                app = ip_app_map[src_ip]
            elif dst_ip in ip_app_map:
                app = ip_app_map[dst_ip]

            # Cek berdasarkan port kalau masih unknown
            if app == "unknown":
                if tp_src in port_app_map:
                    app = port_app_map[tp_src]
                elif tp_dst in port_app_map:
                    app = port_app_map[tp_dst]

            # Protokol
            ip_proto = match.get("ip_proto", 0)
            proto = {6: "tcp", 17: "udp"}.get(ip_proto, "any")

            # Counter kumulatif
            bytes_count = flow.get("byte_count", 0)
            pkts_count = flow.get("packet_count", 0)

            # Key unik delta
            key = (dpid, src_ip, dst_ip, proto)

            delta_bytes = bytes_count - last_bytes.get(key, 0)
            delta_pkts = pkts_count - last_pkts.get(key, 0)

            # Update cache counter
            last_bytes[key] = bytes_count
            last_pkts[key] = pkts_count

            if delta_bytes > 0 or delta_pkts > 0:
                rows.append((
                    ts, dpid, host, app, proto,
                    src_ip, dst_ip, src_mac, dst_mac,
                    delta_bytes, delta_bytes, delta_pkts, delta_pkts,
                    latency_ms
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
    # load cache kalau masih valid, kalau tidak refresh DNS
    if not load_cache():
        refresh_ip_mapping()

    last_refresh = time.time()

    while True:
        # Refresh mapping tiap 600 detik (10 menit)
        if time.time() - last_refresh > 600:
            refresh_ip_mapping()
            last_refresh = time.time()

        rows = collect_flows()
        if rows:
            insert_pg(rows)
            print(f"{len(rows)} baris dimasukkan.", file=sys.stderr)
        else:
            print("Tidak ada delta baru.", file=sys.stderr)

        time.sleep(5)
