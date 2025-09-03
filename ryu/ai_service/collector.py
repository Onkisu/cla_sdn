#!/usr/bin/python3
import requests
import psycopg2
import time
import sys
from datetime import datetime

DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1, 2, 3]

# Cache counter terakhir → untuk delta
last_bytes = {}
last_pkts = {}

# Cache ip→mac
ip_mac_map = {}

def collect_flows():
    rows = []
    mapping = {'10.0.0.1': 'youtube', '10.0.0.2': 'netflix', '10.0.0.3': 'twitch'}

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

            # update cache ip->mac kalau dapat baru
            if src_ip and src_mac:
                ip_mac_map[src_ip] = src_mac
            if dst_ip and dst_mac:
                ip_mac_map[dst_ip] = dst_mac

            # fallback kalau mac null
            if not src_mac and src_ip in ip_mac_map:
                src_mac = ip_mac_map[src_ip]
            if not dst_mac and dst_ip in ip_mac_map:
                dst_mac = ip_mac_map[dst_ip]

            if not src_ip and not dst_ip:
                continue

            # Identifikasi host & app
            host, app = "unknown", "unknown"
            if src_ip in mapping:
                host, app = src_ip, mapping[src_ip]
            elif dst_ip in mapping:
                host, app = dst_ip, mapping[dst_ip]

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
    while True:
        rows = collect_flows()
        if rows:
            insert_pg(rows)
            print(f"{len(rows)} baris dimasukkan.", file=sys.stderr)
        else:
            print("Tidak ada delta baru.", file=sys.stderr)
        time.sleep(5)
