#!/usr/bin/python3
import requests
import psycopg2
import time
import sys
from datetime import datetime
from collections import defaultdict

DB_CONN = "dbname=development user=dev_one password=hijack332 host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1, 2, 3]

# Global aggregator
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

# Track previous bytes to compute delta
prev_bytes_map = {}

def collect_current_traffic():
    global traffic_aggregator

    has_traffic = False
    for dpid in DPIDS:
        try:
            res = requests.get(f"{RYU_REST}/stats/flow/{dpid}", timeout=3).json()
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

            proto = {6:"tcp", 17:"udp"}.get(flow.get("ip_proto", 0), "any")
            bytes_tx = flow.get("byte_count", 0)
            pkts_tx = flow.get("packet_count", 0)

            key = (dpid, src_ip, flow.get("ip_proto", proto))
            prev_bytes = prev_bytes_map.get(key, 0)
            delta_bytes = bytes_tx - prev_bytes
            if delta_bytes < 0:
                delta_bytes = bytes_tx  # handle counter reset
            prev_bytes_map[key] = bytes_tx

            # Save to aggregator
            agg_key = (dpid, "unknown", "data")
            traffic_aggregator[agg_key]["bytes_tx"] += delta_bytes
            traffic_aggregator[agg_key]["pkts_tx"] += pkts_tx
            traffic_aggregator[agg_key]["src_ip"] = src_ip
            traffic_aggregator[agg_key]["dst_ip"] = dst_ip
            traffic_aggregator[agg_key]["proto"] = proto
            traffic_aggregator[agg_key]["count"] += 1

            has_traffic = True

    return has_traffic

def get_aggregated_traffic():
    global traffic_aggregator

    ts = datetime.now()
    rows = []

    for (dpid, app, category), data in traffic_aggregator.items():
        if data["count"] == 0:
            continue

        row = (
            ts, dpid, data["src_ip"], app, data["proto"],
            data["src_ip"], data["dst_ip"], "aa:bb:cc:dd:ee:ff", "ff:ee:dd:cc:bb:aa",
            data["bytes_tx"], data["bytes_tx"], data["pkts_tx"], data["pkts_tx"],
            0, category
        )
        rows.append(row)

    traffic_aggregator.clear()
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
        return len(rows)
    except Exception as e:
        print(f"DB error: {e}", file=sys.stderr)
        return 0

if __name__ == "__main__":
    print("Starting Mininet traffic collector (5s intervals)...")
    while True:
        start_time = time.time()
        has_traffic = False

        # Collect traffic continuously for 5 seconds
        while time.time() - start_time < 5:
            if collect_current_traffic():
                has_traffic = True
            time.sleep(0.5)

        if has_traffic:
            aggregated_rows = get_aggregated_traffic()
            if aggregated_rows:
                inserted = insert_pg(aggregated_rows)
                print(f"{inserted} rows inserted at {datetime.now()}")
            else:
                print("No traffic in this window.")
        else:
            print("No new traffic detected.")
