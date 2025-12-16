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

try:
    from config_voip import DB_CONN, COLLECT_INTERVAL, HOSTS_TO_MONITOR, HOST_INFO
except ImportError:
    print("[CRITICAL] Config missing!")
    sys.exit(1)

last_stats = defaultdict(lambda: {'tx_bytes': 0, 'rx_bytes': 0, 'tx_pkts': 0, 'rx_pkts': 0, 'time': 0})
stop_event = threading.Event()

def run_ns_cmd(host, cmd):
    if not os.path.exists(f"/var/run/netns/{host}"): return None
    try:
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host] + cmd, capture_output=True, text=True, timeout=0.5).stdout
    except: return None

def get_stats(host):
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', 'eth0'])
    if not out: out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', f"{host}-eth0"])
    if not out: return None
    
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    
    if rx and tx:
        return {
            'rx_bytes': int(rx.group(1)), 'rx_pkts': int(rx.group(2)),
            'tx_bytes': int(tx.group(1)), 'tx_pkts': int(tx.group(2)),
            'time': time.time()
        }
    return None

def db_connect():
    try: return psycopg2.connect(DB_CONN)
    except: return None

def collector():
    print(f"*** Monitoring {len(HOSTS_TO_MONITOR)} Hosts (Interval: {COLLECT_INTERVAL}s) ***")
    
    # Baseline
    for h in HOSTS_TO_MONITOR:
        s = get_stats(h)
        if s: last_stats[h] = s

    time.sleep(COLLECT_INTERVAL)
    conn = db_connect()
    
    while not stop_event.is_set():
        if not conn or conn.closed: conn = db_connect()
        
        rows = []
        now = datetime.now()
        
        for h in HOSTS_TO_MONITOR:
            curr = get_stats(h)
            if not curr: continue
            
            prev = last_stats[h]
            meta = HOST_INFO.get(h, {})
            
            dtx = curr['tx_bytes'] - prev['tx_bytes']
            drx = curr['rx_bytes'] - prev['rx_bytes']
            dtx_pkts = curr['tx_pkts'] - prev['tx_pkts']
            drx_pkts = curr['rx_pkts'] - prev['rx_pkts']
            time_diff = curr['time'] - prev['time']
            
            last_stats[h] = curr
            
            # Throughput Calc (Mbps)
            throughput = 0.0
            if time_diff > 0:
                throughput = (dtx * 8) / (time_diff * 1_000_000.0)

            # Labeling: Jika throughput > 5Mbps (ambang batas burst), tandai sebagai 'congestion_burst'
            # (Hanya untuk penandaan data, logic ini opsional)
            label = meta.get('label', 'unknown')
            if throughput > 5.0: # Threshold kasar
                label = 'congestion_event'

            if dtx > 0 or drx > 0:
                rows.append((
                    now, meta.get('dpid', 0), meta.get('ip'), meta.get('dst_ip'),
                    meta.get('mac'), meta.get('dst_mac'), meta.get('proto', 17),
                    meta.get('tp_src', 0), meta.get('tp_dst', 0),
                    dtx, drx, dtx_pkts, drx_pkts,
                    COLLECT_INTERVAL, round(throughput, 4), label
                ))

        if rows and conn:
            try:
                cur = conn.cursor()
                q = """INSERT INTO traffic.flow_stats_ (
                    timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                    ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                    duration_sec, throughput_mbps, traffic_label
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(q, rows)
                conn.commit()
                cur.close()
            except Exception as e:
                print(f"DB Error: {e}")
                conn.rollback()
            
        time.sleep(COLLECT_INTERVAL)

if __name__ == "__main__":
    try: collector()
    except KeyboardInterrupt: pass