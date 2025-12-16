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

# Simpan state terakhir untuk perhitungan delta (throughput)
last_stats = defaultdict(lambda: {'tx_bytes': 0, 'rx_bytes': 0, 'tx_pkts': 0, 'rx_pkts': 0, 'time': 0})
stop_event = threading.Event()

def run_ns_cmd(host, cmd):
    if not os.path.exists(f"/var/run/netns/{host}"): return None
    try:
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host] + cmd, capture_output=True, text=True, timeout=1).stdout
    except: return None

def get_stats(host):
    # Ambil statistik eth0 dari namespace host
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', 'eth0'])
    if not out: out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', f"{host}-eth0"])
    if not out: return None
    
    # Parsing output ip -s link
    rx_match = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx_match = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    
    if rx_match and tx_match:
        return {
            'rx_bytes': int(rx_match.group(1)),
            'rx_pkts': int(rx_match.group(2)),
            'tx_bytes': int(tx_match.group(1)),
            'tx_pkts': int(tx_match.group(2)),
            'time': time.time()
        }
    return None

def db_connect():
    try: return psycopg2.connect(DB_CONN)
    except Exception as e: 
        print(f"[DB ERROR] {e}")
        return None

def collector():
    print(f"*** Monitoring {len(HOSTS_TO_MONITOR)} Hosts (VoIP Traffic) ***")
    
    # Inisialisasi baseline
    for h in HOSTS_TO_MONITOR:
        s = get_stats(h)
        if s: last_stats[h] = s

    time.sleep(COLLECT_INTERVAL) # Tunggu siklus pertama

    conn = db_connect()
    
    while not stop_event.is_set():
        if not conn or conn.closed: conn = db_connect()
        
        rows = []
        current_time_db = datetime.now() # Timestamp untuk DB
        
        for h in HOSTS_TO_MONITOR:
            curr = get_stats(h)
            if not curr: continue
            
            prev = last_stats[h]
            meta = HOST_INFO.get(h, {})
            
            # Hitung Delta
            d_tx_bytes = curr['tx_bytes'] - prev['tx_bytes']
            d_rx_bytes = curr['rx_bytes'] - prev['rx_bytes']
            d_tx_pkts = curr['tx_pkts'] - prev['tx_pkts']
            d_rx_pkts = curr['rx_pkts'] - prev['rx_pkts']
            time_diff = curr['time'] - prev['time']
            
            # Update state
            last_stats[h] = curr
            
            # Hitung Throughput (Mbps) = (Bytes * 8) / (Time * 1M)
            # Kita gunakan arah TX sebagai indikator throughput utama node ini
            throughput = 0.0
            if time_diff > 0:
                throughput = (d_tx_bytes * 8) / (time_diff * 1_000_000.0)

            # Filter: Hanya simpan jika ada trafik atau jika ini adalah node aktif
            if d_tx_bytes > 0 or d_rx_bytes > 0:
                rows.append((
                    current_time_db,            # timestamp
                    meta.get('dpid', 0),        # dpid
                    meta.get('ip'),             # src_ip (Host IP)
                    meta.get('dst_ip'),         # dst_ip (Target IP)
                    meta.get('mac'),            # src_mac
                    meta.get('dst_mac'),        # dst_mac
                    meta.get('proto', 17),      # ip_proto (UDP=17)
                    meta.get('tp_src', 0),      # tp_src
                    meta.get('tp_dst', 0),      # tp_dst
                    d_tx_bytes,                 # bytes_tx (Delta)
                    d_rx_bytes,                 # bytes_rx (Delta)
                    d_tx_pkts,                  # pkts_tx (Delta)
                    d_rx_pkts,                  # pkts_rx (Delta)
                    COLLECT_INTERVAL,           # duration_sec
                    round(throughput, 4),       # throughput_mbps
                    meta.get('label', 'unknown')# traffic_label
                ))

        if rows and conn:
            try:
                cur = conn.cursor()
                # Query INSERT sesuai schema baru
                q = """
                INSERT INTO traffic.flow_stats_ (
                    timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                    ip_proto, tp_src, tp_dst, 
                    bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                    duration_sec, throughput_mbps, traffic_label
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(q, rows)
                conn.commit()
                cur.close()
                print(f"[SAVE] {len(rows)} records inserted.")
            except Exception as e:
                print(f"[DB WRITE ERROR] {e}")
                conn.rollback()
            
        time.sleep(COLLECT_INTERVAL)

if __name__ == "__main__":
    try: collector()
    except KeyboardInterrupt: pass