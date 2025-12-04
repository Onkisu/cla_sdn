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
    from shared_config import DB_CONN, COLLECT_INTERVAL, HOSTS_TO_MONITOR, HOST_INFO, APP_TO_CATEGORY
except ImportError:
    print("[CRITICAL] Config missing!")
    sys.exit(1)

last_tx = defaultdict(int); last_rx = defaultdict(int)
stop_event = threading.Event()

def run_ns_cmd(host, cmd):
    if not os.path.exists(f"/var/run/netns/{host}"): return None
    try:
        return subprocess.run(['sudo', 'ip', 'netns', 'exec', host] + cmd, capture_output=True, text=True, timeout=1).stdout
    except: return None

def get_stats(host):
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', 'eth0'])
    if not out: out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', f"{host}-eth0"])
    if not out: return None
    
    rx = re.search(r'RX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    tx = re.search(r'TX:.*?\n\s*(\d+)\s+(\d+)', out, re.MULTILINE)
    if rx and tx:
        return (int(tx.group(1)), int(tx.group(2)), int(rx.group(1)), int(rx.group(2)))
    return None

def db_connect():
    try: return psycopg2.connect(DB_CONN)
    except: return None

def collector():
    print(f"*** Monitoring {len(HOSTS_TO_MONITOR)} Hosts ***")
    time.sleep(2) # Wait for simulation
    
    # Baseline
    for h in HOSTS_TO_MONITOR:
        s = get_stats(h)
        if s: last_tx[h], _, last_rx[h], _ = s

    conn = db_connect()
    while not stop_event.is_set():
        if not conn or conn.closed: conn = db_connect()
        
        now = datetime.now()
        rows = []
        
        for h in HOSTS_TO_MONITOR:
            s = get_stats(h)
            if not s: continue
            
            ctx, ctp, crx, crp = s
            dtx = ctx - last_tx[h]; drx = crx - last_rx[h]
            last_tx[h] = ctx; last_rx[h] = crx
            
            meta = HOST_INFO.get(h, {})
            cat = APP_TO_CATEGORY.get(meta.get('app'), 'other')
            
            # Logic: Save if traffic OR if VoIP (force save for monitoring)
            if dtx > 0 or drx > 0 or cat == 'voip':
                # Debug print khusus VoIP jika traffic masih 0
                if cat == 'voip' and dtx == 0:
                    print(f"[DEBUG] {h} VoIP traffic is 0. Check routing/iperf!")

                rows.append((
                    now, 1, h, meta.get('app'), meta.get('proto'),
                    meta.get('ip'), meta.get('server_ip'),
                    meta.get('mac'), meta.get('server_mac'),
                    dtx, drx, 0, 0, # Pkts not critical for dashboard usually
                    0.5, cat
                ))
        
        if rows and conn:
            try:
                cur = conn.cursor()
                q = "INSERT INTO traffic.flow_stats (timestamp, dpid, host, app, proto, src_ip, dst_ip, src_mac, dst_mac, bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms, category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cur.executemany(q, rows)
                conn.commit()
                cur.close()
            except: pass
            
        time.sleep(COLLECT_INTERVAL)

if __name__ == "__main__":
    try: collector()
    except KeyboardInterrupt: pass