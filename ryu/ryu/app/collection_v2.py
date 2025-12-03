#!/usr/bin/python3
import psycopg2
import re
import time
import subprocess
from collections import defaultdict
from datetime import datetime
import threading

# Import konfigurasi bersama
from shared_config import DB_CONN, COLLECT_INTERVAL, HOSTS_TO_MONITOR, HOST_INFO, APP_TO_CATEGORY

# State untuk delta calculation
last_tx_bytes = defaultdict(int)
last_rx_bytes = defaultdict(int)
last_tx_pkts  = defaultdict(int)
last_rx_pkts  = defaultdict(int)

stop_event = threading.Event()

# ------------------------------------------------------------
# UTILITY: Eksekusi command di namespace host
# ------------------------------------------------------------
def run_ns_cmd(host, cmd, timeout=4):
    try:
        full = ['sudo', 'ip', 'netns', 'exec', host] + cmd
        out = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return out.stdout
    except:
        return ""


# ------------------------------------------------------------
# Parsing statistik interface (TX/RX bytes & packets)
# ------------------------------------------------------------
def get_iface_stats(host):
    """
    Return: (tx_bytes, tx_pkts, rx_bytes, rx_pkts) ATAU None jika gagal
    """
    out = run_ns_cmd(host, ['ip', '-s', 'link', 'show', 'eth0'])

    if not out:
        return None

    # Cari TX baris data
    tx_match = re.search(r'TX:.*\n\s*(\d+)\s+(\d+)', out)
    rx_match = re.search(r'RX:.*\n\s*(\d+)\s+(\d+)', out)

    if not tx_match or not rx_match:
        return None

    try:
        tx_bytes = int(tx_match.group(1))
        tx_pkts  = int(tx_match.group(2))
        rx_bytes = int(rx_match.group(1))
        rx_pkts  = int(rx_match.group(2))
        return tx_bytes, tx_pkts, rx_bytes, rx_pkts
    except:
        return None


# ------------------------------------------------------------
# Ambil latency via ping (aman, non-blocking)
# ------------------------------------------------------------
def get_latency(host, dst_ip):
    if not dst_ip:
        return 0.0

    out = run_ns_cmd(host, ['ping', '-c', '2', '-W', '1', dst_ip])

    if not out:
        return 0.0

    match = re.search(r'avg = ([\d.]+)', out)
    if match:
        return float(match.group(1))
    
    return 0.0


# ------------------------------------------------------------
# Insert ke PostgreSQL
# ------------------------------------------------------------
def insert_db(rows, conn):
    cur = None
    try:
        if conn is None or conn.closed != 0:
            conn = psycopg2.connect(DB_CONN)

        cur = conn.cursor()

        for r in rows:
            cur.execute("""
                INSERT INTO traffic.flow_stats(
                    timestamp, dpid, host, app, proto,
                    src_ip, dst_ip, src_mac, dst_mac,
                    bytes_tx, bytes_rx, pkts_tx, pkts_rx,
                    latency_ms, category
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, r)

        conn.commit()

    except Exception as e:
        print("DB ERROR:", e)
        if conn:
            try: conn.rollback()
            except: pass
        conn = None
    finally:
        if cur: cur.close()

    return conn


# ------------------------------------------------------------
# LOOP COLLECTOR UTAMA
# ------------------------------------------------------------
def run_collector():
    global last_tx_bytes, last_rx_bytes, last_tx_pkts, last_rx_pkts

    print("\n*** Collector ON ***\n")

    conn = None
    try:
        conn = psycopg2.connect(DB_CONN)
        print("*** DB Connected ***\n")
    except Exception as e:
        print("DB Connect Failed:", e)
        return

    while not stop_event.is_set():

        now = datetime.now()
        rows = []

        for host in HOSTS_TO_MONITOR:

            # Ambil statistik interface NIC
            stats = get_iface_stats(host)
            if not stats:
                continue

            cur_tx_bytes, cur_tx_pkts, cur_rx_bytes, cur_rx_pkts = stats

            # Dapatkan konfigurasi host
            cfg = HOST_INFO.get(host, {})
            app     = cfg.get('app', 'unknown')
            ip      = cfg.get('ip', '')
            mac     = cfg.get('mac', '')
            dst_ip  = cfg.get('server_ip', '')
            dst_mac = cfg.get('server_mac', '')
            proto   = cfg.get('proto', 'udp')
            category = APP_TO_CATEGORY.get(app, 'data')

            # Latency
            latency = get_latency(host, dst_ip)

            # Hitung delta (flow rate)
            d_tx_b = max(0, cur_tx_bytes - last_tx_bytes[host])
            d_rx_b = max(0, cur_rx_bytes - last_rx_bytes[host])
            d_tx_p = max(0, cur_tx_pkts  - last_tx_pkts[host])
            d_rx_p = max(0, cur_rx_pkts  - last_rx_pkts[host])

            # Update last values
            last_tx_bytes[host] = cur_tx_bytes
            last_rx_bytes[host] = cur_rx_bytes
            last_tx_pkts[host]  = cur_tx_pkts
            last_rx_pkts[host]  = cur_rx_pkts

            # Hanya insert jika ada trafik
            if d_tx_b > 0 or d_rx_b > 0:
                print(f"[{host}] {app} TX:{d_tx_b}B/{d_tx_p}pkts  RX:{d_rx_b}B/{d_rx_p}pkts  Lat:{latency:.2f}ms")

                rows.append((
                    now, 1, host, app, proto,
                    ip, dst_ip, mac, dst_mac,
                    d_tx_b, d_rx_b, d_tx_p, d_rx_p,
                    latency, category
                ))

        # Commit ke DB
        if rows:
            conn = insert_db(rows, conn)

        # Tunggu interval
        if stop_event.wait(COLLECT_INTERVAL):
            break

    if conn:
        conn.close()
    print("\n*** Collector STOPPED ***\n")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        stop_event.set()
        print("\nCtrl+C detected. Stopping collector...\n")
