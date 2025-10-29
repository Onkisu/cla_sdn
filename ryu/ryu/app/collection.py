#!/usr/bin/python3
import psycopg2
import re
import time
import subprocess
import math
import random
from collections import defaultdict
from datetime import datetime
import threading

# Import konfigurasi shared
from config import DB_CONN, COLLECT_INTERVAL, HOSTS_TO_MONITOR, HOST_INFO, ROLE_TO_CATEGORY

# ---------------------- STATE & EVENT ----------------------
last_host_bytes_tx = defaultdict(int)
last_host_pkts_tx = defaultdict(int)
last_host_bytes_rx = defaultdict(int)
last_host_pkts_rx = defaultdict(int)
stop_event = threading.Event()

# ---------------------- UTIL: GET INTERFACE STATS ----------------------
def get_host_interface_bytes(host_name):
    """
    Ambil statistik TX/RX dari namespace host.
    """
    tx_bytes, tx_packets, rx_bytes, rx_packets = None, None, None, None
    try:
        cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ip', '-s', 'link', 'show', 'eth0']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True)
        cmd_output = result.stdout

        if cmd_output is None:
            return None, None, None, None

        tx_match = re.search(r'TX:.*\n\s*(\d+)\s+(\d+)\s+(\d+)', cmd_output)
        if tx_match:
            tx_bytes = int(tx_match.group(1))
            tx_packets = int(tx_match.group(2))
        else:
            print(f"[Collector] Gagal parsing TX stats untuk {host_name}")

        rx_match = re.search(r'RX:.*\n\s*(\d+)\s+(\d+)\s+(\d+)', cmd_output)
        if rx_match:
            rx_bytes = int(rx_match.group(1))
            rx_packets = int(rx_match.group(2))
        else:
            print(f"[Collector] Gagal parsing RX stats untuk {host_name}")

        return tx_bytes, tx_packets, rx_bytes, rx_packets

    except Exception:
        return None, None, None, None

# ---------------------- UTIL: GET LATENCY ----------------------
def get_host_latency(host_name, server_ip):
    """
    Hitung RTT rata-rata (ms) antar host dan server di namespace.
    """
    if not server_ip:
        return None
    try:
        cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ping', '-c', '3', '-i', '0.2', '-W', '1', server_ip]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)

        match = re.search(r'rtt min/avg/max/mdev = [\d.]+/([\d.]+)/[\d.]+/[\d.]+', result.stdout)
        if match:
            return float(match.group(1))
        else:
            return None
    except Exception:
        return None

# ---------------------- DB INSERT ----------------------
def insert_pg(rows, conn):
    """
    Masukkan data ke tabel traffic.flow_stats.
    """
    inserted_count = 0
    cur = None
    try:
        if conn is None or conn.closed != 0:
            print("[DB] Reconnecting...")
            conn = psycopg2.connect(DB_CONN)

        cur = conn.cursor()
        for i, r in enumerate(rows):
            try:
                cur.execute("""
                    INSERT INTO traffic.flow_stats(
                        timestamp, dpid, host, app, proto,
                        src_ip, dst_ip, src_mac, dst_mac,
                        bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms, category
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, r)
                inserted_count += 1
            except Exception as row_e:
                print(f"[DB Row Error] Baris {i+1}: {row_e}")
                if conn:
                    conn.rollback()

        if inserted_count > 0:
            conn.commit()

    except Exception as e:
        print(f"[DB Error] {e}")
        if conn:
            try: conn.rollback()
            except: pass
        conn = None
        return 0, conn
    finally:
        if cur:
            cur.close()
    return inserted_count, conn

# ---------------------- MAIN COLLECTOR LOOP ----------------------
def run_collector():
    """
    Kolektor statistik host-host di data center.
    """
    global last_host_bytes_tx, last_host_pkts_tx, last_host_bytes_rx, last_host_pkts_rx

    try:
        conn = psycopg2.connect(DB_CONN)
        print("\n*** Collector Data Center Dimulai ***")
        print(f"*** Monitoring host: {HOSTS_TO_MONITOR} ***\n")
    except Exception as e:
        print(f"[Collector] Gagal konek ke DB: {e}")
        return

    while not stop_event.is_set():
        ts = datetime.now()
        rows_to_insert = []
        has_delta = False

        for host_name in HOSTS_TO_MONITOR:
            # --- Ambil data interface ---
            stats = get_host_interface_bytes(host_name)
            (curr_tx_b, curr_tx_p, curr_rx_b, curr_rx_p) = stats
            if None in stats:
                continue

            # --- Ambil konfigurasi host ---
            host_cfg = HOST_INFO.get(host_name, {})
            role = host_cfg.get('role', 'unknown')
            host_ip = host_cfg.get('ip', host_name)
            host_mac = host_cfg.get('mac', '00:00:00:00:00:00')
            server_ip = host_cfg.get('server_ip', None)
            server_mac = host_cfg.get('server_mac', '00:00:00:00:00:00')
            category = ROLE_TO_CATEGORY.get(role, 'infrastructure')

            # --- Latency aktif ---
            latency = get_host_latency(host_name, server_ip) or 0.0

            # --- Hitung delta ---
            delta_tx_b = curr_tx_b - last_host_bytes_tx[host_name]
            delta_tx_p = curr_tx_p - last_host_pkts_tx[host_name]
            delta_rx_b = curr_rx_b - last_host_bytes_rx[host_name]
            delta_rx_p = curr_rx_p - last_host_pkts_rx[host_name]

            if delta_tx_b < 0: delta_tx_b = curr_tx_b
            if delta_rx_b < 0: delta_rx_b = curr_rx_b
            if delta_tx_p < 0: delta_tx_p = curr_tx_p
            if delta_rx_p < 0: delta_rx_p = curr_rx_p

            last_host_bytes_tx[host_name] = curr_tx_b
            last_host_pkts_tx[host_name] = curr_tx_p
            last_host_bytes_rx[host_name] = curr_rx_b
            last_host_pkts_rx[host_name] = curr_rx_p

            if delta_tx_b > 0 or delta_rx_b > 0:
                has_delta = True
                print(f"[Collector] Host: {host_name}, Role: {role}, "
                      f"TX: {delta_tx_b}B/{delta_tx_p}P, RX: {delta_rx_b}B/{delta_rx_p}P, "
                      f"Latency: {latency:.2f} ms")

                rows_to_insert.append((
                    ts, 1, host_ip, role, "udp",
                    host_ip, server_ip or "0.0.0.0", host_mac, server_mac,
                    delta_tx_b, delta_rx_b, delta_tx_p, delta_rx_p,
                    latency, category
                ))

        # --- Insert ke DB ---
        if has_delta and rows_to_insert:
            inserted, conn = insert_pg(rows_to_insert, conn)

        # --- Tunggu ---
        interrupted = stop_event.wait(COLLECT_INTERVAL)
        if interrupted:
            break

    if conn:
        conn.close()
        print("\n*** Collector Berhenti ***\n")

# ---------------------- ENTRYPOINT ----------------------
if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        print("\n*** Ctrl+C diterima. Menghentikan collector... ***\n")
        stop_event.set()
