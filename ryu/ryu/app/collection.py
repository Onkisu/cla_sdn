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

# Import konfigurasi bersama
from shared_config import DB_CONN, COLLECT_INTERVAL, HOSTS_TO_MONITOR, HOST_INFO, APP_TO_CATEGORY

# [UPDATE] Kita lacak keempat statistik
last_host_bytes_tx = defaultdict(int)
last_host_pkts_tx = defaultdict(int)
last_host_bytes_rx = defaultdict(int)
last_host_pkts_rx = defaultdict(int)

stop_event = threading.Event()

def get_host_interface_bytes(host_name):
    """
    [UPDATE] Mendapatkan total TX dan RX bytes/packets dari interface eth0 host
    """
    tx_bytes, tx_packets, rx_bytes, rx_packets = None, None, None, None
    try:
        cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ip', '-s', 'link', 'show', 'eth0']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True)
        cmd_output = result.stdout

        if cmd_output is None:
             return None, None, None, None

        # [FIX] Regex yang lebih robust, mencari baris DATA setelah baris HEADER
        # Mencari "TX:" diikuti newline, lalu spasi, lalu 3 grup angka
        tx_match = re.search(r'TX:.*\n\s*(\d+)\s+(\d+)\s+(\d+)', cmd_output)
        if tx_match:
            tx_bytes = int(tx_match.group(1))
            tx_packets = int(tx_match.group(2))
        else:
            print(f"  [Collector DEBUG] Gagal parsing TX stats untuk {host_name}")
            
        # [FIX] Regex yang sama untuk RX
        rx_match = re.search(r'RX:.*\n\s*(\d+)\s+(\d+)\s+(\d+)', cmd_output)
        if rx_match:
            rx_bytes = int(rx_match.group(1))
            rx_packets = int(rx_match.group(2))
        else:
            print(f"  [Collector DEBUG] Gagal parsing RX stats untuk {host_name}")
            
        return tx_bytes, tx_packets, rx_bytes, rx_packets

    except Exception as e:
        # print(f"  [Collector DEBUG] Gagal get bytes/pkts untuk {host_name}: {e}\n")
        return None, None, None, None

def get_host_latency(host_name, server_ip):
    """
    [BARU] Mengukur latensi (RTT) nyata dari host ke servernya
    """
    if not server_ip:
        return None
    try:
        cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'ping', '-c', '3', '-i', '0.2', '-W', '1', server_ip]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        match = re.search(r'rtt min/avg/max/mdev = [\d.]+/([\d.]+)/[\d.]+/[\d.]+', result.stdout)
        if match:
            avg_rtt_ms = float(match.group(1))
            return avg_rtt_ms
        else:
            return None
            
    except Exception as e:
        return None


# --- FUNGSI INSERT DB (SAMA PERSIS, TIDAK BERUBAH) ---
def insert_pg(rows, conn):
    """
    Memasukkan data agregasi ke PostgreSQL dengan koneksi yang ada.
    """
    inserted_count = 0
    cur = None 
    try:
        if conn is None or conn.closed != 0:
            print("  [DB Insert] Koneksi terputus, mencoba menyambung ulang...")
            conn = psycopg2.connect(DB_CONN)
            print("  [DB Insert] Berhasil menyambung ulang.")

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
                print(f"  [DB Insert Row Error] Gagal insert baris {i+1}. Error: {row_e}")
                print(f"  [DB Debug] Data baris gagal: {r}")
                if conn: conn.rollback() 

        if inserted_count > 0:
             conn.commit()
             
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as conn_e:
         print(f"  [DB Connection Error] {conn_e}. Menandai koneksi untuk reset.")
         if conn:
             try: conn.close() 
             except: pass
         conn = None 
         return 0, conn 
    except Exception as e:
        print(f"  [DB Error Lain] {e}\n")
        if conn:
             try: conn.rollback() 
             except: pass
        return 0, conn 
    finally:
         if cur:
             cur.close()
             
    return inserted_count, conn

def run_collector():
    """
    [UPDATE] Loop utama collector, sekarang menghitung 4 statistik + latensi
    """
    global last_host_bytes_tx, last_host_pkts_tx, last_host_bytes_rx, last_host_pkts_rx
    
    conn = None 
    try:
        conn = psycopg2.connect(DB_CONN)
        print("\n*** Collector (via 'ip netns exec') Dimulai ***")
        print(f"*** Berhasil terhubung ke DB. Monitoring host: {HOSTS_TO_MONITOR} ***\n")
    except Exception as e:
        print(f"  [Collector] GAGAL terhubung ke DB saat startup: {e}. Collector berhenti.")
        return 

    while not stop_event.is_set():
        ts = datetime.now()
        rows_to_insert = []
        has_delta = False

        for host_name in HOSTS_TO_MONITOR:
            
            # --- 1. Ambil Statistik Interface (Bytes/Packets) ---
            stats = get_host_interface_bytes(host_name)
            (current_bytes_tx, current_pkts_tx, 
             current_bytes_rx, current_pkts_rx) = stats

            # [FIX] Cek jika salah satu None (parsing gagal)
            if current_bytes_tx is None or current_pkts_tx is None or current_bytes_rx is None or current_pkts_rx is None:
                continue 

            # --- 2. Ambil Info Konfigurasi ---
            host_cfg = HOST_INFO.get(host_name, {})
            app_name = host_cfg.get('app', 'unknown')
            host_ip = host_cfg.get('ip', host_name) 
            host_mac = host_cfg.get('mac', 'mac_dummy')
            server_ip = host_cfg.get('server_ip', 'server_ip_dummy')
            server_mac = host_cfg.get('server_mac', 'mac_dummy') # <-- [BARU] Ambil MAC server
            category = APP_TO_CATEGORY.get(app_name, "data")
            
            # --- 3. Ambil Statistik Latensi (Aktif) ---
            latency = get_host_latency(host_name, server_ip)
            latency_to_db = latency if latency is not None else 0.0

            # --- 4. Hitung DELTA untuk semua 4 statistik ---
            # TX Bytes
            delta_bytes_tx = current_bytes_tx - last_host_bytes_tx[host_name]
            if delta_bytes_tx < 0: delta_bytes_tx = current_bytes_tx
            last_host_bytes_tx[host_name] = current_bytes_tx
            
            # TX Packets
            delta_pkts_tx = current_pkts_tx - last_host_pkts_tx[host_name]
            if delta_pkts_tx < 0: delta_pkts_tx = current_pkts_tx
            last_host_pkts_tx[host_name] = current_pkts_tx
            
            # RX Bytes
            delta_bytes_rx = current_bytes_rx - last_host_bytes_rx[host_name]
            if delta_bytes_rx < 0: delta_bytes_rx = current_bytes_rx
            last_host_bytes_rx[host_name] = current_bytes_rx
            
            # RX Packets
            delta_pkts_rx = current_pkts_rx - last_host_pkts_rx[host_name]
            if delta_pkts_rx < 0: delta_pkts_rx = current_pkts_rx
            last_host_pkts_rx[host_name] = current_pkts_rx


            # --- 5. Siapkan data DB jika ada traffic ---
            if delta_bytes_tx > 0 or delta_bytes_rx > 0:
                has_delta = True
                
                # [UPDATE] Log print diperbarui untuk menampilkan paket
                print(f"  [Collector] Host: {host_name}, App: {app_name}, "
                      f"TX_B: {delta_bytes_tx}, TX_P: {delta_pkts_tx}, "
                      f"RX_B: {delta_bytes_rx}, RX_P: {delta_pkts_rx}, "
                      f"Latency: {latency_to_db:.2f} ms")

                # [UPDATE] Masukkan data nyata ke DB
                rows_to_insert.append((
                    ts, 1, host_ip, app_name, "udp", 
                    host_ip, server_ip, host_mac, server_mac, # <-- [FIX] Ganti "mac_dummy"
                    delta_bytes_tx, delta_bytes_rx, 
                    delta_pkts_tx, delta_pkts_rx,   
                    latency_to_db                   
                    , category
                 ))

        # 6. Masukkan data ke DB
        if has_delta and rows_to_insert:
            inserted, conn = insert_pg(rows_to_insert, conn)
        
        # 7. Tunggu
        interrupted = stop_event.wait(COLLECT_INTERVAL)
        if interrupted: 
             break
    
    if conn:
        conn.close()
        print("\n*** Collector Berhenti (Koneksi DB ditutup) ***\n")


if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        print("\n\n*** Ctrl+C diterima. Menghentikan collector...\n")
        stop_event.set()