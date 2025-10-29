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

# [WAJIB] Memori untuk total byte TERAKHIR per host (dari 'ip link')
last_host_bytes = defaultdict(int)
stop_event = threading.Event()

def get_host_interface_bytes(host_name):
    """
    Mendapatkan total TX bytes dari interface eth0 host
    MENGGUNAKAN 'sudo mn --node ...' KARENA BERJALAN SEBAGAI PROSES TERPISAH
    """
    try:
        cmd = ['sudo', 'mn', '--node', host_name, 'ip', '-s', 'link', 'show', 'eth0']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True)
        cmd_output = result.stdout

        if cmd_output is None:
             return None

        # Regex dari v8.0 untuk parsing TX bytes
        match = re.search(r'TX:.*?bytes\s+packets\s+errors.*?(\d+)\s+(\d+)\s+(\d+)', cmd_output, re.DOTALL)
        if match:
            tx_bytes = int(match.group(1))
            return tx_bytes
        else:
            print(f"  [Collector] Gagal parsing output 'ip -s link' (TX) untuk {host_name}\nOutput:\n{cmd_output}\n")
            return None
    except subprocess.TimeoutExpired:
        print(f"  [Collector] Timeout saat menjalankan 'ip -s link' di {host_name}\n")
        return None
    except subprocess.CalledProcessError as e:
        # print(f"  [Collector] 'sudo mn' gagal untuk {host_name}. Mininet sudah jalan? Error: {e.stderr}\n")
        return None 
    except Exception as e:
        print(f"  [Collector] Exception get bytes for {host_name}: {e}\n")
        return None

# --- FUNGSI INSERT DB (Diambil dari v8.0, diadaptasi untuk pooling koneksi) ---
def insert_pg(rows, conn):
    """
    Memasukkan data agregasi ke PostgreSQL dengan koneksi yang ada.
    Menggunakan logging error detail dari v8.0.
    """
    inserted_count = 0
    cur = None # Definisikan di luar
    try:
        # 1. Cek jika koneksi putus
        if conn is None or conn.closed != 0:
            print("  [DB Insert] Koneksi terputus, mencoba menyambung ulang...")
            conn = psycopg2.connect(DB_CONN)
            print("  [DB Insert] Berhasil menyambung ulang.")

        cur = conn.cursor()
        # print(f"  [DB Debug] Mencoba insert {len(rows)} baris.") # Ganti info() -> print()

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
                if conn: conn.rollback() # Rollback baris ini

        if inserted_count > 0:
             conn.commit()
             # print(f"  [DB Debug] Commit {inserted_count} baris berhasil.")
        # else:
             # print("  [DB Debug] Tidak ada baris yang berhasil di-execute, tidak ada commit.")

    except (psycopg2.InterfaceError, psycopg2.OperationalError) as conn_e:
         print(f"  [DB Connection Error] {conn_e}. Menandai koneksi untuk reset.")
         if conn:
             try: conn.close() 
             except: pass
         conn = None # Set ke None agar loop berikutnya menyambung ulang
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
    """Loop utama collector, jalan tiap COLLECT_INTERVAL."""
    global last_host_bytes
    
    conn = None 
    try:
        conn = psycopg2.connect(DB_CONN)
        print("\n*** Collector (via 'sudo mn --node') Dimulai ***")
        print(f"*** Berhasil terhubung ke DB. Monitoring host: {HOSTS_TO_MONITOR} ***\n")
    except Exception as e:
        print(f"  [Collector] GAGAL terhubung ke DB saat startup: {e}. Collector berhenti.")
        return 

    while not stop_event.is_set():
        ts = datetime.now()
        rows_to_insert = []
        has_delta = False

        # Loop berdasarkan NAMA host dari config
        for host_name in HOSTS_TO_MONITOR:
            
            # 1. Ambil total byte (sama seperti v8.0, tapi via subprocess)
            current_total_bytes = get_host_interface_bytes(host_name)

            if current_total_bytes is None:
                continue 

            # 2. Ambil total byte sebelumnya
            last_total_bytes = last_host_bytes[host_name]

            # 3. Hitung DELTA (logika v8.0)
            delta_bytes = current_total_bytes - last_total_bytes
            if delta_bytes < 0:
                delta_bytes = current_total_bytes

            # 4. Update memori
            last_host_bytes[host_name] = current_total_bytes

            # 5. Siapkan data DB (logika v8.0)
            if delta_bytes > 0:
                has_delta = True
                
                # Ambil info dari config (menggantikan 'host.IP()' dll)
                host_cfg = HOST_INFO.get(host_name, {})
                app_name = host_cfg.get('app', 'unknown')
                host_ip = host_cfg.get('ip', host_name) 
                host_mac = host_cfg.get('mac', 'mac_dummy')
                category = APP_TO_CATEGORY.get(app_name, "data")
                
                latency = random.uniform(10, 50)
                loss = random.uniform(0, 1)

                print(f"  [Collector] Host: {host_name}, App: {app_name}, Delta TX Bytes: {delta_bytes}")

                # Format data insert dari v8.0
                rows_to_insert.append((
                    ts, 1, host_ip, app_name, "udp", # dpid=1, host=IP host
                    host_ip, "server_ip_dummy", host_mac, "mac_dummy", # src=host, dst=dummy
                    delta_bytes, delta_bytes, # tx/rx diisi delta_bytes
                    0, 0,                     # pkts_tx/rx diisi 0
                    latency, category
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