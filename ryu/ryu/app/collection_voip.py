#!/usr/bin/python3
"""
[COLLECTION MODULAR]
- Membaca target dari config_voip.py
- Menggunakan metode Kernel Reading (/sys/class/net) agar akurat
- Struktur Data Lengkap (timestamp s/d traffic_label)
"""

import psycopg2
import subprocess
import time
import sys
from datetime import datetime

# Import Config
try:
    import config_voip as cfg
except ImportError:
    print("Error: config_voip.py tidak ditemukan!")
    sys.exit(1)

def get_kernel_stats(host_name, interface):
    """Membaca statistik langsung dari file system kernel namespace"""
    try:
        def read_val(metric):
            cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'cat', f'/sys/class/net/{interface}/statistics/{metric}']
            res = subprocess.run(cmd, capture_output=True, text=True)
            val = res.stdout.strip()
            return int(val) if val.isdigit() else 0

        return (
            read_val('tx_bytes'), read_val('tx_packets'),
            read_val('rx_bytes'), read_val('rx_packets')
        )
    except:
        return 0, 0, 0, 0

def main():
    print(f"--- START MONITORING [DB: {cfg.DB_CONFIG['table_name']}] ---")
    
    # 1. Init Database Connection
    try:
        conn = psycopg2.connect(
            dbname=cfg.DB_CONFIG['dbname'],
            user=cfg.DB_CONFIG['user'],
            password=cfg.DB_CONFIG['password'],
            host=cfg.DB_CONFIG['host']
        )
        print("[OK] Koneksi DB Berhasil.")
    except Exception as e:
        print(f"[FATAL] Gagal Konek DB: {e}")
        sys.exit(1)

    # 2. Filter hanya Host 'Sender' yang perlu dimonitor TX-nya
    targets = [k for k, v in cfg.HOST_MAP.items() if v['role'] == 'sender']
    
    prev_stats = {}

    try:
        while True:
            start_loop = time.time()
            timestamp = datetime.now()
            batch_data = []
            
            # Print Header Log
            print(f"\n--- {timestamp.strftime('%H:%M:%S')} ---")

            for host in targets:
                meta = cfg.HOST_MAP[host]
                
                # Ambil data SENDER (TX)
                tx_b, tx_p, _, _ = get_kernel_stats(host, meta['interface'])
                
                # Ambil data RECEIVER (RX) - untuk throughput yang akurat
                # Kita cari siapa receiver dari sender ini berdasarkan IP target
                target_host_name = [k for k,v in cfg.HOST_MAP.items() if v['ip'] == meta['target_ip']][0]
                target_meta = cfg.HOST_MAP[target_host_name]
                _, _, rx_b, rx_p = get_kernel_stats(target_host_name, target_meta['interface'])

                # Skip jika belum ada trafik
                if tx_b == 0 and rx_b == 0:
                    continue

                # Hitung Delta
                if host in prev_stats:
                    prev = prev_stats[host]
                    time_diff = start_loop - prev['ts']
                    
                    if time_diff > 0:
                        bytes_tx_diff = tx_b - prev['tx_b']
                        bytes_rx_diff = rx_b - prev['rx_b']
                        pkts_tx_diff  = tx_p - prev['tx_p']
                        pkts_rx_diff  = rx_p - prev['rx_p']
                        
                        # Calculate Throughput (Mbps) based on RX
                        throughput = (bytes_rx_diff * 8) / 1_000_000.0 / time_diff
                        throughput = max(0.0, throughput)

                        print(f"[{meta['type']:^10}] {meta['ip']}->{meta['target_ip']} | Load: {throughput:.4f} Mbps")

                        # Susun Data Tuple (16 Kolom)
                        row = (
                            timestamp,              # timestamp
                            "0000000000000001",     # dpid
                            meta['ip'],             # src_ip
                            meta['target_ip'],      # dst_ip
                            meta['mac'],            # src_mac
                            meta['target_mac'],     # dst_mac
                            meta['proto'],          # ip_proto
                            meta['tp_src'],         # tp_src
                            meta['tp_dst'],         # tp_dst
                            bytes_tx_diff,          # bytes_tx
                            bytes_rx_diff,          # bytes_rx
                            pkts_tx_diff,           # pkts_tx
                            pkts_rx_diff,           # pkts_rx
                            time_diff,              # duration_sec
                            throughput,             # throughput_mbps
                            meta['type']            # traffic_label
                        )
                        batch_data.append(row)

                # Update State
                prev_stats[host] = {
                    'tx_b': tx_b, 'tx_p': tx_p,
                    'rx_b': rx_b, 'rx_p': rx_p,
                    'ts': start_loop
                }

            # Insert ke Database
            if batch_data:
                try:
                    cur = conn.cursor()
                    # Query dinamis berdasarkan nama tabel di config
                    sql = f"""INSERT INTO {cfg.DB_CONFIG['table_name']}
                             (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                              ip_proto, tp_src, tp_dst, 
                              bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                              duration_sec, throughput_mbps, traffic_label) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    
                    cur.executemany(sql, batch_data)
                    conn.commit()
                except Exception as e:
                    print(f"[DB ERROR] {e}")
                    # Reconnect logic simple
                    try:
                        conn.rollback()
                    except:
                        pass

            # Jaga Loop 1 Detik
            elapsed = time.time() - start_loop
            time.sleep(max(0, 1.0 - elapsed))

    except KeyboardInterrupt:
        print("\nStop.")
        conn.close()

if __name__ == '__main__':
    main()