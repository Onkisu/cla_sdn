#!/usr/bin/python3
"""
[COLLECTION FINAL - KERNEL MODE]
Membaca statistik langsung dari /sys/class/net/eth0/statistics/
Pasti jalan di distro linux apapun.
"""

import psycopg2
import subprocess
import time
from datetime import datetime

# --- KONFIGURASI DATABASE ---
DB_CONFIG = {
    'dbname': 'development', 
    'user': 'dev_one', 
    'password': 'hijack332.', 
    'host': '103.181.142.165'
}

# --- KONFIGURASI TARGET ---
MONITOR_FLOWS = [
    {
        'label_base': 'VoIP',
        'src_host': 'h1', 'dst_host': 'h3',
        'src_ip': '10.0.0.1', 'dst_ip': '10.0.0.3',
        'src_mac': '00:00:00:00:00:01', 'dst_mac': '00:00:00:00:00:03',
        'proto': 17, 'tp_src': 0, 'tp_dst': 5060 
    },
    {
        'label_base': 'Background_Normal',
        'src_host': 'h2', 'dst_host': 'h4',
        'src_ip': '10.0.0.2', 'dst_ip': '10.0.0.4',
        'src_mac': '00:00:00:00:00:02', 'dst_mac': '00:00:00:00:00:04',
        'proto': 6, 'tp_src': 0, 'tp_dst': 5001 
    }
]

def get_host_stats_kernel(host_name):
    """
    Mengambil data bytes/packet langsung dari file kernel Linux.
    Anti-Gagal parsing regex.
    """
    try:
        def read_file(metric):
            # Path ke statistik interface virtual di dalam namespace
            cmd = ['sudo', 'ip', 'netns', 'exec', host_name, 'cat', f'/sys/class/net/eth0/statistics/{metric}']
            res = subprocess.run(cmd, capture_output=True, text=True)
            val = res.stdout.strip()
            return int(val) if val.isdigit() else 0

        tx_bytes = read_file('tx_bytes')
        tx_pkts  = read_file('tx_packets')
        rx_bytes = read_file('rx_bytes')
        rx_pkts  = read_file('rx_packets')
        
        return tx_bytes, tx_pkts, rx_bytes, rx_pkts

    except Exception as e:
        # Silent error agar tidak spam terminal, kembalikan 0
        return 0, 0, 0, 0

def main():
    print(f"[DB] Tabel 'design_data_ta' siap.")
    print("*** Memulai Monitoring Trafik Real-time (Mode Kernel) ***")
    print(f"Mencatat ke DB: {DB_CONFIG['dbname']}")

    # Simpan state sebelumnya untuk menghitung delta (selisih)
    prev_stats = {}

    while True:
        start_loop = time.time()
        timestamp = datetime.now()
        print(f"\n--- {timestamp.strftime('%H:%M:%S')} ---")

        batch_data = []

        for flow in MONITOR_FLOWS:
            src_host = flow['src_host']
            dst_host = flow['dst_host'] # Kita baca RX di sisi receiver agar akurat
            
            # Ambil statistik dari RECEIVER (h3 atau h4)
            # Karena throughput diukur dari yang diterima
            _, _, rx_b, rx_p = get_host_stats_kernel(dst_host)
            
            # Khusus untuk TX (dikirim), kita ambil dari SENDER (h1 atau h2)
            tx_b, tx_p, _, _ = get_host_stats_kernel(src_host)

            flow_key = f"{src_host}->{dst_host}"

            # Hitung Delta (Selisih dengan detik lalu)
            if flow_key in prev_stats:
                prev = prev_stats[flow_key]
                time_diff = start_loop - prev['ts']
                
                if time_diff > 0:
                    bytes_tx_diff = tx_b - prev['tx_b']
                    bytes_rx_diff = rx_b - prev['rx_b']
                    pkts_tx_diff  = tx_p - prev['tx_p']
                    pkts_rx_diff  = rx_p - prev['rx_p']

                    # Convert Bytes to Megabits per second (Mbps)
                    # (Bytes * 8) / 1,000,000 / seconds
                    throughput_mbps = (bytes_rx_diff * 8) / 1000000.0 / time_diff

                    # Hindari nilai negatif (jika restart)
                    throughput_mbps = max(0.0, throughput_mbps)

                    print(f"[{flow['label_base']:^15}] {flow['src_ip']}->{flow['dst_ip']} | Load: {throughput_mbps:.2f} Mbps | TX: {pkts_tx_diff} pkts")

                    # Siapkan data untuk DB
                    # (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                    #  ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, 
                    #  pkts_tx, pkts_rx, duration, throughput, label)
                    batch_data.append((
                        timestamp, "0000000000000001", 
                        flow['src_ip'], flow['dst_ip'],
                        flow['src_mac'], flow['dst_mac'],
                        flow['proto'], flow['tp_src'], flow['tp_dst'],
                        bytes_tx_diff, bytes_rx_diff,
                        pkts_tx_diff, pkts_rx_diff,
                        time_diff, throughput_mbps, flow['label_base']
                    ))

            # Update prev stats
            prev_stats[flow_key] = {
                'tx_b': tx_b, 'rx_b': rx_b,
                'tx_p': tx_p, 'rx_p': rx_p,
                'ts': start_loop
            }

        # Simpan ke DB
        if batch_data:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                # Pastikan nama tabel benar
                sql = """INSERT INTO traffic.flow_stats_
                         (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                          ip_proto, tp_src, tp_dst, 
                          bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                          duration_sec, throughput_mbps, traffic_label) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.executemany(sql, batch_data)
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"DB Error: {e}")

        # Sleep sisa waktu agar loop pas 1 detik
        elapsed = time.time() - start_loop
        time.sleep(max(0, 1.0 - elapsed))

if __name__ == '__main__':
    # Pastikan dijalankan dengan sudo
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitoring Berhenti.")