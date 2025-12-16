#!/usr/bin/python3
"""
[COLLECTION FINAL - KERNEL MODE]
- Membaca statistik langsung dari Interface Host (Kernel) agar akurat (Anti-0 Mbps).
- Mengirim SEMUA parameter (Timestamp s/d Label) ke Database.
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

# --- DEFINISI FLOW LENGKAP ---
# Parameter statis (IP, MAC, Port) didefinisikan di sini
MONITOR_FLOWS = [
    {
        'label_base': 'VoIP',
        'src_host': 'h1', 'dst_host': 'h3',
        'src_ip': '10.0.0.1', 'dst_ip': '10.0.0.3',
        'src_mac': '00:00:00:00:00:01', 'dst_mac': '00:00:00:00:00:03',
        'proto': 17,       # UDP
        'tp_src': 0,       # Dynamic (Random) -> set 0
        'tp_dst': 5060     # SIP/RTP Port
    },
    {
        'label_base': 'Background_Normal',
        'src_host': 'h2', 'dst_host': 'h4',
        'src_ip': '10.0.0.2', 'dst_ip': '10.0.0.4',
        'src_mac': '00:00:00:00:00:02', 'dst_mac': '00:00:00:00:00:04',
        'proto': 6,        # TCP
        'tp_src': 0,       # Dynamic
        'tp_dst': 5001     # Iperf Port
    }
]

def get_host_stats_kernel(host_name):
    """
    Mengambil data bytes/packet langsung dari file kernel Linux.
    Path: /sys/class/net/eth0/statistics/
    """
    try:
        def read_file(metric):
            # Membaca file angka statistik di dalam namespace host
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
        return 0, 0, 0, 0

def main():
    print(f"[DB] Connect to: {DB_CONFIG['dbname']} at {DB_CONFIG['host']}")
    print("*** Memulai Monitoring Trafik Real-time (Mode Kernel) ***")
    
    # Simpan state sebelumnya untuk menghitung delta
    prev_stats = {}

    try:
        while True:
            start_loop = time.time()
            timestamp = datetime.now()
            print(f"\n--- {timestamp.strftime('%H:%M:%S')} ---")

            batch_data = []

            for flow in MONITOR_FLOWS:
                # 1. Ambil data Real-time
                # TX diambil dari Pengirim, RX diambil dari Penerima agar akurat
                tx_b_curr, tx_p_curr, _, _ = get_host_stats_kernel(flow['src_host'])
                _, _, rx_b_curr, rx_p_curr = get_host_stats_kernel(flow['dst_host'])

                flow_key = f"{flow['src_host']}->{flow['dst_host']}"

                # 2. Hitung Delta (Selisih) jika ada data sebelumnya
                if flow_key in prev_stats:
                    prev = prev_stats[flow_key]
                    time_diff = start_loop - prev['ts']
                    
                    if time_diff > 0:
                        bytes_tx_diff = tx_b_curr - prev['tx_b']
                        bytes_rx_diff = rx_b_curr - prev['rx_b']
                        pkts_tx_diff  = tx_p_curr - prev['tx_p']
                        pkts_rx_diff  = rx_p_curr - prev['rx_p']

                        # Hitung Throughput (Mbps) based on RX
                        throughput_mbps = (bytes_rx_diff * 8) / 1000000.0 / time_diff
                        throughput_mbps = max(0.0, throughput_mbps) # Hindari negatif

                        print(f"[{flow['label_base']:^15}] {flow['src_ip']}->{flow['dst_ip']} | Load: {throughput_mbps:.2f} Mbps | RX Pkts: {pkts_rx_diff}")

                        # 3. Siapkan Data Lengkap untuk DB
                        batch_data.append((
                            timestamp,              # timestamp
                            "0000000000000001",     # dpid (s1)
                            flow['src_ip'],         # src_ip
                            flow['dst_ip'],         # dst_ip
                            flow['src_mac'],        # src_mac
                            flow['dst_mac'],        # dst_mac
                            flow['proto'],          # ip_proto
                            flow['tp_src'],         # tp_src
                            flow['tp_dst'],         # tp_dst
                            bytes_tx_diff,          # bytes_tx
                            bytes_rx_diff,          # bytes_rx
                            pkts_tx_diff,           # pkts_tx
                            pkts_rx_diff,           # pkts_rx
                            time_diff,              # duration_sec
                            throughput_mbps,        # throughput_mbps
                            flow['label_base']      # traffic_label
                        ))

                # Update state untuk loop berikutnya
                prev_stats[flow_key] = {
                    'tx_b': tx_b_curr, 'rx_b': rx_b_curr,
                    'tx_p': tx_p_curr, 'rx_p': rx_p_curr,
                    'ts': start_loop
                }

            # 4. Insert ke Database
            if batch_data:
                try:
                    conn = psycopg2.connect(**DB_CONFIG)
                    cur = conn.cursor()
                    # Query INSERT lengkap
                    sql = """INSERT INTO design_data_ta
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

            # Sleep sisa waktu agar pas 1 detik
            elapsed = time.time() - start_loop
            time.sleep(max(0, 1.0 - elapsed))

    except KeyboardInterrupt:
        print("\nMonitoring Berhenti.")

if __name__ == '__main__':
    main()