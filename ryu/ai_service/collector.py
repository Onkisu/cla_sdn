#!/usr/bin/python3
import requests
import psycopg2
import time
import sys
import json

# Database connection string
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# Ryu REST API endpoint
RYU_REST = "http://127.0.0.1:8080"

# DPIDs for the switches s1, s2, s3
DPIDS = [1, 2, 3]

def collect_flows():
    """
    Collects flow statistics from the Ryu controller for each switch.
    """
    rows = []
    # Mapping of host IPs to application names
    mapping = {'10.0.0.1': 'youtube', '10.0.0.2': 'netflix', '10.0.0.3': 'twitch'}

    for dpid in DPIDS:
        print(f"Mengumpulkan data dari dpid {dpid}...", file=sys.stderr)
        try:
            # Fetch flow stats from the Ryu controller
            res = requests.get(f"{RYU_REST}/stats/flow/{dpid}", timeout=5).json()
            # DEBUG: Print the raw response from Ryu to see the flow data
            print(f"Data mentah dari Ryu (dpid {dpid}):", file=sys.stderr)
            print(json.dumps(res, indent=2), file=sys.stderr)
            
        except requests.exceptions.RequestException as e:
            # Print an error and continue to the next switch if the request fails
            print(f"Error fetching flow stats for dpid {dpid}: {e}", file=sys.stderr)
            continue

        # Check if we got any data for this switch
        if str(dpid) not in res:
            print(f"Tidak ada data flow untuk switch dpid {dpid}", file=sys.stderr)
            continue
            
        # Iterate through the flows for the current switch
        for flow in res.get(str(dpid), []):
            print(f"Memproses flow: {flow.get('match', {})}", file=sys.stderr)
            
            # Try to get the source and destination IPs from the flow match
            match = flow.get('match', {})
            src_ip = match.get('ipv4_src') or match.get('nw_src')  # Try both possible field names
            dst_ip = match.get('ipv4_dst') or match.get('nw_dst')  # Try both possible field names
            
            print(f"IP SRC: {src_ip}, IP DST: {dst_ip}", file=sys.stderr)

            # If no IP information, skip this flow (might be ARP or other non-IP traffic)
            if not src_ip and not dst_ip:
                print("Flow tidak mengandung informasi IP, melewati...", file=sys.stderr)
                continue

            # Determine the host and application based on which IP is in the mapping.
            host = 'unknown'
            app = 'unknown'

            if src_ip in mapping:
                host = src_ip
                app = mapping.get(src_ip)
                print(f"Teridentifikasi: {host} -> {app} (source)", file=sys.stderr)
            elif dst_ip in mapping:
                host = dst_ip
                app = mapping.get(dst_ip)
                print(f"Teridentifikasi: {host} -> {app} (destination)", file=sys.stderr)
            else:
                # If neither the source nor destination IP matches, we can't identify the app.
                print(f"IP tidak dikenal: {src_ip} -> {dst_ip}, melewati...", file=sys.stderr)
                continue

            # Map IP protocol numbers to names
            ip_proto = match.get('ip_proto', 0)
            proto = {6: 'tcp', 17: 'udp'}.get(ip_proto, 'any')
            print(f"Protocol: {ip_proto} -> {proto}", file=sys.stderr)

            # Use the byte and packet counts from the flow
            bytes_count = flow.get('byte_count', 0)
            pkts_count = flow.get('packet_count', 0)
            
            print(f"Bytes: {bytes_count}, Packets: {pkts_count}", file=sys.stderr)

            # Append the data to the rows list
            rows.append((host, app, proto, bytes_count, bytes_count, pkts_count, pkts_count))
            print("Data ditambahkan ke rows", file=sys.stderr)
    
    # DEBUG: Print the final list of rows to be inserted
    print(f"Total baris yang dikumpulkan: {len(rows)}", file=sys.stderr)
    return rows


def insert_pg(rows):
    """
    Inserts collected flow data into the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        for r in rows:
            cur.execute("""
            INSERT INTO traffic.flow_stats(host, app, proto, bytes_tx, bytes_rx, pkts_tx, pkts_rx)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, r)
        conn.commit()
        cur.close()
        conn.close()
        print("Data berhasil dimasukkan ke database.", file=sys.stderr)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}", file=sys.stderr)


def test_ryu_connection():
    """Test connection to Ryu REST API"""
    print("Testing connection to Ryu controller...", file=sys.stderr)
    try:
        response = requests.get(f"{RYU_REST}/stats/desc/1", timeout=5)
        if response.status_code == 200:
            print("Koneksi ke Ryu controller BERHASIL", file=sys.stderr)
            return True
        else:
            print(f"Koneksi ke Ryu controller GAGAL: Status {response.status_code}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"Koneksi ke Ryu controller GAGAL: {e}", file=sys.stderr)
        return False


def test_db_connection():
    """Test connection to PostgreSQL database"""
    print("Testing connection to database...", file=sys.stderr)
    try:
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        print("Koneksi ke database BERHASIL", file=sys.stderr)
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Koneksi ke database GAGAL: {error}", file=sys.stderr)
        return False


if __name__ == "__main__":
    print("Starting collector...", file=sys.stderr)
    
    # Test connections first
    if not test_ryu_connection():
        print("Tidak dapat terhubung ke Ryu controller. Pastikan Ryu sedang berjalan.", file=sys.stderr)
        sys.exit(1)
        
    if not test_db_connection():
        print("Tidak dapat terhubung ke database. Pastikan PostgreSQL sedang berjalan.", file=sys.stderr)
        sys.exit(1)
    
    print("Collector ready. Starting collection loop...", file=sys.stderr)
    
    while True:
        rows = collect_flows()
        if rows:
            insert_pg(rows)
        else:
            print("Tidak ada data yang dikumpulkan. Pastikan:", file=sys.stderr)
            print("1. Ada traffic jaringan yang mengalir", file=sys.stderr)
            print("2. Ryu controller sudah dimodifikasi untuk match IP", file=sys.stderr)
            print("3. Hosts generate traffic ke aplikasi yang dimonitor", file=sys.stderr)
        time.sleep(5)  # Collect every 5 seconds