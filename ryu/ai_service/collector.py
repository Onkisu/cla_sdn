#!/usr/bin/python3
import requests
import psycopg2
import time
import sys

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
            print(f"Data mentah dari Ryu (dpid {dpid}):\n{res}", file=sys.stderr)
        except requests.exceptions.RequestException as e:
            # Print an error and continue to the next switch if the request fails
            print(f"Error fetching flow stats for dpid {dpid}: {e}", file=sys.stderr)
            continue

        # Iterate through the flows for the current switch
        for flow in res.get(str(dpid), []):
            # Try to get the source and destination IPs from the flow match
            src_ip = flow['match'].get('ipv4_src')
            dst_ip = flow['match'].get('ipv4_dst')

            # Determine the host and application based on which IP is in the mapping.
            host = 'unknown'
            app = 'unknown'

            if src_ip in mapping:
                host = src_ip
                app = mapping.get(src_ip)
            elif dst_ip in mapping:
                host = dst_ip
                app = mapping.get(dst_ip)
            else:
                # If neither the source nor destination IP matches, we can't identify the app.
                # DEBUG: Skip this flow if it doesn't match our criteria
                # print(f"Melewati flow yang tidak cocok: {flow['match']}", file=sys.stderr)
                continue

            # Map IP protocol numbers to names
            proto = {6: 'tcp', 17: 'udp'}.get(flow['match'].get('ip_proto', 0), 'any')

            # Use the byte and packet counts from the flow. Since we're collecting aggregated
            # stats from the switches, we'll use the same value for tx and rx.
            bytes_count = flow.get('byte_count', 0)
            pkts_count = flow.get('packet_count', 0)

            # Append the data to the rows list
            rows.append((host, app, proto, bytes_count, bytes_count, pkts_count, pkts_count))
    
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


if __name__ == "__main__":
    while True:
        rows = collect_flows()
        if rows:
            insert_pg(rows)
        else:
            print("Tidak ada data yang dikumpulkan. Memeriksa kembali dalam 5 detik.", file=sys.stderr)
        time.sleep(5)  # Collect every 5 seconds
