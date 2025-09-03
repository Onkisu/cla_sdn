import requests, psycopg2, time, subprocess

DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1,2,3]  # s1,s2,s3

last_bytes = {}
last_pkts = {}

def measure_latency(dst_ip):
    """Ping ke IP tujuan, return latency dalam ms"""
    try:
        out = subprocess.check_output(
            ["ping", "-c", "1", "-W", "1", dst_ip],
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        for line in out.split("\n"):
            if "time=" in line:
                return float(line.split("time=")[1].split()[0])
    except:
        return None

def collect_flows():
    rows = []
    for dpid in DPIDS:
        url = f"{RYU_REST}/stats/flow/{dpid}"
        try:
            flows = requests.get(url).json().get(str(dpid), [])
        except Exception as e:
            print(f"[ERROR] Get flow dpid={dpid}: {e}")
            continue

        for f in flows:
            match = f.get("match", {})
            actions = f.get("actions", [])
            byte_count = f.get("byte_count", 0)
            pkt_count = f.get("packet_count", 0)

            key = (dpid, match.get("in_port"), match.get("ipv4_src"), match.get("ipv4_dst"))

            # Hitung delta
            delta_bytes = byte_count - last_bytes.get(key, 0)
            delta_pkts = pkt_count - last_pkts.get(key, 0)

            # Reset jika negatif (counter reset/overflow)
            if delta_bytes < 0:
                delta_bytes = byte_count
            if delta_pkts < 0:
                delta_pkts = pkt_count

            last_bytes[key] = byte_count
            last_pkts[key] = pkt_count

            # latency (optional, pakai dst_ip kalau ada)
            latency = None
            if match.get("ipv4_dst"):
                latency = measure_latency(match.get("ipv4_dst"))

            rows.append((
                dpid,
                None,  # host mapping kalau ada
                None,  # app mapping kalau ada
                match.get("ip_proto"),
                match.get("ipv4_src"),
                match.get("ipv4_dst"),
                match.get("eth_src"),
                match.get("eth_dst"),
                delta_bytes,
                delta_bytes,  # sementara pakai sama (bisa split TX/RX kalau perlu)
                delta_pkts,
                delta_pkts,   # sementara pakai sama
                latency
            ))
    return rows

def insert_rows(rows):
    if not rows:
        return
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO traffic.flow_stats (
            dpid, host, app, proto, src_ip, dst_ip, src_mac, dst_mac,
            bytes_tx, bytes_rx, pkts_tx, pkts_rx, latency_ms
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    while True:
        rows = collect_flows()
        insert_rows(rows)
        time.sleep(5)  # interval 5 detik
