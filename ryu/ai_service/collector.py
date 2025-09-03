import requests, psycopg2, time, subprocess, logging
from logging.handlers import RotatingFileHandler

# --- Logging setup (dengan rotating log) ---
log_handler = RotatingFileHandler(
    "collector.log", maxBytes=10*1024*1024, backupCount=5  # 10MB, keep 5 files
)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()]  # ke file + console
)

DB_CONN = "dbname=development user=dev_one password=hijack332 host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1,2,3]  # s1,s2,s3

last_bytes = {}
last_pkts = {}

# Mapping IP → aplikasi
APP_MAPPING = {
    "10.0.0.1": "youtube",
    "10.0.0.2": "netflix",
    "10.0.0.3": "twitch"
}

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
    except Exception as e:
        logging.warning(f"Latency check gagal ke {dst_ip}: {e}")
    return None

def resolve_app(ip):
    """Cocokkan IP ke host & app"""
    if not ip:
        return ("unknown", "unknown")
    return (ip, APP_MAPPING.get(ip, "unknown"))

def collect_flows():
    rows = []
    for dpid in DPIDS:
        url = f"{RYU_REST}/stats/flow/{dpid}"
        try:
            flows = requests.get(url, timeout=5).json().get(str(dpid), [])
        except Exception as e:
            logging.error(f"Get flow dpid={dpid} error: {e}")
            continue

        for f in flows:
            match = f.get("match", {})
            byte_count = f.get("byte_count", 0)
            pkt_count = f.get("packet_count", 0)

            src_ip = match.get("ipv4_src")
            dst_ip = match.get("ipv4_dst")
            src_mac = match.get("eth_src")
            dst_mac = match.get("eth_dst")
            proto   = {6: "tcp", 17: "udp"}.get(match.get("ip_proto"), "any")

            # Host & App (prioritaskan src_ip, kalau tidak ada pakai dst_ip)
            if src_ip:
                host, app = resolve_app(src_ip)
            else:
                host, app = resolve_app(dst_ip)

            # Key unik per arah flow
            key = (dpid, src_ip, dst_ip, proto)

            # Hitung delta
            delta_bytes = byte_count - last_bytes.get(key, 0)
            delta_pkts  = pkt_count  - last_pkts.get(key, 0)

            # Reset kalau negatif (counter reset/overflow)
            if delta_bytes < 0:
                delta_bytes = byte_count
            if delta_pkts < 0:
                delta_pkts = pkt_count

            last_bytes[key] = byte_count
            last_pkts[key]  = pkt_count

            if delta_bytes == 0 and delta_pkts == 0:
                continue  # skip yang nggak berubah

            # latency (optional)
            latency = None
            if dst_ip:
                latency = measure_latency(dst_ip)

            # --- Split TX/RX ---
            bytes_tx = delta_bytes if src_ip else 0
            bytes_rx = delta_bytes if dst_ip else 0
            pkts_tx  = delta_pkts  if src_ip else 0
            pkts_rx  = delta_pkts  if dst_ip else 0

            rows.append((
                dpid,
                host, app, proto,
                src_ip, dst_ip,
                src_mac, dst_mac,
                bytes_tx, bytes_rx,
                pkts_tx, pkts_rx,
                latency
            ))
    return rows

def insert_rows(rows):
    if not rows:
        return
    try:
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
        logging.info(f"Inserted {len(rows)} rows into flow_stats")
    except Exception as e:
        logging.error(f"DB insert error: {e}")

if __name__ == "__main__":
    while True:
        rows = collect_flows()
        insert_rows(rows)
        time.sleep(5)  # interval 5 detik
