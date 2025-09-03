import requests, psycopg2, time, subprocess, logging
from logging.handlers import RotatingFileHandler

# --- Logging setup ---
log_handler = RotatingFileHandler(
    "collector.log", maxBytes=10*1024*1024, backupCount=5
)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()]
)

DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1, 2, 3]

last_bytes = {}
last_pkts = {}

# Mapping IP → aplikasi
APP_MAPPING = {
    "10.0.0.1": "youtube",
    "10.0.0.2": "netflix",
    "10.0.0.3": "twitch",
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
        logging.debug(f"Latency check gagal ke {dst_ip}: {e}")
    return None

def resolve_app(ip):
    if not ip:
        return ("unknown", "unknown")
    return (ip, APP_MAPPING.get(ip, "unknown"))

def collect_flows():
    rows = []
    for dpid in DPIDS:
        url = f"{RYU_REST}/stats/flow/{dpid}"
        try:
            res = requests.get(url, timeout=5).json()
            flows = res.get(str(dpid), [])
        except Exception as e:
            logging.error(f"Get flow dpid={dpid} error: {e}")
            continue

        skipped_no_addr = 0
        skipped_zero_delta = 0
        taken = 0

        for f in flows:
            match = f.get("match", {})
            byte_count = f.get("byte_count", 0)
            pkt_count  = f.get("packet_count", 0)

            in_port = match.get("in_port")

            # --- Ambil IP (fallback ke nama lama 'nw_*' bila ada) ---
            src_ip  = match.get("ipv4_src") or match.get("nw_src")
            dst_ip  = match.get("ipv4_dst") or match.get("nw_dst")

            # MAC (kalau ada)
            src_mac = match.get("eth_src")
            dst_mac = match.get("eth_dst")

            # Proto: tcp/udp/icmp/any
            ip_proto_num = match.get("ip_proto")
            proto_map = {6: "tcp", 17: "udp", 1: "icmp"}
            proto = proto_map.get(ip_proto_num, "any")

            # Host & App (pakai src_ip dulu, kalau None pakai dst_ip)
            if src_ip:
                host, app = resolve_app(src_ip)
            else:
                host, app = resolve_app(dst_ip)

            # --- Flow valid kalau ada salah satu identitas (IP atau MAC) ---
            if not (src_ip or dst_ip or src_mac or dst_mac):
                skipped_no_addr += 1
                continue

            # --- Key unik per flow untuk delta ---
            key = (dpid, in_port, src_ip, dst_ip, proto)

            delta_bytes = byte_count - last_bytes.get(key, 0)
            delta_pkts  = pkt_count  - last_pkts.get(key, 0)

            # Reset jika counter turun (reset/overflow)
            if delta_bytes < 0:
                delta_bytes = byte_count
            if delta_pkts < 0:
                delta_pkts = pkt_count

            last_bytes[key] = byte_count
            last_pkts[key]  = pkt_count

            # Jika tidak ada perubahan, skip
            if delta_bytes == 0 and delta_pkts == 0:
                skipped_zero_delta += 1
                continue

            # --- Latency (optional; tetap aktif karena sebelumnya “aman”) ---
            latency = measure_latency(dst_ip) if dst_ip else None

            # --- Split TX/RX (satu baris = arah src → dst) ---
            bytes_tx = delta_bytes
            pkts_tx  = delta_pkts
            bytes_rx = 0
            pkts_rx  = 0

            rows.append((
                dpid,
                host, app, proto,
                src_ip, dst_ip,
                src_mac, dst_mac,
                bytes_tx, bytes_rx,
                pkts_tx, pkts_rx,
                latency
            ))
            taken += 1

        logging.info(
            f"DPID {dpid}: total_flows={len(flows)}, kept={taken}, "
            f"skip_no_addr={skipped_no_addr}, skip_zero_delta={skipped_zero_delta}"
        )

    return rows

def insert_rows(rows):
    if not rows:
        logging.info("No rows to insert (kept=0)")
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
        time.sleep(5)
