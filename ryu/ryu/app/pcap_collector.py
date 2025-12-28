#!/usr/bin/python3

"""
PCAP COLLECTOR – FIXED FOR LINUX_SLL2 (Raw Packets)
- Input  : /tmp/voip.pcap
- Output : PostgreSQL
- Works  : Mininet + Ryu + D-ITG + tcpdump -i any
"""

from scapy.all import rdpcap
from scapy.layers.inet import IP, UDP
import psycopg2
from datetime import datetime

# ================== DB CONFIG ==================
DB_CONN = {
    "dbname": "development",
    "user": "dev_one",
    "password": "hijack332.",
    "host": "127.0.0.1"
}

PCAP_FILE = "/tmp/voip.pcap"

# ================== MAIN ==================
def main():
    print("[*] Reading PCAP...")
    pkts = rdpcap(PCAP_FILE)
    print(f"[*] Total packets: {len(pkts)}")

    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    insert_q = """
        INSERT INTO pcap_logs (
            time,
            source,
            protocol,
            length,
            "Arrival Time",
            info,
            "No.",
            destination
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    pkt_no = 1
    base_time = None

    inserted = 0

    for pkt in pkts:
        raw = bytes(pkt)

        # === MANUAL DECODE (KUNCI FIX) ===
        try:
            ip = IP(raw)
            if ip.proto != 17:
                continue
            udp = ip[UDP]
        except:
            continue

        # Time handling
        if base_time is None:
            base_time = pkt.time

        rel_time = pkt.time - base_time
        arrival_time = datetime.fromtimestamp(pkt.time)

        src_ip = ip.src
        dst_ip = ip.dst
        length = len(raw)

        info = f"{udp.sport}  >  {udp.dport} Len={len(udp.payload)}"

        cur.execute(insert_q, (
            round(rel_time, 6),
            src_ip,
            "UDP",
            length,
            arrival_time,
            info,
            pkt_no,
            dst_ip
        ))

        pkt_no += 1
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"[✓] INSERTED {inserted} ROWS INTO DATABASE")

# ================== RUN ==================
if __name__ == "__main__":
    main()
