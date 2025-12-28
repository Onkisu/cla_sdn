import psycopg2
from scapy.all import rdpcap, UDP, IP
from datetime import datetime

conn = psycopg2.connect(
    host="127.0.0.1",
    dbname="development",
    user="dev_one",
    password="hijack332."
)
cur = conn.cursor()

pkts = rdpcap("/tmp/voip.pcap")

start_time = pkts[0].time

pkt_no = 1
for p in pkts:
    if IP in p and UDP in p:
        rel_time = p.time - start_time
        src = p[IP].src
        dst = p[IP].dst
        length = len(p)
        arrival = datetime.fromtimestamp(p.time)

        info = f"{p[UDP].sport}  >  {p[UDP].dport} Len={len(p[UDP].payload)}"

        cur.execute("""
            INSERT INTO pcap_logs
            ("time","source",protocol,length,
             "Arrival Time",info,"No.",destination)
            VALUES (%s,%s,'UDP',%s,%s,%s,%s,%s)
        """, (
            rel_time, src, length,
            arrival, info, pkt_no, dst
        ))

        pkt_no += 1

conn.commit()
cur.close()
conn.close()
