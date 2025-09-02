import requests, psycopg2, time

DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST = "http://127.0.0.1:8080"
DPIDS = [1,2,3]  # s1,s2,s3

def collect_flows():
    rows=[]
    for dpid in DPIDS:
        res = requests.get(f"{RYU_REST}/stats/flow/{dpid}", timeout=5).json()
        for flow in res.get(str(dpid), []):
            host = flow['match'].get('ipv4_dst','unknown')
            proto = {6:'tcp',17:'udp'}.get(flow['match'].get('ip_proto',0),'any')
            bytes_tx = flow.get('byte_count',0)
            pkts_tx = flow.get('packet_count',0)
            rows.append((host,'unknown',proto,bytes_tx,bytes_tx,pkts_tx,pkts_tx))
    return rows

def insert_pg(rows):
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

if __name__=="__main__":
    while True:
        rows = collect_flows()
        if rows:
            insert_pg(rows)
        time.sleep(5)  # collect tiap 5 detik
