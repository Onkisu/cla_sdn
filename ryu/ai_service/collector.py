#!/usr/bin/python3
import requests
import time
import sys
from collections import defaultdict

# ---------------------- KONFIGURASI ----------------------
RYU_REST = "http://127.0.0.1:8080"
DPID_S1 = 1 # DPID 's1'
POLL_INTERVAL = 2 # Detik (kita percepat biar keliatan)

print(f"Memulai collector_DEBUG.py (Print Raw Flow Stats, poll tiap {POLL_INTERVAL} detik)...")

# Mapping APLIKASI ke IP Client (dari topo.py)
HOST_IP_TO_APP = {
    '10.0.0.1': 'youtube',
    '10.0.0.2': 'netflix',
    '10.0.0.3': 'twitch'
}

# ---------------------- MAIN LOOP ----------------------
if __name__ == "__main__":
    
    while True:
        print(f"\n--- Polling Ryu at {time.strftime('%H:%M:%S')} ---")
        found_traffic = False
        
        try:
            res_flow = requests.get(f"{RYU_REST}/stats/flow/{DPID_S1}", timeout=3).json()
        except Exception as e:
            print(f"Error fetch FLOW dpid {DPID_S1}: {e}", file=sys.stderr)
            time.sleep(POLL_INTERVAL)
            continue # Coba lagi nanti

        if str(DPID_S1) not in res_flow:
            print("DPID 1 not found in response.")
            time.sleep(POLL_INTERVAL)
            continue

        # === Iterasi SEMUA flow, cari dari h1/h2/h3 ===
        flows_found_this_poll = defaultdict(list) # Untuk nyimpen flow per app
        
        for flow in res_flow[str(DPID_S1)]:
            match = flow.get("match", {})
            src_ip = match.get("ipv4_src") or match.get("nw_src")
            
            # Cari tau ini flow app apa (berdasarkan IP client)
            app_name = HOST_IP_TO_APP.get(src_ip)
            
            # Kalo bukan dari h1/h2/h3, skip
            if not app_name:
                continue

            # Dapatkan data byte dan packet
            current_bytes = flow.get("byte_count", 0)
            current_pkts = flow.get("packet_count", 0)
            dst_ip = match.get("ipv4_dst") or match.get("nw_dst", "N/A")
            tp_src = match.get("udp_src") or match.get("tcp_src", "N/A")
            tp_dst = match.get("udp_dst") or match.get("tcp_dst", "N/A")

            # Cetak data mentah yang kita lihat
            print(f"  [RAW] App: {app_name:<7} | Src: {src_ip}:{tp_src} -> Dst: {dst_ip}:{tp_dst} | Bytes: {current_bytes:<12} | Pkts: {current_pkts:<8}")
            flows_found_this_poll[app_name].append(flow) # Simpan flow ini
            found_traffic = True

        if not found_traffic:
            print("  No traffic found from h1, h2, or h3 in this poll.")
            
        # Optional: Hitung total per app (kayak AGG DELTA, tapi cuma buat display)
        if found_traffic:
             print("  --- Totals This Poll ---")
             for app, flows in flows_found_this_poll.items():
                  total_bytes = sum(f.get("byte_count", 0) for f in flows)
                  total_pkts = sum(f.get("packet_count", 0) for f in flows)
                  print(f"    {app:<7}: Total Bytes={total_bytes:<12} | Total Pkts={total_pkts:<8} ({len(flows)} flows)")


        # Tunggu sebelum poll berikutnya
        time.sleep(POLL_INTERVAL)
