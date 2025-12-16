#!/usr/bin/python3
"""
[BACKGROUND TRAFFIC GENERATOR]
File ini dijalankan OLEH HOST H2 di dalam Mininet.
Fungsi: Mengirim trafik TCP ke H4 dengan pola dinamis (Tidak Monoton).
Pola: Sine Wave (Trend) + Random Noise + Micro-bursts Spikes.
"""

import os
import time
import math
import random
import sys

# Target Configuration (Sesuai Proposal/Database)
TARGET_IP = "10.0.0.4" # H4
TARGET_PORT = 80       # Port Tujuan
SOURCE_PORT = 4000     # Port Asal (Agar tp_src tetap)
PROTOCOL = "TCP"

def generate_dynamic_traffic():
    print(f"*** [Background] Mulai mengirim trafik dinamis ke {TARGET_IP}:{TARGET_PORT}...")
    
    start_time = time.time()
    
    try:
        while True:
            # 1. Waktu berjalan (untuk pola Sinus)
            elapsed = time.time() - start_time
            
            # 2. Base Pattern: Gelombang Sinus (Naik turun perlahan setiap 60 detik)
            # Nilai sin berkisar -1 s.d 1. Kita geser biar positif.
            # Range dasar: 1000 pps s.d 3000 pps
            sine_val = math.sin(elapsed / 10.0) # Perioda gelombang
            base_rate = 2000 + (sine_val * 1000) 
            
            # 3. Randomness: Tambahkan Noise Acak (+/- 500 pps)
            noise = random.randint(-500, 500)
            current_rate = int(base_rate + noise)
            
            # 4. MICRO-BURST LOGIC (Forecasting Challenge)
            # Ada kemungkinan 10% terjadi lonjakan drastis tiba-tiba
            is_burst = False
            if random.random() < 0.15: # 15% chance
                current_rate = random.randint(6000, 9000) # SPIKE TINGGI!
                is_burst = True
            
            # Pastikan rate tidak minus
            if current_rate < 100: current_rate = 100
            
            # 5. Eksekusi D-ITG
            # Kita kirim potongan trafik pendek (misal 2 detik) dengan rate yg baru dihitung
            # -t 2000: durasi 2000ms (2 detik)
            burst_msg = "!!! MICRO-BURST !!!" if is_burst else "Normal Fluctuation"
            print(f"   -> Rate: {current_rate} pps | {burst_msg}")
            
            # Command ITGSend
            # -T TCP: Protokol
            # -a: Alamat tujuan
            # -rp: Port penerima (80)
            # -sp: Port pengirim (4000) - PENTING BUAT DB
            # -C: Constant rate (untuk durasi pendek ini)
            # -c: Ukuran paket (512 bytes)
            # -t: Durasi kirim dalam ms
            cmd = (f"ITGSend -T {PROTOCOL} -a {TARGET_IP} -rp {TARGET_PORT} -sp {SOURCE_PORT} "
                   f"-C {current_rate} -c 512 -t 2000 > /dev/null 2>&1")
            
            os.system(cmd)
            
            # Jeda dikit antar batch biar napas
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[Background] Stopped.")

if __name__ == "__main__":
    generate_dynamic_traffic()