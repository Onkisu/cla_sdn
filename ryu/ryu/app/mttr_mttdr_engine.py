import time
import psycopg2
import subprocess
from datetime import datetime
import sys

# --- CONFIG ---
DB_HOST = "103.181.142.165"
DB_PORT = 5432
DB_USER = "dev_one"
DB_PASS = "hijack332."
DB_NAME = "development"

# Threshold untuk memicu Intent
JITTER_THRESHOLD_MS = 15.0 
CHECK_INTERVAL = 2  # Cek DB setiap 2 detik (agresif demi target < 4s)

# Global Variables untuk Tracking Waktu
t_attack_start = None   # Diisi manual/auto saat serangan mulai
t_intent_created = None
t_intent_applied = None
t_system_stabilized = None

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)

def execute_mitigation_action(category):
    """
    Eksekusi perintah ke Mininet/OVS untuk membatasi bandwidth.
    Contoh: Limit interface host terkait menjadi 1Mbps.
    """
    print(f"   [ACTUATOR] Applying mitigation for {category}...")
    
    # Mapping Kategori ke Interface Mininet (Sesuaikan dengan topologi Anda)
    # Misal 'web_app' traffic lewat 's1-eth1'
    interface = "tor1-eth1" # Contoh interface
    
    # Perintah OVS untuk Rate Limiting (Ingress Policing)
    # Set rate ke 1000 Kbps (1 Mbps) burst 100kb
    cmd = f"sudo ovs-vsctl set Interface {interface} ingress_policing_rate=1000"
    cmd_burst = f"sudo ovs-vsctl set Interface {interface} ingress_policing_burst=100"
    
    try:
        subprocess.run(cmd, shell=True, check=True)
        subprocess.run(cmd_burst, shell=True, check=True)
        print(f"   [ACTUATOR] SUCCESS: Rate limit applied on {interface}")
        return True
    except Exception as e:
        print(f"   [ACTUATOR] FAILED: {e}")
        return False

def cla_loop():
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=== CLOSED LOOP AUTOMATION ENGINE STARTED ===")
    print(f"Monitoring Jitter > {JITTER_THRESHOLD_MS} ms...")

    active_incident = False
    
    while True:
        try:
            # 1. MONITORING (Detection Phase)
            # Ambil data raw 5 detik terakhir (bukan hourly!)
            cur.execute("""
                SELECT timestamp, category, jitter_ms 
                FROM traffic.flow_stats 
                ORDER BY timestamp DESC LIMIT 1
            """)
            row = cur.fetchone()
            
            if row:
                ts, category, jitter = row
                current_time = datetime.now()
                
                # --- DETEKSI INSIDEN ---
                if jitter > JITTER_THRESHOLD_MS and not active_incident:
                    print(f"\n[!] ANOMALY DETECTED! Jitter: {jitter}ms at {ts}")
                    active_incident = True
                    
                    # 2. GENERATE INTENT (MTTDR END)
                    intent_sql = """
                        INSERT INTO traffic.intent_logs 
                        (category, intent_type, status, priority, justification, created_at, updated_at)
                        VALUES (%s, 'MITIGATE_CONGESTION', 'PENDING', 'HIGH', %s, NOW(), NOW())
                        RETURNING id, created_at;
                    """
                    cur.execute(intent_sql, (category, f"Auto-detected High Jitter: {jitter}ms"))
                    conn.commit()
                    
                    intent_id, created_at = cur.fetchone()
                    global t_intent_created
                    t_intent_created = created_at
                    print(f"    -> Intent ID {intent_id} CREATED at {created_at}")
                    
                    # --- EXECUTION (MTTR START) ---
                    # Langsung eksekusi saat itu juga (Zero Touch Automation)
                    success = execute_mitigation_action(category)
                    
                    if success:
                        # Update status Intent jadi APPLIED
                        update_sql = "UPDATE traffic.intent_logs SET status = 'APPLIED', updated_at = NOW() WHERE id = %s RETURNING updated_at"
                        cur.execute(update_sql, (intent_id,))
                        conn.commit()
                        global t_intent_applied
                        t_intent_applied = cur.fetchone()[0]
                        print(f"    -> Intent APPLIED at {t_intent_applied}")

                # --- VERIFIKASI (MTTR END) ---
                elif jitter < JITTER_THRESHOLD_MS and active_incident:
                    print(f"\n[OK] SYSTEM STABILIZED. Jitter: {jitter}ms")
                    active_incident = False
                    global t_system_stabilized
                    t_system_stabilized = datetime.now()
                    
                    # HITUNG SKOR AKHIR
                    if t_intent_created and t_intent_applied:
                        # Hitung manual MTTDR jika kita tau kapan serangan dimulai (input manual)
                        print("-" * 30)
                        print("       REPORT CARD")
                        print("-" * 30)
                        print(f"1. Intent Created : {t_intent_created.time()}")
                        print(f"2. Action Applied : {t_intent_applied.time()}")
                        print(f"3. System Stable  : {t_system_stabilized.time()}")
                        
                        mttr = (t_system_stabilized - t_intent_applied).total_seconds() if t_intent_applied else 0
                        # Koreksi time difference (kadang timezone DB beda dgn local)
                        # Kita ambil selisih relatif saja
                        
                        print(f"\n>> MTTR (Applied -> Stable): {abs(mttr):.2f} seconds")
                        if abs(mttr) < 5:
                            print(">> RESULT: PASSED (Target < 5s) ✅")
                        else:
                            print(">> RESULT: FAILED (Too slow) ❌")
                        print("-" * 30)

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            print(f"Error loop: {e}")
            conn = get_db_connection() # Reconnect
            cur = conn.cursor()
            time.sleep(1)

if __name__ == "__main__":
    cla_loop()
```

### Cara Menjalankan Skenario (Manual Test)

Untuk mendapatkan angka PDR, MTTR, dan MTTDR yang valid, ikuti langkah ini:

#### 1. Persiapan
* Jalankan Topologi Mininet: `sudo python3 mininet_script_anda.py`
* Jalankan Collector (Monitoring): `python3 collector_dc.py`
* Jalankan **CLA Engine** (Script di atas): `python3 cla_engine.py`

#### 2. Mulai Serangan (Uji Coba)
Buka terminal baru, masuk ke Mininet CLI atau gunakan script serangan terpisah. Kita akan membuat *congestion* di link `web_app`.

```bash
# Di terminal Mininet (mininet>)
# Tembak trafik UDP besar-besaran agar Jitter naik & Packet Loss terjadi
h1 iperf -u -c 10.10.1.1 -b 100M -t 10
```
*(Catatan: Sesuaikan `h1` dan IP target dengan topologi Anda. Pastikan bandwidth attack melebihi kapasitas link agar terjadi antrian/jitter).*

#### 3. Pantau Output `cla_engine.py`
Anda akan melihat log seperti ini secara otomatis:

```text
[!] ANOMALY DETECTED! Jitter: 45.2ms at 10:00:03
    -> Intent ID 55 CREATED at 10:00:03
   [ACTUATOR] Applying mitigation...
   [ACTUATOR] SUCCESS: Rate limit applied
    -> Intent APPLIED at 10:00:04

... (menunggu beberapa detik) ...

[OK] SYSTEM STABILIZED. Jitter: 2.1ms
       REPORT CARD
------------------------------
>> MTTR (Applied -> Stable): 3.50 seconds
>> RESULT: PASSED (Target < 5s) ✅
```

#### 4. Validasi PDR (Packet Delivery Ratio)
Untuk PDR, Anda lihat output `iperf` di terminal penyerang setelah tes selesai.

```text
Server Report:
   0.0-10.0 sec  12.5 MBytes  10.5 Mbits/sec   0.023 ms  40/10000 (0.4%)