#!/usr/bin/env python3
"""
Burst Traffic Generator - Dataset 2 (Scheduled)
Perubahan dari versi random:
  - TIDAK random — pakai jadwal tetap (scheduled)
  - 1 jam = 2–4 burst saja, tersebar merata
  - Idle dihitung otomatis agar burst berikutnya tepat waktu
  - Urutan siklus sudah ditentukan (deterministik)
"""
import time
import subprocess
import json

H3 = "h3"
DST_IP = "10.0.0.2"
PORT = 9001
STATE_FILE = '/tmp/controller_state.json'

# ─────────────────────────────────────────────────────────────────────────────
#  JADWAL BURST — offset dalam detik dari awal jam (00:00)
#  Pilih salah satu pola, uncomment yang diinginkan
# ─────────────────────────────────────────────────────────────────────────────

# Pola 2x per jam — burst di menit 5 dan 35
# BURST_OFFSETS = [5*60, 35*60]

# Pola 3x per jam — burst di menit 5, 25, 48
# BURST_OFFSETS = [5*60, 25*60, 48*60]

# Pola 4x per jam — burst di menit 2, 17, 35, 50
BURST_OFFSETS = [2*60, 17*60, 35*60, 50*60]

# ─────────────────────────────────────────────────────────────────────────────
#  SIKLUS — urutan tetap, tidak diacak
# ─────────────────────────────────────────────────────────────────────────────

CYCLE_A = [
    (800,  10),
    (3500, 10),
    (7200, 25),
    (6800, 20),
    (8500, 30),
    (6000, 20),
    (4200, 25),
    (2800, 30),
    (1500, 35),
    (700,  40),
    (300,  50),
]

CYCLE_B = [
    (1200, 15),
    (4500, 15),
    (7800, 40),
    (8200, 35),
    (8000, 30),
    (7500, 25),
    (900,  10),
    (2200, 20),
    (1800, 25),
    (600,  30),
]

CYCLE_C = [
    (500,  15),
    (3800, 20),
    (2000, 15),
    (6500, 25),
    (3500, 15),
    (5200, 20),
    (2500, 20),
    (1200, 30),
    (400,  40),
]

CYCLE_D = [
    (300,  30),
    (600,  25),
    (1100, 20),
    (2000, 20),
    (3500, 15),
    (5500, 15),
    (7800, 20),
    (8800, 15),
    (3000, 10),
    (800,  10),
    (250,  15),
]

CYCLE_E = [
    (400,  20),
    (8000, 12),
    (500,  15),
    (7500, 12),
    (600,  15),
    (6800, 15),
    (1500, 20),
    (500,  25),
]

ALL_CYCLES = [
    ("A - FastRise DualPeak SlowFall", CYCLE_A),
    ("B - Plateau SuddenDrop",         CYCLE_B),
    ("C - TriplePeak Asymmetric",      CYCLE_C),
    ("D - SlowRise FastFall",          CYCLE_D),
    ("E - AggressiveSpike",            CYCLE_E),
]

def burst_duration(cycle):
    """Hitung total durasi burst dalam detik."""
    return sum(d for _, d in cycle)

# ─────────────────────────────────────────────────────────────────────────────

def check_controller_state():
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        current_state = state.get('state', 'IDLE')
        transition_states = [
            'DELETING_ALL_FLOWS', 'WAITING_SETTLE', 'INSTALLING_NEW_PATH',
            'REVERT_DELETING', 'REVERT_SETTLE', 'REVERT_INSTALLING'
        ]
        if current_state in transition_states:
            print(f"[BURST] ⏸️  Controller in {current_state}")
            return False
        if state.get('congestion') == True and current_state == 'ACTIVE_REROUTE':
            print("[BURST] ⏸️  Reroute active, waiting 3s...")
            time.sleep(3)
        return True
    except:
        return True

def send_tcp(rate, duration):
    try:
        h3_pid = subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()
        bitrate = rate * 1400 * 8
        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3", "-c", DST_IP, "-p", str(PORT),
            "-b", str(bitrate), "-t", str(duration)
        ], timeout=duration + 5)
    except Exception as e:
        print(f"[TCP] Error: {e}")

def run_bursts(burst_list, label=""):
    for rate, duration in burst_list:
        if not check_controller_state():
            time.sleep(5)
            continue
        mbps = rate * 1400 * 8 / 1e6
        if rate >= 6000:
            print(f"[TCP] 🔥 PEAK   {label} rate={rate}  ({mbps:.1f} Mbps)  dur={duration}s")
        elif rate >= 2000:
            print(f"[TCP] ⚡ MID    {label} rate={rate}  ({mbps:.1f} Mbps)  dur={duration}s")
        else:
            print(f"[TCP] 🌊 LOW    {label} rate={rate}  ({mbps:.1f} Mbps)  dur={duration}s")
        send_tcp(rate, duration)

# ─────────────────────────────────────────────────────────────────────────────
#  SCHEDULER UTAMA
#  Logika:
#    1. Catat waktu mulai jam ini (epoch rounded ke jam)
#    2. Hitung kapan setiap burst harus mulai (epoch absolut)
#    3. Tidur sampai waktu burst berikutnya
#    4. Jalankan burst, lalu tunggu burst berikutnya, dst.
#    5. Di akhir jam, reset ke jam berikutnya
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("[TCP] 🎯 Burst Generator Dataset 2 — SCHEDULED MODE")
    print(f"[TCP] Jadwal burst per jam (offset): {[f'{o//60}m{o%60:02d}s' for o in BURST_OFFSETS]}")
    print(f"[TCP] Total burst per jam: {len(BURST_OFFSETS)}x")
    time.sleep(600)   # warm-up awal sama seperti versi asli

    global_cycle = 0   # indeks siklus — berurutan, tidak acak

    while True:
        # ── Tentukan awal jam sekarang ──────────────────────────────────────
        now = time.time()
        hour_start = now - (now % 3600)   # epoch awal jam ini

        print(f"\n{'='*55}")
        print(f"[TCP] 🕐 Jam baru mulai, {len(BURST_OFFSETS)} burst dijadwalkan")
        print(f"{'='*55}")

        for slot_idx, offset in enumerate(BURST_OFFSETS):
            target_time = hour_start + offset

            # Hitung siklus yang akan dipakai (berurutan, wrap-around)
            label, bursts = ALL_CYCLES[global_cycle % len(ALL_CYCLES)]
            burst_dur = burst_duration(bursts)

            # Pastikan burst selesai sebelum slot berikutnya
            if slot_idx + 1 < len(BURST_OFFSETS):
                next_offset = BURST_OFFSETS[slot_idx + 1]
                gap = next_offset - offset
                if burst_dur > gap * 0.85:
                    print(f"[TCP] ⚠️  Burst terlalu panjang ({burst_dur}s) untuk gap {gap}s, dipangkas")
                    # Pangkas ekor burst agar muat
                    trimmed, total = [], 0
                    for r, d in bursts:
                        if total + d > gap * 0.80:
                            break
                        trimmed.append((r, d))
                        total += d
                    bursts = trimmed

            # ── Tunggu sampai waktu burst ───────────────────────────────────
            wait = target_time - time.time()
            if wait > 0:
                print(f"\n[TCP] ⏳ Slot {slot_idx+1}/{len(BURST_OFFSETS)} — "
                      f"tunggu {wait:.0f}s → mulai jam+{offset//60}m{offset%60:02d}s")
                time.sleep(wait)
            else:
                print(f"\n[TCP] ⚠️  Slot {slot_idx+1} terlambat {-wait:.0f}s, langsung jalan")

            # ── Jalankan burst ──────────────────────────────────────────────
            print(f"[TCP] 🔁 Cycle #{global_cycle+1} | {label}")
            run_bursts(bursts, label=f"[{label}]")
            global_cycle += 1

        # ── Tunggu sampai awal jam berikutnya ──────────────────────────────
        next_hour = hour_start + 3600
        wait_next = next_hour - time.time()
        if wait_next > 0:
            print(f"\n[TCP] 💤 Selesai semua slot, tunggu {wait_next:.0f}s ke jam berikutnya")
            time.sleep(wait_next)