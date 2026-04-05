#!/usr/bin/env python3
"""
Structured Stochastic Traffic Generator
Target throughput ~55 Mbps (bottleneck link spine-leaf).
  - Packet size  : 1400 bytes  (fixed di send_tcp)
  - 55 Mbps      : 55_000_000 / (1400*8) ≈ 4910 pps  → kita pakai 4500 pps sebagai "full load"
  - Noise        : ±8% Gaussian (bukan ±18%)
  - Event        : masih ada, tapi dibatasi supaya tidak melewati 4800 pps
  - Diurnal      : peak sore/malam ~4200–4500 pps, offpeak ~1200–1800 pps
"""
import time
import subprocess
import json
import random
import math

H3 = "h3"
DST_IP = "10.0.0.2"
PORT = 9001
STATE_FILE = '/tmp/controller_state.json'

# ─── Batas keras agar tidak meledak melebihi kapasitas link ─────────────────
LINK_CAP_PPS = 4800   # ~53.8 Mbps  — hard ceiling
FLOOR_PPS    = 400    # minimum bermakna agar koneksi tetap hidup

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
            "iperf3",
            "-c", DST_IP,
            "-p", str(PORT),
            "-b", str(bitrate),
            "-t", str(duration)
        ], timeout=duration + 10)  # tambah buffer 10 detik
    except subprocess.TimeoutExpired:
        print(f"[TCP] ⚠️ iperf3 timeout (dur={duration}s), skipping")
    except Exception as e:
        print(f"[TCP] Error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  DIURNAL BASELINE  (pps)
#
#  Skala ulang dari versi lama ke range 400–4500 pps.
#  Peak sore = 4500 pps ≈ 50.4 Mbps  (sengaja sedikit di bawah 55 Mbps)
#  Offpeak dini hari = 400 pps ≈ 4.5 Mbps
#
#  Mapping kasar lama→baru:
#    lama 4000 pps → baru 4500 pps  (peak sore)
#    lama  150 pps → baru  400 pps  (dini hari)
# ─────────────────────────────────────────────────────────────────────────────
DIURNAL_BASELINE = [
     400,   350,   320,   300,   # 00–03 dini hari
     320,   480,   800,  1400,   # 04–07 mulai naik
    2200,  3000,  3600,  4000,   # 08–11 rampup pagi
    4200,  4400,  3800,  4000,   # 12–15 peak siang
    4300,  4500,  4200,  3600,   # 16–19 peak sore ← target ~55 Mbps
    2800,  2000,  1200,   700,   # 20–23 turun malam
]

def get_baseline_for_hour(hour):
    """Interpolasi smooth antar jam (mendukung hour float)."""
    hour = hour % 24
    h_int  = int(math.floor(hour))
    h_next = (h_int + 1) % 24
    frac   = hour - h_int
    h0 = DIURNAL_BASELINE[h_int]
    h1 = DIURNAL_BASELINE[h_next]
    return h0 + (h1 - h0) * frac

def natural_noise(base, day_factor=1.0):
    """
    Gaussian noise ±8% (dikurangi dari ±18% agar tidak terlalu liar).
    day_factor tidak mengubah std, hanya menggeser mean.
    """
    std = base * 0.08 * day_factor
    val = int(random.gauss(base * day_factor, std))
    return max(FLOOR_PPS, min(val, LINK_CAP_PPS))

def occasional_event():
    """
    Kejadian tak terduga — probabilitas diperkecil dan multiplier diklem
    agar tidak melewati LINK_CAP_PPS.
    """
    roll = random.random()
    if roll < 0.04:       # 4% — flash crowd / viral content
        print("[TCP] 🚨 EVENT: Flash crowd!")
        # multiplier dikecilkan: maks 1.06 agar pps tidak melebihi cap
        return (random.uniform(1.02, 1.06), random.randint(20, 40))
    elif roll < 0.07:     # 3% — batch job burst
        print("[TCP] 🔧 EVENT: Batch job burst!")
        return (random.uniform(1.01, 1.04), random.randint(15, 30))
    elif roll < 0.10:     # 3% — traffic drop partial outage
        print("[TCP] 📉 EVENT: Partial drop!")
        return (random.uniform(0.30, 0.55), random.randint(10, 25))
    return None           # 90% — normal

def build_period(hour, n_bursts, day_factor=1.0):
    """
    Buat satu period traffic berdasarkan jam.
    Tiap burst masih punya noise, tapi tetap terkontrol di sekitar baseline.
    """
    base = get_baseline_for_hour(hour)
    bursts = []

    for i in range(n_bursts):
        rate = natural_noise(base, day_factor)

        # Micro-trend dalam 1 period — bounded ±10%
        trend = random.choice(['up', 'down', 'flat', 'flat'])
        if trend == 'up':
            base = min(base * random.uniform(1.02, 1.08), LINK_CAP_PPS)
        elif trend == 'down':
            base = max(base * random.uniform(0.92, 0.98), FLOOR_PPS)

        # Duration stochastic tapi lebih stabil (gauss 30±6, clamp 12–55)
        dur = int(random.gauss(30, 6))
        dur = max(12, min(dur, 55))

        # Check occasional event
        event = occasional_event()
        if event:
            mult, event_dur = event
            rate = int(min(rate * mult, LINK_CAP_PPS))
            dur  = event_dur

        bursts.append((rate, dur))

    return bursts

def simulate_day_type():
    """
    Pilih tipe hari — mempengaruhi overall traffic level.
    Factor dikurangi range-nya agar tidak drop/spike terlalu jauh.
    """
    roll = random.random()
    if roll < 0.08:
        print("[TCP] 📅 Day type: HOLIDAY (traffic rendah)")
        return 0.55, "HOLIDAY"
    elif roll < 0.22:
        print("[TCP] 📅 Day type: WEEKEND")
        return 0.72, "WEEKEND"
    elif roll < 0.38:
        print("[TCP] 📅 Day type: LIGHT WEEKDAY")
        return 0.88, "LIGHT_WEEKDAY"
    elif roll < 0.85:
        print("[TCP] 📅 Day type: NORMAL WEEKDAY")
        return 1.00, "NORMAL_WEEKDAY"
    else:
        print("[TCP] 📅 Day type: BUSY DAY (event/promo)")
        return 1.12, "BUSY_DAY"   # dikurangi dari 1.35 → 1.12

# Period schedule — jam berapa, berapa burst, nama label
PERIOD_SCHEDULE = [
    (0,   4,  "offpeak"),
    (2,   4,  "offpeak"),
    (5,   5,  "earlymorning"),
    (7,   5,  "rampup"),
    (9,   6,  "morning_peak"),
    (11,  6,  "midday"),
    (13,  5,  "lunch"),
    (14,  6,  "afternoon"),
    (16,  7,  "evening_peak"),
    (18,  6,  "after_work"),
    (20,  5,  "night"),
    (22,  4,  "latenight"),
]

# Batas display untuk label pps
PEAK_THRESHOLD = int(LINK_CAP_PPS * 0.80)   # ≥ 3840 pps → PEAK
MID_THRESHOLD  = int(LINK_CAP_PPS * 0.35)   # ≥ 1680 pps → MID

if __name__ == "__main__":
    print("[TCP] 🌐 Structured Stochastic Traffic Generator Started")
    print(f"[TCP] 🎯 Target link cap  : {LINK_CAP_PPS} pps  "
          f"({LINK_CAP_PPS * 1400 * 8 / 1e6:.1f} Mbps)")
    print(f"[TCP] 🎯 Peak diurnal     : {max(DIURNAL_BASELINE)} pps  "
          f"({max(DIURNAL_BASELINE) * 1400 * 8 / 1e6:.1f} Mbps)")
    time.sleep(600)

    day = 0
    while True:
        day += 1
        day_factor, day_label = simulate_day_type()

        print(f"\n{'='*60}")
        print(f"[TCP] 📅 Day #{day} | {day_label} | factor={day_factor:.2f}")
        print(f"{'='*60}")

        # Geser waktu tiap period ±20 menit (dikurangi dari ±30)
        schedule = list(PERIOD_SCHEDULE)
        schedule = [(h + random.uniform(-0.33, 0.33), n, name)
                    for h, n, name in schedule]

        for hour, n_bursts, period_name in schedule:
            if not check_controller_state():
                time.sleep(5)
                continue

            print(f"\n[TCP] 🕐 Period: {period_name.upper()} (~hour {hour:.1f})")
            bursts = build_period(hour, n_bursts, day_factor)

            for rate, dur in bursts:
                if not check_controller_state():
                    time.sleep(5)
                    continue

                mbps = rate * 1400 * 8 / 1e6
                if rate >= PEAK_THRESHOLD:
                    print(f"[TCP] 🔥 PEAK   rate={rate} pps  ({mbps:.1f} Mbps)  dur={dur}s")
                elif rate >= MID_THRESHOLD:
                    print(f"[TCP] ⚡ MID    rate={rate} pps  ({mbps:.1f} Mbps)  dur={dur}s")
                else:
                    print(f"[TCP] 🌊 LOW    rate={rate} pps  ({mbps:.1f} Mbps)  dur={dur}s")

                send_tcp(rate, dur)

                # Gap antar burst: lebih kecil variasinya (gauss 4±1.5)
                gap = max(1, int(random.gauss(4, 1.5)))
                time.sleep(gap)

            # Jeda antar period
            if hour >= 22 or hour <= 4:
                rest = random.gauss(180, 20)
            elif hour >= 16:
                rest = random.gauss(60, 10)
            else:
                rest = random.gauss(45, 8)

            rest = max(10, int(rest))
            print(f"[TCP] 💤 Rest {rest}s after {period_name}")
            time.sleep(rest)

        print(f"\n[TCP] 🔁 Day #{day} done\n")