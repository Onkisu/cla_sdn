#!/usr/bin/env python3
"""
Structured Stochastic Traffic Generator
Realistic: pola harian tetap ada, tapi dengan variasi natural
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
        ])
    except Exception as e:
        print(f"[TCP] Error: {e}")

# ─────────────────────────────────────────────────────────
#  BASELINE ENVELOPE — target rate per "jam" dalam sehari
#  Ini representasi kurva diurnal (pola harian)
#  Index 0–23 = jam 0 sampai 23
# ─────────────────────────────────────────────────────────
DIURNAL_BASELINE = [
    150,  120,  100,  90,   # 00–03 dini hari
    100,  130,  200,  350,  # 04–07 mulai naik
    600,  1000, 1600, 2200, # 08–11 rampup pagi
    3000, 3400, 2800, 3200, # 12–15 peak siang
    3800, 4000, 3500, 2800, # 16–19 peak sore
    2000, 1400, 800,  400,  # 20–23 turun malam
]

def get_baseline_for_hour(hour):
    """
    Interpolasi smooth antar jam (mendukung hour float).
    """
    hour = hour % 24  # wrap-around kalau negatif / >24

    h_int = int(math.floor(hour))        # jam bawah
    h_next = (h_int + 1) % 24            # jam berikutnya
    frac = hour - h_int                  # fraksi antar jam

    h0 = DIURNAL_BASELINE[h_int]
    h1 = DIURNAL_BASELINE[h_next]

    return h0 + (h1 - h0) * frac

def natural_noise(base, day_factor=1.0):
    """
    Gaussian noise — lebih natural dari uniform.
    Std dev 18% dari base, dikali day_factor (hari sibuk vs sepi)
    """
    std = base * 0.18 * day_factor
    val = int(random.gauss(base, std))
    return max(80, min(val, 5000))   # clamp 80–5000

def occasional_event():
    """
    Kejadian tak terduga dengan probabilitas kecil.
    Return (rate_multiplier, duration) atau None
    """
    roll = random.random()
    if roll < 0.05:       # 5% — flash crowd / viral content
        print("[TCP] 🚨 EVENT: Flash crowd!")
        return (random.uniform(1.8, 2.5), random.randint(20, 45))
    elif roll < 0.09:     # 4% — batch job / backup tiba-tiba
        print("[TCP] 🔧 EVENT: Batch job burst!")
        return (random.uniform(1.3, 1.7), random.randint(15, 30))
    elif roll < 0.12:     # 3% — traffic drop (partial outage)
        print("[TCP] 📉 EVENT: Partial drop!")
        return (random.uniform(0.2, 0.4), random.randint(10, 25))
    return None           # 88% — normal, tidak ada event

def build_period(hour, n_bursts, day_factor=1.0):
    """
    Buat satu period traffic berdasarkan jam.
    Shape masih ada tapi tiap burst punya noise natural.
    """
    base = get_baseline_for_hour(hour)
    bursts = []

    for i in range(n_bursts):
        rate = natural_noise(base, day_factor)

        # Slight trend dalam 1 period — kadang naik, kadang turun, kadang flat
        trend = random.choice(['up', 'down', 'flat', 'flat'])
        if trend == 'up':
            base = min(base * random.uniform(1.05, 1.15), 5000)
        elif trend == 'down':
            base = max(base * random.uniform(0.85, 0.95), 80)

        # Duration juga stochastic
        dur = int(random.gauss(30, 8))
        dur = max(10, min(dur, 60))

        # Check occasional event
        event = occasional_event()
        if event:
            mult, event_dur = event
            rate = int(min(rate * mult, 5000))
            dur  = event_dur

        bursts.append((rate, dur))

    return bursts

def simulate_day_type():
    """
    Pilih tipe hari — mempengaruhi overall traffic level.
    Tidak selalu sama tiap hari.
    """
    roll = random.random()
    if roll < 0.10:
        print("[TCP] 📅 Day type: HOLIDAY (traffic rendah)")
        return 0.4, "HOLIDAY"
    elif roll < 0.25:
        print("[TCP] 📅 Day type: WEEKEND")
        return 0.65, "WEEKEND"
    elif roll < 0.40:
        print("[TCP] 📅 Day type: LIGHT WEEKDAY")
        return 0.85, "LIGHT_WEEKDAY"
    elif roll < 0.85:
        print("[TCP] 📅 Day type: NORMAL WEEKDAY")
        return 1.0, "NORMAL_WEEKDAY"
    else:
        print("[TCP] 📅 Day type: BUSY DAY (event/promo)")
        return 1.35, "BUSY_DAY"

# Period schedule — jam berapa, berapa burst, nama label
# hour bisa float (misal 13.5 = 13:30)
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

if __name__ == "__main__":
    print("[TCP] 🌐 Structured Stochastic Traffic Generator Started")
    time.sleep(600)

    day = 0
    while True:
        day += 1
        day_factor, day_label = simulate_day_type()

        print(f"\n{'='*60}")
        print(f"[TCP] 📅 Day #{day} | {day_label} | factor={day_factor:.2f}")
        print(f"{'='*60}")

        # Shuffle sedikit urutan period — kadang lunch dip lebih awal/telat
        schedule = list(PERIOD_SCHEDULE)
        # Geser waktu tiap period ±30 menit (0.5 jam) secara acak
        schedule = [(h + random.uniform(-0.5, 0.5), n, name)
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

                if rate >= 3500:
                    print(f"[TCP] 🔥 PEAK   rate={rate}  dur={dur}s")
                elif rate >= 1500:
                    print(f"[TCP] ⚡ MID    rate={rate}  dur={dur}s")
                else:
                    print(f"[TCP] 🌊 LOW    rate={rate}  dur={dur}s")

                send_tcp(rate, dur)

                # Gap antar burst: gaussian, bukan uniform
                gap = max(1, random.gauss(4, 2))
                time.sleep(gap)

            # Jeda antar period: lebih lama di malam hari
            if hour >= 22 or hour <= 4:
                rest = random.gauss(180, 30)
            elif hour >= 16:
                rest = random.gauss(60, 15)
            else:
                rest = random.gauss(45, 10)

            rest = max(10, int(rest))
            print(f"[TCP] 💤 Rest {rest}s after {period_name}")
            time.sleep(rest)

        print(f"\n[TCP] 🔁 Day #{day} done\n")