#!/usr/bin/env python3
"""
Burst Traffic Generator - Dataset 2
Karakteristik berbeda dari Dataset 1:
  - Pola asimetris: naik cepat, turun lambat
  - Multi-peak dalam 1 siklus
  - Idle bervariasi (bukan fixed)
  - Siklus lebih pendek tapi lebih sering
"""
import time
import subprocess
import json
import random

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
#  SIKLUS — setiap siklus punya "bentuk" berbeda
#  Dataset 1: 1 gunung simetris, idle 500 detik
#  Dataset 2: multi-peak asimetris, idle variatif 60–200 detik
# ─────────────────────────────────────────────────────────────────────────────

# Siklus A: Naik CEPAT → 2 peak → turun LAMBAT bertahap
CYCLE_A = [
    (800,  10),   # naik cepat
    (3500, 10),
    (7200, 25),   # peak 1
    (6800, 20),
    (8500, 30),   # peak 2 lebih tinggi
    (6000, 20),   # turun lambat
    (4200, 25),
    (2800, 30),
    (1500, 35),
    (700,  40),
    (300,  50),   # ekor panjang
]

# Siklus B: Plateau tinggi → drop tiba-tiba → recovery kecil
CYCLE_B = [
    (1200, 15),
    (4500, 15),
    (7800, 40),   # naik ke plateau
    (8200, 35),
    (8000, 30),   # plateau
    (7500, 25),
    (900,  10),   # drop tiba-tiba
    (2200, 20),   # recovery kecil
    (1800, 25),
    (600,  30),
]

# Siklus C: 3 peak berbeda tinggi, tidak simetris
CYCLE_C = [
    (500,  15),
    (3800, 20),   # peak 1 sedang
    (2000, 15),   # valley
    (6500, 25),   # peak 2 tinggi
    (3500, 15),   # valley
    (5200, 20),   # peak 3 medium
    (2500, 20),
    (1200, 30),
    (400,  40),
]

# Siklus D: Naik LAMBAT → peak singkat → turun CEPAT
CYCLE_D = [
    (300,  30),
    (600,  25),
    (1100, 20),
    (2000, 20),
    (3500, 15),
    (5500, 15),
    (7800, 20),   # peak singkat
    (8800, 15),
    (3000, 10),   # turun cepat
    (800,  10),
    (250,  15),
]

# Siklus E: Double spike pendek-pendek (bursty agresif)
CYCLE_E = [
    (400,  20),
    (8000, 12),   # spike 1 cepat
    (500,  15),   # drop
    (7500, 12),   # spike 2
    (600,  15),   # drop
    (6800, 15),   # spike 3 lebih lama
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

if __name__ == "__main__":
    print("[TCP] 🎯 Burst Generator Dataset 2 Started")
    print("[TCP] Pola: multi-peak asimetris, idle variatif")
    time.sleep(600)

    cycle_num = 0
    while True:
        # Pilih siklus — tidak selalu urut, bisa acak
        # Tapi tidak mengulang siklus yang sama 2x berturut-turut
        prev = cycle_num % len(ALL_CYCLES)
        choices = [i for i in range(len(ALL_CYCLES)) if i != prev]
        idx = random.choice(choices)
        cycle_num += 1

        label, bursts = ALL_CYCLES[idx]
        print(f"\n{'='*55}")
        print(f"[TCP] 🔁 Cycle #{cycle_num} | {label}")
        print(f"{'='*55}")

        run_bursts(bursts, label=f"[{label}]")

        # Idle variatif — bukan fixed 500 detik
        # Kadang pendek (recovery cepat), kadang panjang (network idle)
        roll = random.random()
        if roll < 0.30:
            idle = random.randint(60, 100)    # 30% — idle pendek
            print(f"[TCP] 💤 Short idle {idle}s")
        elif roll < 0.75:
            idle = random.randint(120, 180)   # 45% — idle medium
            print(f"[TCP] 💤 Medium idle {idle}s")
        else:
            idle = random.randint(200, 300)   # 25% — idle panjang
            print(f"[TCP] 💤 Long idle {idle}s")

        time.sleep(idle)