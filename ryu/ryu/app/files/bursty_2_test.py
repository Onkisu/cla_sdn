#!/usr/bin/env python3
"""
Burst Traffic Generator - UDP FLOOD (Sidang Demo ~10 min)
==========================================================
Compressed version of bursty_2.py untuk simulasi sidang.
Karakteristik sama: background traffic + burst cycles A/B/C
dengan ramp-up, plateau bergelombang, ramp-down.

Total: ~10 menit (2 burst cycle + background)
Threshold: 20 Mbps → reroute trigger + revert visible
"""

import time
import subprocess
import json
import threading
import random

H3 = "h3"
DST_IP = "10.0.0.2"
PORT = 9001
STATE_FILE = '/tmp/controller_state.json'

# Background config (sama dengan asli)
BG_RATE_MBPS_MIN = 3
BG_RATE_MBPS_MAX = 9
BG_RAMP_STEP     = 2
BG_STOP_PRE      = 5   # stop N detik sebelum burst

# ─── CYCLE PROFILES (compressed dari asli) ───────────────────────────────────
# Asli: idle 300-480s → di sini jadi 15s
# Asli: peak 30-45s   → di sini jadi 15-20s
# Karakteristik shape tetap sama: ramp-up, plateau noise, ramp-down

# Cycle A: FastRise DualPeak SlowFall
CYCLE_A = [
    (0,   15),   # idle
    (40,  10),   # ramp up
    (90,  10),
    (140, 12),
    (170, 15),   # mendekati peak
    (185, 15),   # peak
    (175, 12),
    (190, 15),   # peak kedua (noise)
    (160, 10),
    (120, 10),   # ramp down
    (70,  8),
    (30,  8),
    (0,   15),   # idle
]  # total ≈ 155s

# Cycle B: Plateau SuddenDrop
CYCLE_B = [
    (0,   15),   # idle
    (60,  8),    # ramp up cepat
    (130, 8),
    (190, 15),   # peak
    (175, 12),   # noise turun
    (195, 15),   # balik naik
    (180, 12),
    (130, 10),   # ramp down
    (80,  8),
    (40,  8),
    (0,   15),   # idle
]  # total ≈ 126s

# Cycle C: SpikeyBurst (dari Cycle E asli)
CYCLE_C = [
    (0,   15),   # idle
    (70,  8),    # ramp up
    (160, 10),
    (120, 8),    # dip
    (190, 12),   # spike
    (140, 8),    # dip lagi
    (200, 12),   # peak tertinggi
    (130, 10),
    (80,  8),    # ramp down
    (40,  8),
    (0,   15),   # idle
]  # total ≈ 114s

ALL_CYCLES = [
    ("A - FastRise DualPeak", CYCLE_A),
    ("B - Plateau SuddenDrop", CYCLE_B),
    ("C - Spikey Burst", CYCLE_C),
]


# ─── HELPERS ─────────────────────────────────────────────────────────────────

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
            print(f"[BURST] Controller in {current_state}")
            return False
        if state.get('congestion') is True and current_state == 'ACTIVE_REROUTE':
            print("[BURST] Reroute active, waiting 3s...")
            time.sleep(3)
        return True
    except Exception:
        return True


def get_h3_pid():
    return subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()


# ─── UDP FLOOD ────────────────────────────────────────────────────────────────

def send_udp_flood(rate_mbps, duration, parallel=4):
    try:
        h3_pid = get_h3_pid()
        bw_bps = int(rate_mbps * 1_000_000)

        print(f"[UDP] {rate_mbps} Mbps x{parallel} streams {duration}s")

        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3",
            "-c", DST_IP,
            "-p", str(PORT),
            "-u",
            "-b", str(bw_bps),
            "-P", str(parallel),
            "-t", str(duration),
            "--length", "1400",
        ], timeout=duration + 10)
    except subprocess.TimeoutExpired:
        print("[UDP] Timeout (normal)")
    except Exception as e:
        print(f"[UDP] Error: {e}")


def run_bursts(burst_list, label=""):
    for rate_mbps, duration in burst_list:
        if rate_mbps == 0:
            print(f"[UDP] IDLE {duration}s")
            time.sleep(duration)
            continue

        if not check_controller_state():
            time.sleep(3)
            continue

        tag = "PEAK" if rate_mbps >= 100 else ("MID" if rate_mbps >= 40 else "LOW")
        print(f"\n[UDP] {tag} {label} | {rate_mbps} Mbps | {duration}s")

        parallel = 6 if rate_mbps >= 100 else (4 if rate_mbps >= 40 else 2)
        send_udp_flood(rate_mbps, duration, parallel=parallel)


# ─── BACKGROUND TRAFFIC (threading, sama dengan asli) ────────────────────────

def send_bg_udp(h3_pid, rate_mbps, duration):
    try:
        bw_bps = int(rate_mbps * 1_000_000)
        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3", "-c", DST_IP, "-p", str(PORT),
            "-u",
            "-b", str(bw_bps),
            "-P", "1",
            "-t", str(duration),
        ], timeout=duration + 5)
    except Exception:
        pass


def ramp_send(h3_pid, from_mbps, to_mbps, steps=3):
    for i in range(1, steps):
        r = from_mbps + (to_mbps - from_mbps) * i / steps
        send_bg_udp(h3_pid, r, BG_RAMP_STEP)


def background_worker(stop_event, stop_at):
    print(f"[BG] Background UDP dimulai ({BG_RATE_MBPS_MIN}-{BG_RATE_MBPS_MAX} Mbps)")
    prev_rate = random.uniform(BG_RATE_MBPS_MIN, BG_RATE_MBPS_MAX)

    while not stop_event.is_set():
        now = time.time()
        if now >= stop_at:
            print("[BG] Stop — pre-burst window")
            break

        remaining = stop_at - now
        seg = min(random.randint(15, 30), int(remaining))  # segment lebih pendek untuk demo
        if seg <= BG_RAMP_STEP * 2:
            break

        delta = (BG_RATE_MBPS_MAX - BG_RATE_MBPS_MIN) * 0.30
        lo = max(BG_RATE_MBPS_MIN, prev_rate - delta)
        hi = min(BG_RATE_MBPS_MAX, prev_rate + delta)
        new_rate = random.uniform(lo, hi)

        print(f"[BG] {prev_rate:.1f} -> {new_rate:.1f} Mbps seg={seg}s")

        try:
            h3_pid = get_h3_pid()
            ramp_send(h3_pid, prev_rate, new_rate)
            main_dur = seg - BG_RAMP_STEP * 2
            if main_dur > 3:
                send_bg_udp(h3_pid, new_rate, main_dur)
        except Exception as e:
            print(f"[BG] Error: {e}")
            time.sleep(2)

        prev_rate = new_rate

    print("[BG] Background selesai")


def start_background(stop_at):
    stop_event = threading.Event()
    t = threading.Thread(target=background_worker, args=(stop_event, stop_at), daemon=True)
    t.start()
    return t, stop_event


def stop_background(t, stop_event):
    stop_event.set()
    t.join(timeout=40)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("[UDP] Burst Generator — Sidang Demo (~10 min)")
    print("[UDP] Pattern: background → burst A → background → burst B → burst C")
    print("[UDP] Threshold 20 Mbps → reroute trigger visible per cycle")
    print()

    # Jadwal burst (dalam detik dari start):
    # +10s  stabilization wait
    # +10s  burst A start  (~155s)
    # +175s burst B start  (~126s)
    # +310s burst C start  (~114s)
    # +430s selesai ≈ 7.2 menit, sisanya buffer
    BURST_SCHEDULE = [
        # (10,  ALL_CYCLES[0]),   # +10s  → Cycle A
        # (175, ALL_CYCLES[1]),   # +175s → Cycle B
        # (310, ALL_CYCLES[2]),   # +310s → Cycle C
        (10, ALL_CYCLES[2]),   # +10s → Cycle C
    ]
    

    start_time = time.time()
    bg_thread, bg_stop = None, None
    global_cycle = 0

    print(f"[UDP] Start time: {time.strftime('%H:%M:%S')}")
    print(f"[UDP] Estimated finish: ~{time.strftime('%H:%M:%S', time.localtime(start_time + 450))}")
    print()

    while True:
        start_time = time.time()  # ← reset tiap loop
        bg_thread, bg_stop = None, None

        for slot_idx, (offset, (label, bursts)) in enumerate(BURST_SCHEDULE):
            burst_start = start_time + offset
            bg_stop_at  = burst_start - BG_STOP_PRE

            if bg_stop_at - time.time() > 10:
                if bg_thread and bg_thread.is_alive():
                    stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = start_background(bg_stop_at)

            wait = burst_start - time.time()
            if wait > 0:
                print(f"\n[UDP] Slot {slot_idx+1}/3 — {label} — mulai dalam {wait:.0f}s")
                time.sleep(wait)
            else:
                print(f"\n[UDP] Slot {slot_idx+1}/3 — {label} — mulai (terlambat {-wait:.0f}s)")

            if bg_thread and bg_thread.is_alive():
                stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = None, None

            total_dur = sum(d for _, d in bursts)
            print(f"[UDP] === Cycle #{global_cycle+1} | {label} | {total_dur}s total ===")
            run_bursts(bursts, label=f"[{label}]")
            global_cycle += 1

        if bg_thread and bg_thread.is_alive():
            stop_background(bg_thread, bg_stop)

        elapsed = time.time() - start_time
        print(f"\n[UDP] Loop selesai. {elapsed:.0f}s — jeda 10s")
        time.sleep(10)
        break;