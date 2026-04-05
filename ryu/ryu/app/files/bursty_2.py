#!/usr/bin/env python3
"""
Burst Traffic Generator - Dataset 2 (Scheduled + Background Traffic)
Perubahan dari versi scheduled:
  - Saat idle antar burst: ada background traffic 15-40 Mbps (bukan 0)
  - Background traffic jalan di thread terpisah (non-blocking)
  - Burst tetap terjadwal tepat waktu
  - Background berhenti ~12 detik sebelum burst dimulai (clean transition)
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

# Jadwal burst - offset dalam detik dari awal jam
# BURST_OFFSETS = [5*60, 35*60]            # 2x per jam
# BURST_OFFSETS = [5*60, 25*60, 48*60]     # 3x per jam
BURST_OFFSETS = [2*60, 17*60, 35*60, 50*60]  # 4x per jam

# Background traffic config (15-40 Mbps)
# 15 Mbps = ~1340 pkt/s,  40 Mbps = ~3571 pkt/s
BG_RATE_MIN = 1340
BG_RATE_MAX = 3571
BG_SEGMENT  = 20   # detik per segmen background
BG_STOP_PRE = 12   # berhenti N detik sebelum burst

# ─────────────────────────────────────────────────────────────────────────────
#  SIKLUS BURST
# ─────────────────────────────────────────────────────────────────────────────

CYCLE_A = [
    (800,10),(3500,10),(7200,25),(6800,20),(8500,30),
    (6000,20),(4200,25),(2800,30),(1500,35),(700,40),(300,50),
]
CYCLE_B = [
    (1200,15),(4500,15),(7800,40),(8200,35),(8000,30),
    (7500,25),(900,10),(2200,20),(1800,25),(600,30),
]
CYCLE_C = [
    (500,15),(3800,20),(2000,15),(6500,25),(3500,15),
    (5200,20),(2500,20),(1200,30),(400,40),
]
CYCLE_D = [
    (300,30),(600,25),(1100,20),(2000,20),(3500,15),
    (5500,15),(7800,20),(8800,15),(3000,10),(800,10),(250,15),
]
CYCLE_E = [
    (400,20),(8000,12),(500,15),(7500,12),
    (600,15),(6800,15),(1500,20),(500,25),
]

ALL_CYCLES = [
    ("A - FastRise DualPeak SlowFall", CYCLE_A),
    ("B - Plateau SuddenDrop",         CYCLE_B),
    ("C - TriplePeak Asymmetric",      CYCLE_C),
    ("D - SlowRise FastFall",          CYCLE_D),
    ("E - AggressiveSpike",            CYCLE_E),
]

def burst_duration(cycle):
    return sum(d for _, d in cycle)

# ─────────────────────────────────────────────────────────────────────────────
#  HELPER
# ─────────────────────────────────────────────────────────────────────────────

def check_controller_state():
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        current_state = state.get('state', 'IDLE')
        transition_states = [
            'DELETING_ALL_FLOWS','WAITING_SETTLE','INSTALLING_NEW_PATH',
            'REVERT_DELETING','REVERT_SETTLE','REVERT_INSTALLING'
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

def get_h3_pid():
    return subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()

def send_tcp_once(rate, duration):
    try:
        h3_pid = get_h3_pid()
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
            print(f"[TCP] PEAK   {label} rate={rate}  ({mbps:.1f} Mbps)  dur={duration}s")
        elif rate >= 2000:
            print(f"[TCP] MID    {label} rate={rate}  ({mbps:.1f} Mbps)  dur={duration}s")
        else:
            print(f"[TCP] LOW    {label} rate={rate}  ({mbps:.1f} Mbps)  dur={duration}s")
        send_tcp_once(rate, duration)

# ─────────────────────────────────────────────────────────────────────────────
#  BACKGROUND TRAFFIC THREAD
# ─────────────────────────────────────────────────────────────────────────────

def background_worker(stop_event, stop_at):
    print(f"[BG] Background traffic dimulai (15-40 Mbps)")
    while not stop_event.is_set():
        now = time.time()
        if now >= stop_at:
            print("[BG] Berhenti — masuk pre-burst window")
            break
        remaining = stop_at - now
        seg = min(BG_SEGMENT, int(remaining))
        if seg <= 2:
            break
        rate = random.randint(BG_RATE_MIN, BG_RATE_MAX)
        mbps = rate * 1400 * 8 / 1e6
        print(f"[BG] rate={rate} ({mbps:.1f} Mbps) dur={seg}s")
        try:
            h3_pid = get_h3_pid()
            subprocess.run([
                "mnexec", "-a", h3_pid,
                "iperf3", "-c", DST_IP, "-p", str(PORT),
                "-b", str(rate * 1400 * 8), "-t", str(seg)
            ], timeout=seg + 5)
        except Exception as e:
            print(f"[BG] Error: {e}")
            time.sleep(2)
    print("[BG] Background traffic selesai")

def start_background(stop_at):
    stop_event = threading.Event()
    t = threading.Thread(target=background_worker, args=(stop_event, stop_at), daemon=True)
    t.start()
    return t, stop_event

def stop_background(t, stop_event):
    stop_event.set()
    t.join(timeout=BG_SEGMENT + 10)

# ─────────────────────────────────────────────────────────────────────────────
#  SCHEDULER UTAMA
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("[TCP] Burst Generator Dataset 2 — SCHEDULED + BACKGROUND MODE")
    print(f"[TCP] Jadwal: {[f'{o//60}m{o%60:02d}s' for o in BURST_OFFSETS]}")
    print(f"[TCP] Background idle: ~15–40 Mbps")
    time.sleep(600)

    global_cycle = 0
    bg_thread, bg_stop = None, None

    while True:
        now = time.time()
        hour_start = now - (now % 3600)

        print(f"\n{'='*55}")
        print(f"[TCP] Jam baru — {len(BURST_OFFSETS)} burst dijadwalkan")
        print(f"{'='*55}")

        for slot_idx, offset in enumerate(BURST_OFFSETS):
            target_time = hour_start + offset
            label, bursts = ALL_CYCLES[global_cycle % len(ALL_CYCLES)]
            burst_dur = burst_duration(bursts)

            # Pangkas burst kalau terlalu panjang
            if slot_idx + 1 < len(BURST_OFFSETS):
                gap = BURST_OFFSETS[slot_idx + 1] - offset
                if burst_dur > gap * 0.85:
                    trimmed, total = [], 0
                    for r, d in bursts:
                        if total + d > gap * 0.80:
                            break
                        trimmed.append((r, d))
                        total += d
                    bursts = trimmed

            # Mulai background traffic, berhenti BG_STOP_PRE detik sebelum burst
            bg_stop_at = target_time - BG_STOP_PRE
            if bg_stop_at - time.time() > BG_SEGMENT + 5:
                if bg_thread and bg_thread.is_alive():
                    stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = start_background(bg_stop_at)

            # Tunggu sampai waktu burst
            wait = target_time - time.time()
            if wait > 0:
                print(f"\n[TCP] Slot {slot_idx+1}/{len(BURST_OFFSETS)} — "
                      f"burst jam+{offset//60}m{offset%60:02d}s (tunggu {wait:.0f}s)")
                time.sleep(wait)
            else:
                print(f"\n[TCP] Slot {slot_idx+1} terlambat {-wait:.0f}s, langsung jalan")

            # Pastikan background sudah stop
            if bg_thread and bg_thread.is_alive():
                stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = None, None

            # Jalankan burst
            print(f"[TCP] Cycle #{global_cycle+1} | {label}")
            run_bursts(bursts, label=f"[{label}]")
            global_cycle += 1

        # Background sampai akhir jam
        next_hour = hour_start + 3600
        bg_stop_at = next_hour - BG_STOP_PRE
        if bg_stop_at - time.time() > BG_SEGMENT + 5:
            print(f"\n[TCP] Background traffic sampai akhir jam")
            if bg_thread and bg_thread.is_alive():
                stop_background(bg_thread, bg_stop)
            bg_thread, bg_stop = start_background(bg_stop_at)

        wait_next = next_hour - time.time()
        if wait_next > 0:
            time.sleep(wait_next)

        if bg_thread and bg_thread.is_alive():
            stop_background(bg_thread, bg_stop)
            bg_thread, bg_stop = None, None