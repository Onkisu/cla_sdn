#!/usr/bin/env python3
"""
Burst Traffic Generator - Dataset 2 (Scheduled + Background Traffic)
v3 fixes:
  - Background lebih smooth: rate berubah LAMBAT (segmen panjang 60-120s)
  - Transisi background naik/turun gradual (ramp), bukan loncat
  - Durasi tiap step burst dipanjangin 2x
  - Burst peak lebih lama dan dominan
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

# Jadwal burst
# BURST_OFFSETS = [5*60, 35*60]
# BURST_OFFSETS = [5*60, 25*60, 48*60]
BURST_OFFSETS = [2*60, 17*60, 35*60, 50*60]

# Background config — smooth & stabil
BG_SEGMENT_MIN = 60    # detik per segmen (lebih panjang = lebih smooth)
BG_SEGMENT_MAX = 120
BG_RATE_MIN    = 1340  # ~15 Mbps
BG_RATE_MAX    = 3571  # ~40 Mbps
BG_RAMP_STEP   = 3     # detik per ramp step (transisi antar rate)
BG_STOP_PRE    = 15    # berhenti N detik sebelum burst

# ─────────────────────────────────────────────────────────────────────────────
#  SIKLUS BURST — durasi dipanjangin 2x biar lebih dominan di grafik
# ─────────────────────────────────────────────────────────────────────────────

CYCLE_A = [
    (800,  20),
    (3500, 25),
    (7200, 50),   # peak 1
    (6800, 45),
    (8500, 60),   # peak 2 (lebih tinggi, lebih lama)
    (6000, 45),
    (4200, 50),
    (2800, 55),
    (1500, 60),
    (700,  70),
    (300,  80),   # ekor panjang
]

CYCLE_B = [
    (1200, 30),
    (4500, 30),
    (7800, 75),   # naik ke plateau
    (8200, 70),
    (8000, 65),   # plateau panjang
    (7500, 55),
    (900,  20),   # drop tiba-tiba
    (2200, 40),
    (1800, 45),
    (600,  50),
]

CYCLE_C = [
    (500,  30),
    (3800, 45),   # peak 1
    (2000, 30),
    (6500, 55),   # peak 2 tinggi
    (3500, 35),
    (5200, 45),   # peak 3
    (2500, 45),
    (1200, 55),
    (400,  65),
]

CYCLE_D = [
    (300,  55),
    (600,  50),
    (1100, 45),
    (2000, 40),
    (3500, 35),
    (5500, 30),
    (7800, 45),   # peak
    (8800, 35),
    (3000, 20),
    (800,  20),
    (250,  25),
]

CYCLE_E = [
    (400,  40),
    (8000, 30),   # spike 1
    (500,  30),
    (7500, 30),   # spike 2
    (600,  30),
    (6800, 35),   # spike 3
    (1500, 40),
    (500,  45),
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
            print(f"[BURST] Controller in {current_state}")
            return False
        if state.get('congestion') == True and current_state == 'ACTIVE_REROUTE':
            print("[BURST] Reroute active, waiting 3s...")
            time.sleep(3)
        return True
    except:
        return True

def get_h3_pid():
    return subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()

def send_tcp_once(rate, duration):
    try:
        h3_pid = get_h3_pid()
        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3", "-c", DST_IP, "-p", str(PORT),
            "-b", str(rate * 1400 * 8), "-t", str(duration)
        ], timeout=duration + 5)
    except Exception as e:
        print(f"[TCP] Error: {e}")

def run_bursts(burst_list, label=""):
    for rate, duration in burst_list:
        if not check_controller_state():
            time.sleep(5)
            continue
        mbps = rate * 1400 * 8 / 1e6
        tag = "PEAK" if rate >= 6000 else ("MID" if rate >= 2000 else "LOW")
        print(f"[TCP] {tag:4s} {label} {mbps:.1f} Mbps  {duration}s")
        send_tcp_once(rate, duration)

# ─────────────────────────────────────────────────────────────────────────────
#  BACKGROUND — smooth ramp antar segmen, bukan loncat tiba-tiba
# ─────────────────────────────────────────────────────────────────────────────

def ramp_send(h3_pid, from_rate, to_rate, steps=5):
    """Kirim beberapa segmen pendek untuk transisi halus antar rate."""
    if from_rate == to_rate or steps <= 1:
        return
    for i in range(1, steps):
        if abs(to_rate - from_rate) < 100:
            break
        r = int(from_rate + (to_rate - from_rate) * i / steps)
        try:
            subprocess.run([
                "mnexec", "-a", h3_pid,
                "iperf3", "-c", DST_IP, "-p", str(PORT),
                "-b", str(r * 1400 * 8), "-t", str(BG_RAMP_STEP)
            ], timeout=BG_RAMP_STEP + 3)
        except:
            pass

def background_worker(stop_event, stop_at):
    print(f"[BG] Background traffic dimulai (smooth 15-40 Mbps)")
    prev_rate = random.randint(BG_RATE_MIN, BG_RATE_MAX)

    while not stop_event.is_set():
        now = time.time()
        if now >= stop_at:
            print("[BG] Berhenti — pre-burst window")
            break

        remaining = stop_at - now
        seg = min(random.randint(BG_SEGMENT_MIN, BG_SEGMENT_MAX), int(remaining))
        if seg <= BG_RAMP_STEP * 2:
            break

        # Rate baru tidak terlalu jauh dari sebelumnya (max ±30%)
        delta = int((BG_RATE_MAX - BG_RATE_MIN) * 0.30)
        lo = max(BG_RATE_MIN, prev_rate - delta)
        hi = min(BG_RATE_MAX, prev_rate + delta)
        new_rate = random.randint(lo, hi)

        mbps_prev = prev_rate * 1400 * 8 / 1e6
        mbps_new  = new_rate  * 1400 * 8 / 1e6
        print(f"[BG] {mbps_prev:.1f} -> {mbps_new:.1f} Mbps  seg={seg}s")

        try:
            h3_pid = get_h3_pid()
            # Ramp transisi
            ramp_send(h3_pid, prev_rate, new_rate)
            # Segmen utama (sisa setelah ramp)
            main_dur = seg - BG_RAMP_STEP * 4
            if main_dur > 5:
                subprocess.run([
                    "mnexec", "-a", h3_pid,
                    "iperf3", "-c", DST_IP, "-p", str(PORT),
                    "-b", str(new_rate * 1400 * 8), "-t", str(main_dur)
                ], timeout=main_dur + 5)
        except Exception as e:
            print(f"[BG] Error: {e}")
            time.sleep(3)

        prev_rate = new_rate

    print("[BG] Background traffic selesai")

def start_background(stop_at):
    stop_event = threading.Event()
    t = threading.Thread(target=background_worker, args=(stop_event, stop_at), daemon=True)
    t.start()
    return t, stop_event

def stop_background(t, stop_event):
    stop_event.set()
    t.join(timeout=BG_SEGMENT_MAX + 15)

# ─────────────────────────────────────────────────────────────────────────────
#  SCHEDULER UTAMA
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("[TCP] Burst Generator Dataset 2 — SCHEDULED + SMOOTH BACKGROUND")
    print(f"[TCP] Jadwal: {[f'{o//60}m{o%60:02d}s' for o in BURST_OFFSETS]}")
    print("[TCP] Background: smooth 15-40 Mbps, segmen 60-120s")
    # Tidak ada warm-up — background langsung jalan

    global_cycle = 0
    bg_thread, bg_stop = None, None

    while True:
        now = time.time()
        hour_start = now - (now % 3600)

        # Hitung slot burst berikutnya yang belum lewat
        next_burst_time = None
        for offset in BURST_OFFSETS:
            t = hour_start + offset
            if t > now + BG_STOP_PRE:
                next_burst_time = t
                break
        # Kalau semua slot jam ini sudah lewat, pakai jam berikutnya
        if next_burst_time is None:
            next_burst_time = hour_start + 3600 + BURST_OFFSETS[0]

        # Mulai background SEKARANG sampai BG_STOP_PRE sebelum burst pertama
        bg_stop_at = next_burst_time - BG_STOP_PRE
        if bg_stop_at - time.time() > BG_SEGMENT_MIN + 5:
            print(f"[TCP] Background langsung dimulai, berhenti {BG_STOP_PRE}s sebelum burst pertama")
            bg_thread, bg_stop = start_background(bg_stop_at)

        print(f"\n{'='*55}")
        print(f"[TCP] Jam baru — {len(BURST_OFFSETS)} burst dijadwalkan")
        print(f"{'='*55}")

        for slot_idx, offset in enumerate(BURST_OFFSETS):
            target_time = hour_start + offset
            label, bursts = ALL_CYCLES[global_cycle % len(ALL_CYCLES)]
            burst_dur = burst_duration(bursts)

            # Pangkas burst kalau tidak muat
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
                    print(f"[TCP] Burst dipangkas agar muat dalam gap {gap}s")

            # Mulai background sampai BG_STOP_PRE detik sebelum burst
            bg_stop_at = target_time - BG_STOP_PRE
            if bg_stop_at - time.time() > BG_SEGMENT_MIN + 10:
                if bg_thread and bg_thread.is_alive():
                    stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = start_background(bg_stop_at)

            # Tunggu sampai waktu burst
            wait = target_time - time.time()
            if wait > 0:
                print(f"\n[TCP] Slot {slot_idx+1}/{len(BURST_OFFSETS)} — "
                      f"burst +{offset//60}m{offset%60:02d}s (tunggu {wait:.0f}s)")
                time.sleep(wait)
            else:
                print(f"\n[TCP] Slot {slot_idx+1} terlambat {-wait:.0f}s")

            # Stop background sebelum burst
            if bg_thread and bg_thread.is_alive():
                stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = None, None

            # Jalankan burst
            print(f"[TCP] === Cycle #{global_cycle+1} | {label} | {burst_dur}s total ===")
            run_bursts(bursts, label=f"[{label}]")
            global_cycle += 1

        # Background sampai akhir jam
        next_hour = hour_start + 3600
        bg_stop_at = next_hour - BG_STOP_PRE
        if bg_stop_at - time.time() > BG_SEGMENT_MIN + 10:
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