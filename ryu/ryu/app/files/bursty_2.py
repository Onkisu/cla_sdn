#!/usr/bin/env python3
"""
Burst Traffic Generator - UDP FLOOD (No Congestion Control)
============================================================
KENAPA UDP, BUKAN TCP:
- TCP punya congestion control (CUBIC/BBR) → dia BACK OFF sendiri saat congestion
  → hasilnya throttled, bukan real packet loss
- UDP TIDAK peduli congestion → terus kirim paksa → buffer penuh → REAL PACKET DROP
- Ini yang bikin VoIP (juga UDP) benar-benar experience loss & delay tinggi

CARA KERJA:
- Burst pakai iperf3 UDP (-u) dengan bandwidth target jauh di atas link capacity
- Multi-stream (-P) untuk saturate lebih agresif
- Background juga UDP (lebih stabil, tidak interfere dengan VoIP testing)

PENTING:
- Di topology, pastikan link spine-leaf TIDAK ada bw= limit DAN max_queue_size kecil
  (lihat komentar di bawah tentang patch topology)
- Atau jalankan patch_queue.sh sebelum simulasi untuk shrink queue OVS
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

# ─── PATCH HELPER ────────────────────────────────────────────────────────────
# Sebelum burst: shrink TX queue interface agar packet drop lebih cepat terjadi
# (kalau queue besar, packet cuma di-buffer → delay, bukan drop)
SPINE_LEAF_IFACES = [
    # Format: (host_namespace, interface_name)
    # Sesuaikan dengan interface yang dipakai di topologi lo
    # Kosongkan list ini kalau tidak mau patch otomatis
]

# ─── JADWAL BURST ────────────────────────────────────────────────────────────
BURST_OFFSETS = [2*60, 17*60, 35*60, 50*60]

# ─── BACKGROUND CONFIG ───────────────────────────────────────────────────────
BG_SEGMENT_MIN = 60
BG_SEGMENT_MAX = 120
BG_RATE_MBPS_MIN = 3     # Mbps
BG_RATE_MBPS_MAX = 9     # Mbps
BG_RAMP_STEP    = 3      # detik per ramp step
BG_STOP_PRE     = 15     # berhenti N detik sebelum burst

# ─── SIKLUS BURST (dalam Mbps) ───────────────────────────────────────────────
# Format: (rate_mbps, durasi_detik)
# rate=0 → IDLE murni (time.sleep), tidak ada iperf3 sama sekali
#
# FILOSOFI:
# - Burst harus PENDEK (10-20 detik) dan TINGGI (>>50 Mbps)
# - Idle harus PANJANG (60-120 detik) dan benar-benar 0
# - Grafik harusnya: _____/\___________/\___________/\______
#                         burst  idle       burst  idle

# Profil A: Ramp up pelan, plateau dengan noise, drop cepat
CYCLE_A = [
    (0,   300),  # idle 5 menit
    (40,  20),   # ramp up mulai
    (90,  20),
    (140, 25),
    (170, 30),   # mendekati peak
    (185, 40),   # peak dengan sedikit variasi
    (175, 25),
    (190, 35),   # naik lagi (noise)
    (165, 20),
    (180, 30),
    (160, 25),   # mulai turun
    (120, 20),   # ramp down
    (70,  15),
    (30,  15),
    (0,   360),  # idle 6 menit
]

# Profil B: Langsung agresif, plateau panjang bergelombang, turun bertahap
CYCLE_B = [
    (0,   420),  # idle 7 menit
    (60,  15),   # ramp up cepat
    (130, 15),
    (190, 35),   # peak
    (175, 30),   # noise turun
    (195, 40),   # balik naik
    (180, 35),
    (170, 30),   # mulai ramp down
    (130, 20),
    (80,  20),
    (40,  15),
    (0,   300),  # idle 5 menit
]

# Profil C: Dua burst berbeda karakter, idle panjang di antara
CYCLE_C = [
    (0,   300),  # idle 5 menit
    (50,  20),   # burst 1 ramp up
    (110, 20),
    (160, 30),
    (175, 35),   # burst 1 peak
    (155, 25),
    (170, 30),
    (100, 20),   # ramp down burst 1
    (50,  15),
    (0,   360),  # idle 6 menit
    (80,  15),   # burst 2 ramp up lebih cepat
    (150, 20),
    (195, 45),   # burst 2 peak lebih tinggi & lama
    (185, 35),
    (200, 40),   # noise naik
    (170, 25),
    (120, 20),   # ramp down
    (60,  15),
    (0,   300),  # idle 5 menit
]

# Profil D: Slow build sangat pelan, peak lama, turun cepat
CYCLE_D = [
    (0,   480),  # idle 8 menit
    (20,  25),   # ramp up sangat pelan
    (40,  25),
    (70,  25),
    (100, 25),
    (130, 25),
    (155, 30),
    (175, 35),   # peak
    (185, 40),
    (180, 35),   # plateau bergelombang
    (190, 30),
    (175, 25),
    (80,  15),   # drop cepat
    (30,  10),
    (0,   300),  # idle 5 menit
]

# Profil E: Spiky — naik turun tidak beraturan selama burst
CYCLE_E = [
    (0,   360),  # idle 6 menit
    (70,  15),   # ramp up
    (160, 20),
    (120, 15),   # dip
    (190, 25),   # spike
    (140, 20),   # dip lagi
    (200, 30),   # peak tertinggi
    (160, 25),
    (185, 20),   # naik lagi
    (130, 20),
    (80,  15),   # ramp down
    (40,  15),
    (0,   420),  # idle 7 menit
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

# ─── STATE CHECK ──────────────────────────────────────────────────────────────

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
        if state.get('congestion') == True and current_state == 'ACTIVE_REROUTE':
            print("[BURST] Reroute active, waiting 3s...")
            time.sleep(3)
        return True
    except Exception:
        return True

def get_h3_pid():
    return subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()

# ─── UDP FLOOD (Core Function) ────────────────────────────────────────────────

def send_udp_flood(rate_mbps, duration, parallel=4):
    """
    Kirim UDP flood dengan iperf3 -u.
    
    KENAPA INI BEKERJA (tidak throttle):
    - iperf3 -u: UDP mode, TIDAK ada congestion control
    - iperf3 akan kirim sebesar -b yang diminta, tidak peduli loss
    - Kalau link tidak kuat → router drop → VoIP juga kena drop
    - Parallel streams (-P) untuk saturasi lebih agresif
    
    PENTING: rate_mbps SENGAJA jauh di atas kapasitas link spine-leaf
    supaya queue overflow → real packet loss.
    """
    try:
        h3_pid = get_h3_pid()
        bw_bps = int(rate_mbps * 1_000_000)
        
        print(f"[UDP] Sending {rate_mbps} Mbps UDP flood x{parallel} streams for {duration}s")
        
        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3",
            "-c", DST_IP,
            "-p", str(PORT),
            "-u",                          # ← UDP mode! tidak ada congestion control
            "-b", str(bw_bps),             # target bandwidth (akan dicoba terus walau loss)
            "-P", str(parallel),           # parallel streams → lebih agresif
            "-t", str(duration),
            "--length", "1400",            # ukuran paket besar → lebih efisien saturasi
        ], timeout=duration + 10)
    except subprocess.TimeoutExpired:
        print(f"[UDP] Timeout (normal jika burst selesai)")
    except Exception as e:
        print(f"[UDP] Error: {e}")

def run_bursts(burst_list, label=""):
    for rate_mbps, duration in burst_list:
        # rate=0 → IDLE MURNI, tidak ada iperf3, tidak ada traffic dari H3
        if rate_mbps == 0:
            print(f"[UDP] IDLE  {duration}s — no burst, background takes over")
            time.sleep(duration)
            continue

        if not check_controller_state():
            time.sleep(5)
            continue

        tag = "PEAK" if rate_mbps >= 100 else ("MID" if rate_mbps >= 40 else "LOW")
        print(f"[UDP] {tag:4s} {label} {rate_mbps} Mbps  {duration}s")

        # Parallel streams: lebih tinggi rate → lebih banyak stream untuk saturasi
        parallel = 6 if rate_mbps >= 100 else (4 if rate_mbps >= 40 else 2)
        send_udp_flood(rate_mbps, duration, parallel=parallel)

# ─── BACKGROUND (juga UDP, lebih ringan) ─────────────────────────────────────

def send_bg_udp(h3_pid, rate_mbps, duration):
    """Background traffic: UDP tapi rate rendah (tidak dimaksudkan untuk congestion)."""
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

def ramp_send(h3_pid, from_mbps, to_mbps, steps=5):
    if from_mbps == to_mbps or steps <= 1:
        return
    for i in range(1, steps):
        if abs(to_mbps - from_mbps) < 1:
            break
        r = from_mbps + (to_mbps - from_mbps) * i / steps
        send_bg_udp(h3_pid, r, BG_RAMP_STEP)

def background_worker(stop_event, stop_at):
    print(f"[BG] UDP background traffic dimulai ({BG_RATE_MBPS_MIN}-{BG_RATE_MBPS_MAX} Mbps)")
    prev_rate = random.uniform(BG_RATE_MBPS_MIN, BG_RATE_MBPS_MAX)

    while not stop_event.is_set():
        now = time.time()
        if now >= stop_at:
            print("[BG] Berhenti — pre-burst window")
            break

        remaining = stop_at - now
        seg = min(random.randint(BG_SEGMENT_MIN, BG_SEGMENT_MAX), int(remaining))
        if seg <= BG_RAMP_STEP * 2:
            break

        delta = (BG_RATE_MBPS_MAX - BG_RATE_MBPS_MIN) * 0.30
        lo = max(BG_RATE_MBPS_MIN, prev_rate - delta)
        hi = min(BG_RATE_MBPS_MAX, prev_rate + delta)
        new_rate = random.uniform(lo, hi)

        print(f"[BG] {prev_rate:.1f} -> {new_rate:.1f} Mbps  seg={seg}s")

        try:
            h3_pid = get_h3_pid()
            ramp_send(h3_pid, prev_rate, new_rate)
            main_dur = seg - BG_RAMP_STEP * 4
            if main_dur > 5:
                send_bg_udp(h3_pid, new_rate, main_dur)
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

# ─── MAIN SCHEDULER ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("[UDP] Burst Generator — UDP FLOOD MODE (Real Congestion, No Throttle)")
    print(f"[UDP] Jadwal: {[f'{o//60}m{o%60:02d}s' for o in BURST_OFFSETS]}")
    print("[UDP] Mode: iperf3 UDP (-u), tidak ada congestion control → real packet loss")
    print()
    print("[UDP] CATATAN PENTING:")
    print("      Pastikan di spine_leaf_voip_simulation.py:")
    print("      - Link spine-leaf: max_queue_size=8 (atau lebih kecil)")
    print("      - TIDAK ada bw= limit di link spine-leaf")
    print("      Ini yang bikin buffer overflow → real drop (bukan shape/throttle)")
    print()

    global_cycle = 0
    bg_thread, bg_stop = None, None

    while True:
        now = time.time()
        hour_start = now - (now % 3600)

        next_burst_time = None
        for offset in BURST_OFFSETS:
            t = hour_start + offset
            if t > now + BG_STOP_PRE:
                next_burst_time = t
                break
        if next_burst_time is None:
            next_burst_time = hour_start + 3600 + BURST_OFFSETS[0]

        bg_stop_at = next_burst_time - BG_STOP_PRE
        if bg_stop_at - time.time() > BG_SEGMENT_MIN + 5:
            print(f"[UDP] Background langsung dimulai, berhenti {BG_STOP_PRE}s sebelum burst pertama")
            bg_thread, bg_stop = start_background(bg_stop_at)

        print(f"\n{'='*55}")
        print(f"[UDP] Jam baru — {len(BURST_OFFSETS)} burst dijadwalkan")
        print(f"{'='*55}")

        for slot_idx, offset in enumerate(BURST_OFFSETS):
            target_time = hour_start + offset
            label, bursts = ALL_CYCLES[global_cycle % len(ALL_CYCLES)]
            burst_dur = burst_duration(bursts)

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
                    print(f"[UDP] Burst dipangkas agar muat dalam gap {gap}s")

            bg_stop_at = target_time - BG_STOP_PRE
            if bg_stop_at - time.time() > BG_SEGMENT_MIN + 10:
                if bg_thread and bg_thread.is_alive():
                    stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = start_background(bg_stop_at)

            wait = target_time - time.time()
            if wait > 0:
                print(f"\n[UDP] Slot {slot_idx+1}/{len(BURST_OFFSETS)} — "
                      f"burst +{offset//60}m{offset%60:02d}s (tunggu {wait:.0f}s)")
                time.sleep(wait)
            else:
                print(f"\n[UDP] Slot {slot_idx+1} terlambat {-wait:.0f}s")

            if bg_thread and bg_thread.is_alive():
                stop_background(bg_thread, bg_stop)
                bg_thread, bg_stop = None, None

            print(f"[UDP] === Cycle #{global_cycle+1} | {label} | {burst_dur}s total ===")
            run_bursts(bursts, label=f"[{label}]")
            global_cycle += 1

        next_hour = hour_start + 3600
        bg_stop_at = next_hour - BG_STOP_PRE
        if bg_stop_at - time.time() > BG_SEGMENT_MIN + 10:
            print(f"\n[UDP] Background traffic sampai akhir jam")
            if bg_thread and bg_thread.is_alive():
                stop_background(bg_thread, bg_stop)
            bg_thread, bg_stop = start_background(bg_stop_at)

        wait_next = next_hour - time.time()
        if wait_next > 0:
            time.sleep(wait_next)

        if bg_thread and bg_thread.is_alive():
            stop_background(bg_thread, bg_stop)
            bg_thread, bg_stop = None, None