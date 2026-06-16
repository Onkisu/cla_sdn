#!/usr/bin/env python3
"""
Bursty Traffic Generator - Sidang Demo Version (~10 min)
Karakteristik sama dengan bursty.py tapi compressed
H3 -> H2 using iperf3 UDP
"""

import time
import subprocess
import json

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
            print(f"[BURST] Controller in {current_state}")
            return False

        if state.get('congestion') is True and current_state == 'ACTIVE_REROUTE':
            print("[BURST] Reroute active, waiting 3s...")
            time.sleep(3)

        return True
    except Exception:
        return True


def send_udp(rate, duration):
    try:
        h3_pid = subprocess.check_output(
            ["pgrep", "-n", "-f", H3]
        ).decode().strip()

        bitrate = rate * 1400 * 8

        print(f"[BURST] rate={rate} | ≈{bitrate/1_000_000:.1f} Mbps | {duration}s")

        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3", "-c", DST_IP,
            "-p", str(PORT),
            "-P", "3",
            "-b", str(bitrate),
            "-u",
            "-t", str(duration),
            "-l", "1400"
        ], timeout=duration + 5)

    except Exception as e:
        print(f"[BURST] Error: {e}")


if __name__ == "__main__":
    print("[BURST] Sidang Demo Burst Started (~10 min)")
    print("[BURST] Waiting 10s for stabilization...")
    time.sleep(10)

    # Total ≈ 590 detik + 10s stabilization = ~600s (10 menit)
    # Karakteristik: normal → ramp up → above threshold → BURST PEAK → ramp down → normal
    bursts = [
        # NORMAL (below threshold)
        (250, 15),      # ≈ 2.8 Mbps
        (500, 15),      # ≈ 5.6 Mbps
        (1200, 15),     # ≈ 13.4 Mbps

        # ABOVE THRESHOLD — reroute mulai trigger di sini
        (1800, 15),     # ≈ 20.2 Mbps  ← pas di threshold
        (2500, 15),     # ≈ 28 Mbps
        (4500, 10),     # ≈ 50 Mbps

        # PEAK
        (8750, 20),     # ≈ 98 Mbps
        (7200, 15),     # ≈ 80 Mbps

        # RAMP DOWN
        (3200, 15),     # ≈ 35 Mbps
        (1800, 10),     # ≈ 20 Mbps  ← revert zone
        (900, 10),      # ≈ 10 Mbps

        # NORMAL
        (250, 15),      # ≈ 2.8 Mbps
    ]
    # Total: 170 detik ≈ ~2.8 menit per cycle
    # 10 menit → bisa 3x cycle
    # Total duration: 30+20+20+20+15+30+30+20+20+20+20+30+35 = 310s + 10s wait = 320s
    # Kalau mau pas 10 menit, loop 1x lagi atau extend durasi phase normal

    while True:
        print("\n[BURST] === Cycle Start ===")
        total = sum(d for _, d in bursts)
        print(f"[BURST] Estimated cycle duration: {total}s ({total//60}m {total%60}s)")

        for rate, duration in bursts:
            if not check_controller_state():
                time.sleep(2)

            if rate >= 5000:
                phase = "BURST PEAK"
            elif rate >= 2000:
                phase = "ABOVE THRESHOLD"
            elif rate >= 1000:
                phase = "RAMP"
            else:
                phase = "NORMAL"

            print(f"\n[BURST] {phase} | rate={rate} | ≈{rate*1400*8/1_000_000:.1f} Mbps | {duration}s")
            send_udp(rate, duration)

        print("\n[BURST] === Cycle Complete, Restarting ===\n")
        break;  