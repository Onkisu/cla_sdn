#!/usr/bin/env python3
"""
Irregular Burst Traffic Generator for H3 using iperf3
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

def jitter(value, pct=0.15):
    """Add ±pct% random noise to a value"""
    return int(value * random.uniform(1 - pct, 1 + pct))

def random_pause(lo=2, hi=12):
    """Short irregular gap between bursts"""
    gap = random.uniform(lo, hi)
    print(f"[TCP] ⏳ Gap {gap:.1f}s")
    time.sleep(gap)

def build_irregular_cycle():
    """
    Peak max ~4000-5000, lebih moderat dan realistis.
    """
    pattern = random.choice(['spike', 'sawtooth', 'staircase', 'valley', 'double_peak', 'random_walk'])
    print(f"\n[TCP] 🎲 Pattern: {pattern.upper()}")

    if pattern == 'spike':
        # Calm → spike moderat → turun
        bursts = [
            (jitter(200),  random.randint(20, 40)),
            (jitter(500),  random.randint(10, 20)),
            (jitter(4200), random.randint(8,  15)),   # spike
            (jitter(900),  random.randint(10, 20)),
            (jitter(300),  random.randint(30, 60)),
        ]

    elif pattern == 'sawtooth':
        # Naik bertahap, drop, repeat — puncak max ~4500
        bursts = [
            (jitter(400),  random.randint(15, 25)),
            (jitter(1000), random.randint(15, 25)),
            (jitter(2500), random.randint(15, 25)),
            (jitter(4500), random.randint(10, 20)),
            (jitter(600),  random.randint(5,  10)),   # drop
            (jitter(1500), random.randint(15, 25)),
            (jitter(3200), random.randint(15, 20)),
            (jitter(300),  random.randint(5,  10)),   # drop
        ]

    elif pattern == 'staircase':
        # Step naik sampai ~4800, lalu turun
        steps_up   = [800, 1800, 3000, 4000, 4800]
        steps_down = [3200, 2000, 1000, 400]
        bursts = [(jitter(r), random.randint(15, 30)) for r in steps_up + steps_down]

    elif pattern == 'valley':
        # Mulai medium-high, turun ke valley, naik lagi
        bursts = [
            (jitter(3500), random.randint(20, 35)),
            (jitter(1800), random.randint(10, 20)),
            (jitter(500),  random.randint(10, 20)),   # valley
            (jitter(2200), random.randint(10, 20)),
            (jitter(4000), random.randint(20, 35)),
        ]

    elif pattern == 'double_peak':
        # Dua puncak, keduanya moderat
        bursts = [
            (jitter(300),  random.randint(20, 30)),
            (jitter(1500), random.randint(10, 20)),
            (jitter(4500), random.randint(15, 25)),   # peak 1
            (jitter(800),  random.randint(20, 35)),   # calm bridge
            (jitter(3500), random.randint(15, 25)),   # peak 2
            (jitter(400),  random.randint(20, 40)),
        ]

    elif pattern == 'random_walk':
        # Random tapi dibatasi max 5000
        n = random.randint(6, 12)
        bursts = [(random.randint(150, 5000), random.randint(8, 45)) for _ in range(n)]

    return bursts

if __name__ == "__main__":
    print("[TCP] 🎯 Irregular iperf3 Burst Started")
    time.sleep(600)

    cycle = 0
    while True:
        cycle += 1
        print(f"\n{'='*50}")
        print(f"[TCP] 🔁 Cycle #{cycle}")
        print(f"{'='*50}")

        bursts = build_irregular_cycle()

        for rate, duration in bursts:
            if not check_controller_state():
                time.sleep(5)
                continue

            if rate >= 3500:
                print(f"\n[TCP] 🔥 BURST  rate={rate}  dur={duration}s")
            elif rate >= 1500:
                print(f"\n[TCP] ⚡ HIGH   rate={rate}  dur={duration}s")
            else:
                print(f"\n[TCP] 🌊 NORMAL rate={rate}  dur={duration}s")

            send_tcp(rate, duration)
            random_pause()

        # Inter-cycle pause — juga irregular
        inter = random.randint(120, 600)
        print(f"\n[TCP] 💤 Inter-cycle rest {inter}s\n")
        time.sleep(inter)