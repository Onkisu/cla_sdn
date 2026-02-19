#!/usr/bin/env python3
"""
Burst Traffic Generator for H3 using iperf3
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

if __name__ == "__main__":
    print("[TCP] 🎯 iperf3 Burst Started")
    time.sleep(600)
    while True:

        bursts = [
            (250, 60),
            (500, 30),
            (1200, 20),
            (2500, 20),
            (4500, 15),

            (8750, 30),
            (7200, 45),
            (5400, 30),

            (3200, 20),
            (1800, 20),
            (900, 30),

            (400, 60),
            (250, 120)
        ]

        for rate, duration in bursts:
            if rate >= 5000:
                print(f"\n[TCP] 🔥 BURST {rate}")
            elif rate >= 1000:
                print(f"\n[TCP] ⚡ HIGH {rate}")
            else:
                print(f"\n[TCP] 🌊 NORMAL {rate}")

            send_tcp(rate, duration)

        print("\n[TCP] 🔁 Cycle Repeat\n")
        time.sleep(500)

