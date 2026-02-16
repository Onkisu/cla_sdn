#!/usr/bin/env python3
"""
Burst Traffic Generator for H3
Coordinates with controller state to avoid interference during reroute
"""
import time
import subprocess
import json

H3 = "h3"
DST_IP = "10.0.0.2"
STATE_FILE = '/tmp/controller_state.json'

def check_controller_state():
    """
    Check controller state before sending burst
    Returns True if safe to send, False if controller is busy
    """
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        current_state = state.get('state', 'IDLE')
        
        # Don't send burst if controller is in transition states
        transition_states = [
            'DELETING_ALL_FLOWS',
            'WAITING_SETTLE', 
            'INSTALLING_NEW_PATH',
            'REVERT_DELETING',
            'REVERT_SETTLE',
            'REVERT_INSTALLING'
        ]
        
        if current_state in transition_states:
            print(f"[BURST] ⏸️  Waiting - Controller in {current_state} state")
            return False
        
        # If controller just rerouted, wait a bit
        if state.get('congestion') == True and current_state == 'ACTIVE_REROUTE':
            print("[BURST] ⏸️  Reroute active, waiting 3 seconds...")
            time.sleep(3)
        
        return True
        
    except FileNotFoundError:
        # File doesn't exist yet, allow burst
        return True
    except Exception as e:
        print(f"[BURST] ⚠️  State check error: {e}")
        return True  # Default allow on error

def send_tcp(rate, duration):
    try:
        pid = subprocess.check_output(
            ["pgrep", "-n", "-f", H3]
        ).decode().strip()

        subprocess.run([
            "mnexec", "-a",
            pid,
            "ITGSend",
            "-T", "TCP",
            "-rp", "9001",
            "-a", DST_IP,
            "-c", "160",
            "-C", str(rate),
            "-t", str(duration * 1000),
            "-l", "/dev/null"
        ], timeout=duration + 5)

    except Exception as e:
        print(f"[TCP] Error: {e}")


if __name__ == "__main__":
    print("[TCP] 🎯 Structured Continuous TCP Started")

    while True:

        # 1️⃣ NORMAL PHASE (8 minutes)
        print("\n[TCP] 🌊 NORMAL PHASE")
        send_tcp(120, 480)   # 8 menit

        # 2️⃣ RAMP UP (2 minutes)
        print("\n[TCP] 📈 RAMP UP")
        send_tcp(180, 120)

        # 3️⃣ BURST (2 minutes)
        print("\n[TCP] 🔥 BURST")
        send_tcp(350, 120)

        # 4️⃣ COOLDOWN (3 minutes)
        print("\n[TCP] ❄ COOLDOWN")
        send_tcp(150, 180)

        print("\n[TCP] 🔁 Cycle Repeat\n")
