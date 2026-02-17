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
        # Get H3 PID
        h3_pid = subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()
        
        # Get H2 PID & restart server
        h2_pid = subprocess.check_output(["pgrep", "-n", "-f", "h2"]).decode().strip()
        subprocess.run(["mnexec", "-a", h2_pid, "pkill", "-9", "iperf3"])
        time.sleep(0.3)
        subprocess.run(["mnexec", "-a", h2_pid, "iperf3", "-s", "-p", str(PORT), "-D"])
        time.sleep(1)
        
        # Send traffic
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
    
    while True:
        print("\n[TCP] 🌊 NORMAL"); send_tcp(120, 480)
        print("\n[TCP] 🔥 BURST"); send_tcp(8750, 120)
        print("\n[TCP] 🔥 BURST"); send_tcp(7320, 90)
        print("\n[TCP] 🌊 NORMAL"); send_tcp(110, 600)
        print("\n[TCP] 🔥 BURST"); send_tcp(8250, 180)
        print("\n[TCP] 🌊 NORMAL"); send_tcp(132, 500)
        print("\n[TCP] 🔁 Cycle Repeat\n")