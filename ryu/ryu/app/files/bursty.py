#!/usr/bin/env python3
"""
Burst Traffic Generator for H3 using iperf3
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

def send_tcp_iperf3(bandwidth_mbps, duration_sec):
    """
    Send TCP traffic using iperf3
    Args:
        bandwidth_mbps: Target bandwidth in Mbps
        duration_sec: Duration in seconds
    """
    try:
        # Get h3 PID for mnexec
        pid = subprocess.check_output(
            ["pgrep", "-n", "-f", H3]
        ).decode().strip()

        # Run iperf3 client
        # Port 9001 digunakan untuk burst traffic
        result = subprocess.run([
            "mnexec", "-a", pid,
            "iperf3",
            "-c", DST_IP,
            "-p", "9001",
            "-b", f"{bandwidth_mbps}M",
            "-t", str(duration_sec),
            "--logfile", f"/tmp/h3_burst_{int(time.time())}.log"
        ], timeout=duration_sec + 10, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"[iperf3] Error: {result.stderr}")
        else:
            print(f"[iperf3] ✓ Sent {bandwidth_mbps}M for {duration_sec}s")

    except subprocess.TimeoutExpired:
        print(f"[iperf3] Timeout after {duration_sec}s")
    except Exception as e:
        print(f"[iperf3] Error: {e}")

if __name__ == "__main__":
    print("[iperf3] 🎯 Structured Continuous TCP Burst Started")
    print("[iperf3] Using iperf3 instead of ITGSend")

    while True:
        # 1️⃣ NORMAL PHASE (8 minutes)
        # Bandwidth calculation: 120 pps * 1400 bytes * 8 bits ≈ 1.344 Mbps
        print("\n[iperf3] 🌊 NORMAL PHASE")
        send_tcp_iperf3(1.3, 480)   # 8 menit

        # 2️⃣ RAMP UP (2 minutes)
        # 8750 pps * 1400 bytes * 8 bits ≈ 98 Mbps
        print("\n[iperf3] 🔥 BURST")
        send_tcp_iperf3(98, 120)

        # 3️⃣ BURST (90 seconds)
        # 7320 pps * 1400 bytes * 8 bits ≈ 82 Mbps
        print("\n[iperf3] 🔥 BURST")
        send_tcp_iperf3(82, 90)

        # 4️⃣ NORMAL (10 minutes)
        # 110 pps * 1400 bytes * 8 bits ≈ 1.232 Mbps
        print("\n[iperf3] 🌊 NORMAL PHASE")
        send_tcp_iperf3(1.2, 600)  # 10 menit
        
        # 5️⃣ BURST (3 minutes)
        # 8250 pps * 1400 bytes * 8 bits ≈ 92.4 Mbps
        print("\n[iperf3] 🔥 BURST")
        send_tcp_iperf3(92, 180)

        # 6️⃣ NORMAL (8 minutes 20 seconds)
        # 132 pps * 1400 bytes * 8 bits ≈ 1.478 Mbps
        print("\n[iperf3] 🌊 NORMAL PHASE")
        send_tcp_iperf3(1.5, 500)   # 8 menit 20 detik
        
        print("\n[iperf3] 🔁 Cycle Repeat\n")