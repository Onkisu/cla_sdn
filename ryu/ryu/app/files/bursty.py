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
        
        # Don't send burst if controller is in transition state
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

def burst(rate, duration):
    """
    Send burst traffic from H3 to H2
    """
    # Wait for controller to be ready
    max_wait = 10
    waited = 0
    while not check_controller_state() and waited < max_wait:
        time.sleep(1)
        waited += 1
    
    if waited >= max_wait:
        print(f"[BURST] ⏭️  Skipping burst (controller busy too long)")
        return
    
    print(f"[BURST] 🚀 Sending: rate={rate} pps | duration={duration}s")
    
    try:
        pid = subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()
        subprocess.run([
            "mnexec", "-a",
            pid,
            "ITGSend",
            "-T", "UDP",
            "-rp", "9001",
            "-a", DST_IP,
            "-c", "160",
            "-C", str(rate),
            "-t", str(duration * 1000),
            "-l", "/dev/null"
        ], timeout=duration + 5)
        
        print(f"[BURST] ✅ Completed: {rate} pps for {duration}s")
        
    except subprocess.TimeoutExpired:
        print(f"[BURST] ⚠️  Timeout after {duration}s")
    except Exception as e:
        print(f"[BURST] ❌ Error: {e}")

if __name__ == "__main__":
    print("[BURST] 🎯 Burst Generator Started (Controller-Aware)")
    print("[BURST] 📍 H3 -> H2 via Spine 2 (permanent path)")
    print("[BURST] ⏰ Initial wait: 10 minutes before first burst")
    
    while True:
        # Initial wait - 10 minutes
        print("[BURST] ⏳ Waiting 600 seconds before burst cycle...")
        time.sleep(600)
        
        print("[BURST] 🔥 Starting burst sequence...")
        
        # Burst sequence with cooldown
        bursts = [
            (150, 60),   # Warm-up
            (200, 30),   # Ramp up
            (250, 20),   # Medium
            (300, 20),   # High
            (400, 10),   # Peak
            (250, 20),   # Cool down
            (360, 10),   # Second peak
            (200, 20),   # Ramp down
            (150, 30)    # Final cool down
        ]
        
        for i, (rate, duration) in enumerate(bursts, 1):
            print(f"\n[BURST] === Burst {i}/{len(bursts)} ===")
            burst(rate, duration)
            
            if i < len(bursts):
                print(f"[BURST] 💤 Cooldown 2 seconds...")
                time.sleep(2)  # Cooldown between bursts
        
        print("\n[BURST] ✅ Burst sequence completed")
        print("[BURST] ⏳ Waiting 500 seconds before next cycle...")
        time.sleep(500)