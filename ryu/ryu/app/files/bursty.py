#!/usr/bin/env python3
import time
import subprocess
import json

H3 = "h3"
DST_IP = "10.0.0.2"
STATE_FILE = '/tmp/controller_state.json'

def check_controller_state():
    """Cek state controller sebelum mengirim burst"""
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        # Jangan kirim burst jika controller sedang transition
        if state.get('state') in ['VERIFYING_SILENCE', 'REVERT_VERIFY']:
            print(f"[BURST] Skipping - Controller in {state['state']} state")
            return False
        
        # Jika baru saja reroute, tunggu sedikit
        if state.get('congestion') == True:
            print("[BURST] Reroute active, waiting...")
            time.sleep(2)
            
        return True
    except:
        return True  # Default allow jika file tidak ada

def burst(rate, duration):
    if not check_controller_state():
        return
    
    print(f"[BURST] rate={rate} pps | duration={duration}s")
    subprocess.run([
        "mnexec", "-a",
        subprocess.check_output(["pgrep", "-f", H3]).decode().strip(),
        "ITGSend",
        "-T", "UDP",
        "-a", DST_IP,
        "-c", "160",
        "-C", str(rate),
        "-t", str(duration * 1000),
        "-l", "/dev/null"
    ])

if __name__ == "__main__":
    print("[BURST] Generator with controller awareness started")
    
    while True:
        # Initial wait
        time.sleep(600)  # Wait 10 minutes
        
        # Burst sequence dengan cooldown
        bursts = [
            (150, 60), (200, 30), (250, 20),
            (300, 20), (400, 10), (250, 20),
            (360, 10), (200, 20), (150, 30)
        ]
        
        for rate, duration in bursts:
            burst(rate, duration)
            time.sleep(1)  # Cooldown antar burst
        
        # Wait sebelum cycle berikutnya
        time.sleep(500)  