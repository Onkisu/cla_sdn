#!/usr/bin/env python3
import time
import subprocess

H3 = "h3"
DST_IP = "10.0.0.2"

def clear_recv_log():
    subprocess.run(["truncate", "-s", "0", "/tmp/recv_steady.log"])
    print("[CLEANUP] /tmp/recv_steady.log cleared")

def burst(rate, duration):
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
    while True:
        # Burst 1
        time.sleep(180) # Wait 30 minutes before starting bursts
        burst(rate=150, duration=60)
        # Burst 2
        time.sleep(1)
        burst(rate=200, duration=30)
        
        time.sleep(1)
        # Burst 3
        burst(rate=250, duration=20)
        time.sleep(1)

        # Burst 3
        burst(rate=300, duration=20)
        time.sleep(1)
        # Burst 4
        burst(rate=400, duration=10)
        time.sleep(1)

        # # Burst 5
        # burst(rate=250, duration=20)
        # time.sleep(1)
        # # Burst 6
        # burst(rate=360, duration=10)
        # time.sleep(1)
        # # Burst 7
        # burst(rate=200, duration=20)
        # time.sleep(1)
        # # Burst 8
        # burst(rate=150, duration=30)

        # time.sleep(180) # Wait 20 minutes before starting bursts
        # burst(rate=150, duration=60)
        # # Burst 2
        # time.sleep(1)
        # burst(rate=200, duration=30)
        
        # time.sleep(1)
        # # Burst 3
        # burst(rate=250, duration=20)
        # time.sleep(1)

        # # Burst 3
        # burst(rate=300, duration=20)
        # time.sleep(1)
        # # Burst 4
        # burst(rate=400, duration=10)
        # time.sleep(1)

        # # Burst 5
        # burst(rate=250, duration=20)
        # time.sleep(1)
        # # Burst 6
        # burst(rate=360, duration=10)
        # time.sleep(1)
        # # Burst 7
        # burst(rate=200, duration=20)
        # time.sleep(1)
        # # Burst 8
        # burst(rate=150, duration=30)

        # time.sleep(180) # Wait 30 minutes before starting bursts
        # burst(rate=150, duration=60)
        # # Burst 2
        # time.sleep(1)
        # burst(rate=200, duration=30)
        
        # time.sleep(1)
        # # Burst 3
        # burst(rate=250, duration=20)
        # time.sleep(1)

        # # Burst 3
        # burst(rate=300, duration=20)
        # time.sleep(1)
        # # Burst 4
        # burst(rate=400, duration=10)
        # time.sleep(1)

        # # Burst 5
        # burst(rate=250, duration=20)
        # time.sleep(1)
        # # Burst 6
        # burst(rate=360, duration=10)
        # time.sleep(1)
        # # Burst 7
        # burst(rate=200, duration=20)
        # time.sleep(1)
        # # Burst 8
        # burst(rate=150, duration=30)
        clear_recv_log()
        print("[CYCLE COMPLETE] Restarting burst cycle...\n")