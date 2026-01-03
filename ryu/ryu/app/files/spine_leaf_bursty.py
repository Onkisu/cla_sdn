#!/usr/bin/env python3
import time
import subprocess

H3 = "h3"
DST_IP = "10.0.0.2"

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
        time.sleep(60)
        burst(rate=300, duration=60)

        # Burst 2
        time.sleep(30)
        burst(rate=500, duration=30)
