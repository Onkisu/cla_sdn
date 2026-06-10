#!/usr/bin/env python3
"""
Short Burst Traffic Generator for Sidang Demo
H3 -> H2 using iperf3 UDP
"""

import time
import subprocess
import json

H3 = "h3"
DST_IP = "10.0.0.2"
PORT = 9001
STATE_FILE = "/tmp/controller_state.json"


def check_controller_state():
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)

        current_state = state.get("state", "IDLE")

        transition_states = [
            "DELETING_ALL_FLOWS",
            "WAITING_SETTLE",
            "INSTALLING_NEW_PATH",
            "REVERT_DELETING",
            "REVERT_SETTLE",
            "REVERT_INSTALLING",
        ]

        if current_state in transition_states:
            print(f"[DEMO BURST] Controller in transition: {current_state}")
            return False

        if state.get("congestion") is True and current_state == "ACTIVE_REROUTE":
            print("[DEMO BURST] Reroute already active, waiting 3s...")
            time.sleep(3)

        return True

    except Exception:
        return True


def send_udp(rate, duration):
    """
    rate value is converted to bitrate:
    bitrate = rate * 1400 * 8
    Example:
    rate 2500 ≈ 28 Mbps
    """
    try:
        h3_pid = subprocess.check_output(
            ["pgrep", "-n", "-f", H3]
        ).decode().strip()

        bitrate = rate * 1400 * 8

        print(f"[DEMO BURST] Sending UDP rate={rate}, bitrate≈{bitrate/1_000_000:.2f} Mbps, duration={duration}s")

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
        print(f"[DEMO BURST] Error: {e}")


if __name__ == "__main__":
    print("[DEMO BURST] Short demo burst started")
    print("[DEMO BURST] Waiting 10 seconds for system stabilization...")
    time.sleep(10)

    bursts = [
        # rate, duration
        (500, 10),     # normal traffic ≈ 5.6 Mbps
        (1200, 15),    # medium traffic ≈ 13.4 Mbps
        (2200, 20),    # above threshold ≈ 24.6 Mbps
        (3500, 20),    # high burst ≈ 39.2 Mbps
        (1800, 15),    # decreasing traffic ≈ 20.1 Mbps
        (500, 10),     # back to normal ≈ 5.6 Mbps
    ]

    while True:  
        for rate, duration in bursts:
            if not check_controller_state():
                time.sleep(2)

            if rate >= 3000:
                print(f"\n[DEMO BURST] HIGH BURST {rate}")
            elif rate >= 2000:
                print(f"\n[DEMO BURST] ABOVE THRESHOLD {rate}")
            else:
                print(f"\n[DEMO BURST] NORMAL/MEDIUM {rate}")

            send_udp(rate, duration)

        print("\n[DEMO BURST] Demo burst finished")