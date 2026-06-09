#!/usr/bin/env python3
"""
Short Burst Traffic Generator - UDP FLOOD for Sidang Demo
H3 -> H2 using iperf3 UDP

Demo pattern:
idle -> ramp-up -> plateau -> spike -> ramp-down -> idle
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
            print(f"[DEMO UDP] Controller in transition: {current_state}")
            return False

        if state.get("congestion") is True and current_state == "ACTIVE_REROUTE":
            print("[DEMO UDP] Reroute already active, waiting 3s...")
            time.sleep(3)

        return True

    except Exception:
        return True


def get_h3_pid():
    return subprocess.check_output(["pgrep", "-n", "-f", H3]).decode().strip()


def send_udp_flood(rate_mbps, duration, parallel=4):
    try:
        h3_pid = get_h3_pid()
        bw_bps = int(rate_mbps * 1_000_000)

        print(
            f"[DEMO UDP] Sending {rate_mbps} Mbps UDP flood "
            f"x{parallel} streams for {duration}s"
        )

        subprocess.run([
            "mnexec", "-a", h3_pid,
            "iperf3",
            "-c", DST_IP,
            "-p", str(PORT),
            "-u",
            "-b", str(bw_bps),
            "-P", str(parallel),
            "-t", str(duration),
            "--length", "1400",
        ], timeout=duration + 10)

    except subprocess.TimeoutExpired:
        print("[DEMO UDP] Timeout, burst finished")
    except Exception as e:
        print(f"[DEMO UDP] Error: {e}")


def run_demo_burst(pattern):
    for rate_mbps, duration in pattern:
        if rate_mbps == 0:
            print(f"[DEMO UDP] IDLE {duration}s — no burst traffic")
            time.sleep(duration)
            continue

        if not check_controller_state():
            time.sleep(2)

        if rate_mbps >= 120:
            tag = "SPIKE"
        elif rate_mbps >= 70:
            tag = "PLATEAU"
        elif rate_mbps >= 30:
            tag = "RAMP"
        else:
            tag = "LOW"

        print(f"\n[DEMO UDP] {tag} | {rate_mbps} Mbps | {duration}s")

        parallel = 6 if rate_mbps >= 100 else 4
        send_udp_flood(rate_mbps, duration, parallel=parallel)


if __name__ == "__main__":
    print("[DEMO UDP] Short UDP Flood Demo Started")
    print("[DEMO UDP] Waiting 10 seconds for system stabilization...")
    time.sleep(10)

    demo_pattern = [
        (0, 10),      # idle
        (30, 10),     # ramp-up
        (70, 15),     # plateau
        (120, 15),    # high burst
        (180, 15),    # spike
        (100, 10),    # down
        (50, 10),     # ramp-down
        (0, 10),      # idle
    ]

    run_demo_burst(demo_pattern)

    print("\n[DEMO UDP] Demo UDP flood finished")