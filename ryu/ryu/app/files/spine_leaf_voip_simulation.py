#!/usr/bin/env python3

from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import subprocess
import threading
import itertools
import psycopg2
import re
import random

STEADY_DURATION_MS = 60000   # 60 detik per sesi
STEADY_RATE = 50             # pps
PKT_SIZE = 160               # bytes
RESTART_DELAY = 1            # detik

traffic_lock = threading.Lock()

def check_ditg():
    return subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0

def check_iperf3():
    return subprocess.run(['which', 'iperf3'], capture_output=True).returncode == 0

def save_itg_session_to_db(log_file="/tmp/recv_steady.log"):
    # Jalankan ITGDec
    result = subprocess.check_output(
        ["ITGDec", log_file],
        text=True
    )

    data = {}
    in_target_flow = False

    for line in result.splitlines():
        # Stop kalau masuk TOTAL RESULTS
        if "TOTAL RESULTS" in line:
            break

        # Detect flow src IP dari baris "From 10.0.0.1:51102"
        if line.strip().startswith("From "):
            src_ip = line.strip().split()[1].split(":")[0]
            in_target_flow = (src_ip == "10.0.0.1")
            continue

        # Skip kalau bukan flow dari 10.0.0.1
        if not in_target_flow:
            continue

        if "Total time" in line:
            data["duration"] = float(line.split("=")[1].split()[0])
        elif "Total packets" in line:
            data["total_packets"] = int(line.split("=")[1])
        elif "Packets dropped" in line:
            data["dropped"] = int(line.split("=")[1].split()[0])
        elif "Average delay" in line:
            data["avg_delay"] = float(line.split("=")[1].split()[0]) * 1000
        elif "Average jitter" in line:
            data["avg_jitter"] = float(line.split("=")[1].split()[0]) * 1000
        elif "Maximum delay" in line:
            data["max_delay"] = float(line.split("=")[1].split()[0]) * 1000
        elif "Average bitrate" in line:
            data["bitrate"] = float(line.split("=")[1].split()[0])
        elif "Average packet rate" in line:
            data["pps"] = float(line.split("=")[1].split()[0])

    # Kalau flow 10.0.0.1 tidak ditemukan, jangan save
    if "total_packets" not in data:
        info(f"!!! No flow from 10.0.0.1 found in {log_file}, skipping\n")
        return

    data["loss"] = (
        data["dropped"] / data["total_packets"] * 100
        if data["total_packets"] > 0 else 0
    )

    conn = psycopg2.connect(
        host="127.0.0.1",
        dbname="development",
        user="dev_one",
        password="hijack332."
    )
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO traffic.itg_session_results (
            src_ip, dst_ip,
            duration_sec, total_packets,
            packets_dropped, loss_percent,
            avg_delay_ms, avg_jitter_ms, max_delay_ms,
            avg_bitrate_kbps, avg_pps
        ) VALUES (
            '10.0.0.1', '10.0.0.2',
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s
        )
    """, (
        data["duration"],
        data["total_packets"],
        data["dropped"],
        data["loss"],
        data["avg_delay"],
        data["avg_jitter"],
        data["max_delay"],
        data["bitrate"],
        data["pps"]
    ))

    conn.commit()
    cur.close()
    conn.close()

def keep_steady_traffic(src_host, dst_host, dst_ip):
    for i in itertools.count(1):
        try:
            # Check if hosts still alive
            if not hasattr(dst_host, 'shell') or dst_host.shell is None:
                info("*** Host disconnected\n")
                break

            session_ts = int(time.time())
            logfile = f"/tmp/recv_steady_{session_ts}.log"

            info(f"*** [SESSION {i}] Starting ITGRecv -> {logfile}\n")

            with traffic_lock:  # LOCK akses host
                # Kill old senders
                src_host.cmd("pkill -9 ITGSend")
                src_host.cmd("pkill -9 iperf3")
                time.sleep(0.5)

                # Kill existing receivers
                dst_host.cmd("pkill -f 'ITGRecv -Sp 9000'")
                dst_host.cmd("pkill -f 'iperf3 -s -p 9001'")
                dst_host.cmd("pkill -f 'iperf3 -s -p 9003'")
                time.sleep(0.5)

                # Start ITGRecv for VoIP (UDP port 9000)
                dst_host.cmd(f"ITGRecv -Sp 9000 -l {logfile} &")
                
                # Start iperf3 servers for burst (port 9001) and steady TCP (port 9003)
                dst_host.cmd(f"iperf3 -s -p 9001 -D --logfile /tmp/iperf_burst_{session_ts}.log")
                dst_host.cmd(f"iperf3 -s -p 9003 -D --logfile /tmp/iperf_steady_{session_ts}.log")

            # Tunggu iperf3 servers siap
            time.sleep(1)
            for port in [9001, 9003]:
                for _ in range(10):
                    check = dst_host.cmd(f"pgrep -f 'iperf3 -s -p {port}'").strip()
                    if check:
                        info(f"*** iperf3 server {port} ready (PID {check})\n")
                        break
                    time.sleep(0.5)
                else:
                    info(f"!!! iperf3 server {port} failed to start!\n")

            # --- Generate VoIP-like random behavior ---
            base_rate = 50
            rate_variation = random.randint(-10, 15)
            current_rate = max(30, base_rate + rate_variation)
            packet_size = random.randint(140, 200)
            duration = STEADY_DURATION_MS + random.randint(-5000, 5000)

            # Random silence
            if random.random() < 0.25:
                silence_time = random.uniform(0.5, 2.0)
                info(f"*** Simulating silence {silence_time:.2f}s\n")
                time.sleep(silence_time)

            # Occasional micro-burst
            if random.random() < 0.15:
                burst_rate = random.randint(80, 120)
                info(f"*** Micro burst at {burst_rate} pps\n")
                with traffic_lock:
                    src_host.cmd(
                        f'ITGSend -T UDP -a {dst_ip} '
                        f'-rp 9000 '
                        f'-c {packet_size} -C {burst_rate} '
                        f'-t 3000 -l /dev/null'
                    )

            info(f"*** Starting Noisy VoIP: {current_rate} pps | {packet_size} bytes\n")

            # TCP background traffic using iperf3 (port 9003)
            # Bandwidth calculation: ~260 pps * 1200 bytes * 8 bits = ~2.5 Mbps
            TCP_BASE_RATE_MBPS = 2.5
            TCP_VARIATION_MBPS = 0.3
            tcp_rate_mbps = TCP_BASE_RATE_MBPS + random.uniform(-TCP_VARIATION_MBPS, TCP_VARIATION_MBPS)
            tcp_duration_sec = duration / 1000

            # Start background traffic (UDP + TCP)
            with traffic_lock:
                # UDP VoIP traffic
                src_host.cmd(
                    f'ITGSend -T UDP -a {dst_ip} '
                    f'-rp 9000 '
                    f'-c {packet_size} -C {current_rate} '
                    f'-t {duration} -l /dev/null &'
                )
                
                # TCP steady traffic using iperf3
                src_host.cmd(
                    f'iperf3 -c {dst_ip} -p 9003 '
                    f'-b {tcp_rate_mbps}M -t {tcp_duration_sec} '
                    f'--logfile /tmp/iperf_client_steady_{session_ts}.log &'
                )

            # Verifikasi TCP sender
            for _ in range(5):
                check = src_host.cmd("pgrep -f 'iperf3 -c.*9003'").strip()
                if check:
                    info(f"*** TCP:9003 iperf3 client running (PID {check})\n")
                    break
                time.sleep(0.5)
            else:
                info("!!! TCP:9003 iperf3 client FAILED to start!\n")

            # Tunggu durasi selesai (ms → sec)
            time.sleep(tcp_duration_sec + 1)

            # Save session ke DB
            try:
                save_itg_session_to_db(logfile)
            except Exception as e:
                info(f"!!! DB SAVE FAILED: {e}\n")

            time.sleep(RESTART_DELAY)

        except Exception as e:
            info(f"*** Traffic loop error: {e}\n")
            break

def run():
    info("*** Starting Spine-Leaf Topology (VoIP + iperf3)\n")

    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True
    )

    net.addController('c0', ip='127.0.0.1', port=6653)

    spines, leaves = [], []

    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13'))
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13'))

    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=100, delay='1ms', max_queue_size=10, use_htb=True)

    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')

    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    net.start()
    time.sleep(3)
    net.pingAll()

    if not check_ditg():
        info("!!! D-ITG not installed, VoIP traffic disabled\n")
    
    if not check_iperf3():
        info("!!! iperf3 not installed, TCP traffic disabled\n")
    
    if check_ditg() and check_iperf3():
        time.sleep(1)

        info("*** Starting STEADY VoIP + TCP Watchdog (h1 -> h2)\n")
        t = threading.Thread(
            target=keep_steady_traffic,
            args=(h1, h2, h2.IP()) 
        )
        t.daemon = True
        t.start()

    info("*** Running Mininet CLI\n")
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()