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
import random


STEADY_DURATION_MS = 60000   # 60 detik per sesi
STEADY_RATE = 50             # pps
PKT_SIZE = 160               # bytes
RESTART_DELAY = 1            # detik

traffic_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
#  SAFE HOST CMD WRAPPER
#  Mininet raises bare AssertionError when host.shell is dead/None.
#  Wrap every host.cmd() call through here so the session loop can
#  catch it gracefully and retry instead of crashing the thread.
# ─────────────────────────────────────────────────────────────────────────────

class HostDead(RuntimeError):
    """Raised when the Mininet host shell is no longer alive."""
    pass


def host_cmd(host, cmd, timeout=None):
    """
    Safe wrapper around host.cmd().
    Converts AssertionError (dead shell) to HostDead so callers
    can decide whether to retry or bail out.
    """
    try:
        if timeout is not None:
            return host.cmd(cmd, timeout=timeout)
        return host.cmd(cmd)
    except AssertionError as exc:
        raise HostDead(f"Host {host.name} shell is dead: {exc}") from exc


def check_ditg():
    return subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0


def iperf3_watchdog(host, ports=(9001, 9003), interval=5):
    """Restart iperf3 servers on host if they die."""
    while True:
        time.sleep(interval)
        try:
            for port in ports:
                running = host_cmd(host, f"pgrep -f 'iperf3 -s -p {port}'").strip()
                if not running:
                    info(f"*** [WATCHDOG] Restarting iperf3 -s -p {port}\n")
                    host_cmd(host, f"iperf3 -s -p {port} -D")
                    time.sleep(0.5)
        except HostDead as e:
            info(f"*** [WATCHDOG] {e} — stopping watchdog\n")
            return
        except Exception as e:
            info(f"*** [WATCHDOG] Unexpected error: {e}\n")


def save_itg_session_to_db(log_file="/tmp/recv_steady.log"):
    result = subprocess.check_output(["ITGDec", log_file], text=True)

    data = {}
    in_target_flow = False

    for line in result.splitlines():
        if "TOTAL RESULTS" in line:
            break

        if line.strip().startswith("From "):
            src_ip = line.strip().split()[1].split(":")[0]
            in_target_flow = (src_ip == "10.0.0.1")
            continue

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
        data["pps"],
    ))

    conn.commit()
    cur.close()
    conn.close()


def keep_steady_traffic(src_host, dst_host, dst_ip):
    for i in itertools.count(1):
        try:
            session_ts = int(time.time())
            logfile = f"/tmp/recv_steady_{session_ts}.log"

            info(f"*** [SESSION {i}] Starting ITGRecv -> {logfile}\n")

            # ── Teardown previous senders/receivers ──────────────────────────
            # Lock only for the brief teardown+startup window, NOT for the
            # entire traffic duration. This prevents the lock from being held
            # for 60+ seconds and starving other threads.
            with traffic_lock:
                host_cmd(src_host, "pkill -9 ITGSend 2>/dev/null; true")
                host_cmd(dst_host, "pkill -9 ITGRecv 2>/dev/null; true")
                time.sleep(0.8)  # give OS time to clean up sockets
                host_cmd(dst_host, f"ITGRecv -l {logfile} &")
                time.sleep(1.0)

            # ── Wait for ITGRecv to be ready ─────────────────────────────────
            recv_ready = False
            for _ in range(10):
                pid = host_cmd(dst_host, "pgrep -f 'ITGRecv'").strip()
                if pid:
                    info(f"*** ITGRecv ready (PID {pid})\n")
                    recv_ready = True
                    break
                time.sleep(0.5)

            if not recv_ready:
                info("!!! ITGRecv failed to start — skipping session\n")
                time.sleep(RESTART_DELAY)
                continue

            # ── Build session parameters ──────────────────────────────────────
            base_rate    = 50
            current_rate = max(30, base_rate + random.randint(-10, 15))
            packet_size  = random.randint(140, 200)
            duration     = STEADY_DURATION_MS + random.randint(-5000, 5000)

            # Random silence
            if random.random() < 0.25:
                silence_time = random.uniform(0.5, 2.0)
                info(f"*** Simulating silence {silence_time:.2f}s\n")
                time.sleep(silence_time)

            # Occasional micro-burst (lock only while issuing the command)
            if random.random() < 0.15:
                burst_rate = random.randint(80, 120)
                info(f"*** Micro burst at {burst_rate} pps\n")
                with traffic_lock:
                    host_cmd(
                        src_host,
                        f'ITGSend -T UDP -a {dst_ip} -rp 9000 '
                        f'-c {packet_size} -C {burst_rate} '
                        f'-t 3000 -l /dev/null'
                    )
                # NOTE: this is a *blocking* micro-burst (3 s). If you want it
                # non-blocking, append '&' and remove the with-lock — the
                # lock would then only protect the command string dispatch.

            info(f"*** Starting Noisy VoIP: {current_rate} pps | {packet_size} bytes\n")

            TCP_BASE_RATE  = 260
            TCP_VARIATION  = 20
            tcp_rate       = TCP_BASE_RATE + random.randint(-TCP_VARIATION, TCP_VARIATION)
            tcp_pkt_size   = 1200
            tcp_duration   = STEADY_DURATION_MS

            # ── Launch background traffic (non-blocking, lock only for dispatch) ──
            with traffic_lock:
                host_cmd(
                    src_host,
                    f'ITGSend -T UDP -a {dst_ip} -rp 9000 '
                    f'-c {packet_size} -C {current_rate} '
                    f'-t {duration} -l /dev/null &'
                )
                host_cmd(
                    src_host,
                    f'iperf3 -c {dst_ip} -p 9003 '
                    f'-b {tcp_rate * tcp_pkt_size * 8} '
                    f'-t {tcp_duration // 1000} &'
                )

            # ── Wait for session to complete ──────────────────────────────────
            time.sleep(max(duration, tcp_duration) / 1000)

            # ── Persist to DB ─────────────────────────────────────────────────
            try:
                save_itg_session_to_db(logfile)
            except Exception as e:
                info(f"!!! DB SAVE FAILED: {e}\n")

            time.sleep(RESTART_DELAY)

        except HostDead as e:
            # Shell is gone — no point retrying this session, stop the thread.
            info(f"*** [SESSION {i}] Host dead, stopping traffic loop: {e}\n")
            break

        except Exception as e:
            # Any other error (network blip, ITG parse issue, etc.) — log and
            # continue to the next session rather than killing the thread.
            info(f"*** [SESSION {i}] Traffic loop error (retrying): {type(e).__name__}: {e}\n")
            time.sleep(RESTART_DELAY * 2)


def run():
    info("*** Starting Spine-Leaf Topology\n")

    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True,
    )

    net.addController('c0', ip='127.0.0.1', port=6653)

    spines, leaves = [], []

    for i in range(1, 4):
        spines.append(net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13'))
    for i in range(1, 4):
        leaves.append(net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13'))

    for l in leaves:
        for s in spines:
            net.addLink(l, s, bw=55, delay='1ms', max_queue_size=10, use_htb=True)

    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')

    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    net.start()
    time.sleep(3)
    net.pingAll()

    time.sleep(1)
    h2.cmd("iperf3 -s -p 9001 -D")
    h2.cmd("iperf3 -s -p 9003 -D")
    time.sleep(1)

    wd = threading.Thread(target=iperf3_watchdog, args=(h2,), daemon=True)
    wd.start()

    if check_ditg():
        time.sleep(1)
        info("*** Starting STEADY VoIP thread (h1 -> h2)\n")
        t = threading.Thread(
            target=keep_steady_traffic,
            args=(h1, h2, h2.IP()),
            daemon=True,
        )
        t.start()
    else:
        info("!!! D-ITG not installed, traffic disabled\n")

    info("*** Running Mininet CLI\n")
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    run()