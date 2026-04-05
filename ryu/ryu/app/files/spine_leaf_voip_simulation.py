#!/usr/bin/env python3
"""
Spine-Leaf VoIP Simulation — FIXED VERSION
========================================================
ROOT CAUSE yang diperbaiki:

1. RACE CONDITION host.cmd() tidak thread-safe
   → Semua perintah ke host (ITGSend, ITGRecv, iperf3) dipindah ke
     subprocess via mnexec dari luar Mininet, BUKAN via host.cmd() di thread.
     Ini persis seperti cara bursty_2.py bekerja — terbukti stabil.

2. time.sleep() di dalam with traffic_lock
   → Lock sekarang hanya untuk flag/state, bukan untuk I/O blocking.

3. State OVS kotor dari run sebelumnya
   → Script sekarang memanggil cleanup OVS sebelum net.start().

4. net.waitConnected() ditambahkan setelah net.start()
   → Memastikan semua switch benar-benar terhubung ke controller
     sebelum traffic thread distart.

5. Watchdog sekarang pakai subprocess/mnexec, bukan host.cmd()
   → Tidak ada lagi akses concurrent ke host shell.
"""

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
import os
import signal

STEADY_DURATION_MS = 60000
RESTART_DELAY      = 1

# ─── PID cache untuk mnexec ──────────────────────────────────────────────────
_pid_cache = {}
_pid_lock  = threading.Lock()

def get_host_pid(hostname):
    """
    Ambil PID shell dari host Mininet via pgrep.
    Di-cache agar tidak pgrep terus-terusan.
    Ini cara yang sama seperti bursty_2.py — AMAN dari thread manapun.
    """
    with _pid_lock:
        if hostname in _pid_cache:
            return _pid_cache[hostname]
        try:
            pid = subprocess.check_output(
                ["pgrep", "-n", "-f", hostname],
                stderr=subprocess.DEVNULL
            ).decode().strip()
            if pid:
                _pid_cache[hostname] = pid
                return pid
        except Exception:
            pass
        return None

def invalidate_pid_cache():
    with _pid_lock:
        _pid_cache.clear()

def mnexec_run(hostname, cmd, timeout=10, background=False):
    """
    Jalankan perintah di namespace host via mnexec.
    TIDAK menggunakan host.cmd() sama sekali → thread-safe 100%.
    """
    pid = get_host_pid(hostname)
    if not pid:
        info(f"!!! Cannot get PID for {hostname}\n")
        return None

    full_cmd = ["mnexec", "-a", pid] + cmd
    try:
        if background:
            # Non-blocking, return Popen object
            return subprocess.Popen(
                full_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        else:
            return subprocess.run(
                full_cmd,
                timeout=timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
    except subprocess.TimeoutExpired:
        info(f"!!! mnexec timeout on {hostname}: {cmd}\n")
        return None
    except Exception as e:
        info(f"!!! mnexec error on {hostname}: {e}\n")
        return None

def pkill_in_host(hostname, pattern):
    """Kill proses di dalam namespace host."""
    mnexec_run(hostname, ["pkill", "-9", "-f", pattern], timeout=5)

# ─── Cleanup OVS sebelum start ───────────────────────────────────────────────
def pre_cleanup():
    """
    Bersihkan state OVS dan proses sisa dari run sebelumnya.
    Ekuivalen dengan 'mn -c' tapi lebih selektif.
    """
    info("*** Pre-cleanup: killing leftover processes\n")
    for proc in ["ITGSend", "ITGRecv", "iperf3"]:
        subprocess.run(["pkill", "-9", "-f", proc],
                       stderr=subprocess.DEVNULL)
    time.sleep(1)

    info("*** Pre-cleanup: resetting OVS bridges\n")
    try:
        result = subprocess.run(
            ["ovs-vsctl", "list-br"],
            capture_output=True, text=True
        )
        for br in result.stdout.strip().splitlines():
            subprocess.run(["ovs-vsctl", "del-br", br],
                           stderr=subprocess.DEVNULL)
    except Exception as e:
        info(f"*** OVS cleanup warning (non-fatal): {e}\n")
    time.sleep(1)

# ─── Database save ────────────────────────────────────────────────────────────
def save_itg_session_to_db(log_file):
    try:
        result = subprocess.check_output(["ITGDec", log_file], text=True)
    except Exception as e:
        info(f"!!! ITGDec failed: {e}\n")
        return

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
        if "Total time"        in line: data["duration"]      = float(line.split("=")[1].split()[0])
        elif "Total packets"   in line: data["total_packets"] = int(line.split("=")[1])
        elif "Packets dropped" in line: data["dropped"]       = int(line.split("=")[1].split()[0])
        elif "Average delay"   in line: data["avg_delay"]     = float(line.split("=")[1].split()[0]) * 1000
        elif "Average jitter"  in line: data["avg_jitter"]    = float(line.split("=")[1].split()[0]) * 1000
        elif "Maximum delay"   in line: data["max_delay"]     = float(line.split("=")[1].split()[0]) * 1000
        elif "Average bitrate" in line: data["bitrate"]       = float(line.split("=")[1].split()[0])
        elif "Average packet rate" in line: data["pps"]       = float(line.split("=")[1].split()[0])

    if "total_packets" not in data:
        info(f"!!! No flow from 10.0.0.1 in {log_file}, skipping DB save\n")
        return

    data["loss"] = (
        data["dropped"] / data["total_packets"] * 100
        if data["total_packets"] > 0 else 0
    )

    try:
        conn = psycopg2.connect(
            host="127.0.0.1", dbname="development",
            user="dev_one", password="hijack332."
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO traffic.itg_session_results (
                src_ip, dst_ip,
                duration_sec, total_packets, packets_dropped, loss_percent,
                avg_delay_ms, avg_jitter_ms, max_delay_ms,
                avg_bitrate_kbps, avg_pps
            ) VALUES (
                '10.0.0.1', '10.0.0.2',
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            data["duration"], data["total_packets"], data["dropped"],
            data["loss"], data["avg_delay"], data["avg_jitter"],
            data["max_delay"], data["bitrate"], data["pps"],
        ))
        conn.commit()
        cur.close()
        conn.close()
        info("*** DB save OK\n")
    except Exception as e:
        info(f"!!! DB SAVE FAILED: {e}\n")

# ─── Watchdog iperf3 server — pakai mnexec, bukan host.cmd() ─────────────────
def iperf3_watchdog(h2_name, ports=(9001, 9003), interval=10):
    """
    Cek dan restart iperf3 server di h2 jika mati.
    Pakai mnexec → tidak ada akses ke host.shell → thread-safe.
    """
    while True:
        time.sleep(interval)
        for port in ports:
            try:
                result = mnexec_run(
                    h2_name,
                    ["pgrep", "-f", f"iperf3 -s -p {port}"],
                    timeout=5
                )
                running = result and result.stdout.strip()
                if not running:
                    info(f"*** [WATCHDOG] Restarting iperf3 -s -p {port} on {h2_name}\n")
                    mnexec_run(h2_name,
                               ["iperf3", "-s", "-p", str(port), "-D"],
                               timeout=5)
            except Exception as e:
                info(f"*** [WATCHDOG] Error checking port {port}: {e}\n")

# ─── Traffic loop — sepenuhnya via mnexec, ZERO host.cmd() ───────────────────
def keep_steady_traffic(h1_name, h2_name, dst_ip):
    """
    VoIP steady traffic h1 → h2.
    Semua command via mnexec dari subprocess biasa.
    Tidak ada host.cmd(), tidak ada race condition.
    """
    for i in itertools.count(1):
        try:
            session_ts = int(time.time())
            logfile    = f"/tmp/recv_steady_{session_ts}.log"

            info(f"*** [SESSION {i}] Starting ITGRecv -> {logfile}\n")

            # ── Teardown sisa sesi sebelumnya ────────────────────────────────
            pkill_in_host(h1_name, "ITGSend")
            pkill_in_host(h2_name, "ITGRecv")
            time.sleep(1.0)

            # ── Start ITGRecv di h2 ──────────────────────────────────────────
            mnexec_run(h2_name, ["ITGRecv", "-l", logfile], background=True)
            time.sleep(1.5)

            # ── Verifikasi ITGRecv jalan ─────────────────────────────────────
            recv_ready = False
            for _ in range(10):
                result = mnexec_run(h2_name, ["pgrep", "-f", "ITGRecv"], timeout=3)
                if result and result.stdout.strip():
                    info(f"*** ITGRecv ready\n")
                    recv_ready = True
                    break
                time.sleep(0.5)

            if not recv_ready:
                info("!!! ITGRecv failed to start — skipping session\n")
                time.sleep(RESTART_DELAY)
                continue

            # ── Parameter sesi ───────────────────────────────────────────────
            base_rate    = 50
            current_rate = max(30, base_rate + random.randint(-10, 15))
            packet_size  = random.randint(140, 200)
            duration     = STEADY_DURATION_MS + random.randint(-5000, 5000)

            # Silence simulasi
            if random.random() < 0.25:
                silence = random.uniform(0.5, 2.0)
                info(f"*** Simulating silence {silence:.2f}s\n")
                time.sleep(silence)

            # Micro-burst sesekali (blocking 3s, tidak perlu lock)
            if random.random() < 0.15:
                burst_rate = random.randint(80, 120)
                info(f"*** Micro burst at {burst_rate} pps\n")
                mnexec_run(
                    h1_name,
                    ["ITGSend", "-T", "UDP", "-a", dst_ip,
                     "-rp", "9000", "-c", str(packet_size),
                     "-C", str(burst_rate), "-t", "3000", "-l", "/dev/null"],
                    timeout=6
                )

            info(f"*** Starting VoIP: {current_rate} pps | {packet_size} B\n")

            tcp_rate     = 260 + random.randint(-20, 20)
            tcp_pkt_size = 1200
            tcp_duration = STEADY_DURATION_MS

            # ── Launch ITGSend (background) ──────────────────────────────────
            mnexec_run(
                h1_name,
                ["ITGSend", "-T", "UDP", "-a", dst_ip,
                 "-rp", "9000", "-c", str(packet_size),
                 "-C", str(current_rate), "-t", str(duration),
                 "-l", "/dev/null"],
                background=True
            )

            # ── Launch iperf3 background TCP (background) ────────────────────
            mnexec_run(
                h1_name,
                ["iperf3", "-c", dst_ip, "-p", "9003",
                 "-b", str(tcp_rate * tcp_pkt_size * 8),
                 "-t", str(tcp_duration // 1000)],
                background=True
            )

            # ── Tunggu sesi selesai ──────────────────────────────────────────
            wait_sec = max(duration, tcp_duration) / 1000
            info(f"*** Waiting {wait_sec:.0f}s for session to complete\n")
            time.sleep(wait_sec)

            # ── Simpan ke DB ─────────────────────────────────────────────────
            save_itg_session_to_db(logfile)

            time.sleep(RESTART_DELAY)

        except Exception as e:
            info(f"*** [SESSION {i}] Error (retrying): {type(e).__name__}: {e}\n")
            time.sleep(RESTART_DELAY * 2)

# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    # STEP 0: bersihkan state lama sebelum apapun
    pre_cleanup()

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

    # STEP 1: tunggu semua switch benar-benar connected ke controller
    info("*** Waiting for switches to connect...\n")
    net.waitConnected(timeout=30)
    info("*** All switches connected\n")

    time.sleep(3)
    net.pingAll()
    time.sleep(2)

    # STEP 2: start iperf3 server di h2 via host.cmd() — AMAN karena di main thread
    h2.cmd("iperf3 -s -p 9001 -D")
    h2.cmd("iperf3 -s -p 9003 -D")
    time.sleep(2)

    # STEP 3: invalidate PID cache dulu (host baru start)
    invalidate_pid_cache()
    time.sleep(1)

    # STEP 4: start watchdog — pakai mnexec, thread-safe
    wd = threading.Thread(
        target=iperf3_watchdog,
        args=("h2",),
        daemon=True
    )
    wd.start()

    # STEP 5: start traffic thread jika D-ITG tersedia
    ditg_ok = subprocess.run(
        ['which', 'ITGSend'], capture_output=True
    ).returncode == 0

    if ditg_ok:
        time.sleep(2)  # beri waktu tambahan sebelum traffic mulai
        info("*** Starting STEADY VoIP thread (h1 -> h2)\n")
        t = threading.Thread(
            target=keep_steady_traffic,
            args=("h1", "h2", h2.IP()),
            daemon=True,
        )
        t.start()
    else:
        info("!!! D-ITG not installed, VoIP traffic disabled\n")

    # STEP 6: CLI — main thread, tidak terganggu apapun
    info("*** Running Mininet CLI\n")
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    run()