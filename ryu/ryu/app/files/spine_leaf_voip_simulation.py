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
import subprocess
import psycopg2
import re

STEADY_DURATION_MS = 60000   # 60 detik per sesi
STEADY_RATE = 50             # pps
PKT_SIZE = 160               # bytes
RESTART_DELAY = 1            # detik

def check_ditg():
    return subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0

import subprocess
import psycopg2
import re

def save_itg_session_to_db(log_file):
    # Jalankan ITGDec
    result = subprocess.check_output(
        ["ITGDec", log_file],
        text=True
    )

    data = {}

    for line in result.splitlines():
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
            if not hasattr(dst_host, 'shell') or dst_host.shell is None:
                time.sleep(1)
                continue


            session_ts = int(time.time())
            logfile = f"/tmp/recv_steady_{session_ts}.log"
            logfile_burst = f"/tmp/recv_burst_{session_ts}.log"

            info(f"*** [SESSION {i}] Restarting ITGRecv -> {logfile}\n")

            # Kill old receiver
            dst_host.cmd("pkill -9 ITGRecv 2>/dev/null")
            time.sleep(0.5)

            # Start fresh receiver (9000 steady, 9001 burst)
            dst_host.popen(f"ITGRecv -Sp 9000 -l {logfile} &", shell=True)
            dst_host.popen(f"ITGRecv -Sp 9001 -l {logfile_burst} &", shell=True)
            time.sleep(1)

            info(f"*** [SESSION {i}] Starting ITGSend (STEADY)\n")

            # Blocking send
            p = src_host.popen(
                f'ITGSend -T UDP -a {dst_ip} '
                f'-rp 9000 '
                f'-c {PKT_SIZE} -C {STEADY_RATE} '
                f'-t {STEADY_DURATION_MS} -l /dev/null',
                shell=True
            )
            p.wait()

            try:
                save_itg_session_to_db(logfile)
            except Exception as e:
                info(f"!!! DB SAVE FAILED: {e}\n")

            time.sleep(RESTART_DELAY)

        except Exception as e:
            info(f"*** Traffic loop error: {e}\n")
            break


# def keep_steady_traffic(src_host, dst_host, dst_ip):
#     for i in itertools.count(1):
#         try:
#             # Check if hosts still alive
#             if not hasattr(dst_host, 'shell') or dst_host.shell is None:
#                 info("*** Host disconnected\n")
#                 break
                
#             session_ts = int(time.time())
#             logfile = f"/tmp/recv_steady_{session_ts}.log"

#             info(f"*** [SESSION {i}] Starting ITGRecv -> {logfile}\n")

#             # Kill existing ITGRecv (non-blocking)
#             try:
#                 dst_host.cmd("pkill -9 ITGRecv 2>/dev/null")
#             except:
#                 pass
#             time.sleep(0.5)
            
#             # Start ITGRecv
#             dst_host.cmd(f"ITGRecv -Sp 9000 -l {logfile} &")
#             dst_host.cmd(f"ITGRecv -Sp 9001 -l {logfile} &")
#             time.sleep(1)

#             info("*** Starting ITGSend (STEADY)\n")
#             src_host.cmd(
#                 f'ITGSend -T UDP -a {dst_ip} '
#                 f'-rp 9000 '  # Pastikan port tujuan sesuai dengan yang di-listen ITGRecv
#                 f'-c {PKT_SIZE} -C {STEADY_RATE} '
#                 f'-t {STEADY_DURATION_MS} -l /dev/null'
#             )

#             try:
#                 save_itg_session_to_db(logfile)
#             except Exception as e:
#                 info(f"!!! DB SAVE FAILED: {e}\n")

#             time.sleep(RESTART_DELAY)
            
#         except Exception as e:
#             info(f"*** Traffic loop error: {e}\n")
#             break


def run():
    info("*** Starting Spine-Leaf Topology (STEADY ONLY + WATCHDOG)\n")

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
            #net.addLink(l, s, bw=1000, delay='1ms')
            net.addLink(l, s, bw=0.55, delay='1ms', max_queue_size=5, use_htb=True)

    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')

    net.addLink(h1, leaves[0], bw=100, delay='1ms')
    net.addLink(h2, leaves[1], bw=100, delay='1ms')
    net.addLink(h3, leaves[2], bw=100, delay='1ms')

    net.start()
    time.sleep(3)
    net.pingAll()

    if check_ditg():
        # Kita tidak perlu start ITGRecv manual di sini lagi, 
        # karena Watchdog sekarang cukup pintar untuk menyalakannya jika belum ada.
        # Tapi untuk inisiasi awal yang cepat, kita nyalakan sekali.
        info("*** Starting ITGRecv on h2 (Initial)\n")
        h2.cmd('ITGRecv -Sp 9000 -l /tmp/recv_voip.log &')
        h2.cmd('ITGRecv -Sp 9001 -l /tmp/recv_burst.log &')
        time.sleep(1)

        info("*** Starting STEADY VoIP Watchdog (h1 -> h2)\n")
        # FIX: Pass h2 object juga ke argumen thread
        t = threading.Thread(
            target=keep_steady_traffic,
            args=(h1, h2, h2.IP()) 
        )
        t.daemon = True
        t.start()
    else:
        info("!!! D-ITG not installed, traffic disabled\n")

    info("*** Running Mininet CLI\n")
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()