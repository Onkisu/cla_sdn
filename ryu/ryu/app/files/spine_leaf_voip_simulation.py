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
RESTART_DELAY = 1         # detik

def check_ditg():
    return subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0

import subprocess
import psycopg2
import re

def save_itg_session_to_db_from_text(result):
    data = {
        "duration": 0,
        "total_packets": 0,
        "dropped": 0,
        "avg_delay": 0,
        "avg_jitter": 0,
        "max_delay": 0,
        "bitrate": 0,
        "pps": 0
    }

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

    data["loss"] = data["dropped"] / data["total_packets"] * 100 if data["total_packets"] > 0 else 0
    # (bagian insert DB lu BIARIN SAMA)


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

def itgsend_alive(host):
    return bool(host.cmd("pgrep -f 'ITGSend.*-rp 9000'").strip())


# def keep_steady_traffic(src_host, dst_host, dst_ip):
#     for i in itertools.count(1):
#         try:
#             if not hasattr(dst_host, 'shell') or dst_host.shell is None:
#                 info("*** Host disconnected\n")
#                 break


#             session_ts = int(time.time())
#             logfile = f"/tmp/recv_steady.log"
#             logfile_burst = f"/tmp/recv_burst.log"

#             info(f"*** [SESSION {i}] Restarting ITGRecv -> {logfile}\n")

#             # Kill existing ITGRecv (non-blocking)# Cek apakah ITGRecv masih hidup
#             check = dst_host.popen("pgrep ITGRecv", shell=True, stdout=subprocess.PIPE)
#             out = check.stdout.read().decode().strip()

#             if not out:
#                 info("*** ITGRecv not running, restarting...\n")
#                 dst_host.popen(f"ITGRecv -Sp 9000 -l {logfile} &", shell=True)
#                 dst_host.popen(f"ITGRecv -Sp 9001 -l {logfile_burst} &", shell=True)
#                 time.sleep(1)
#             else:
#                 info("*** ITGRecv still running\n")

#             dst_host.cmd(f"> {logfile}")  
#             dst_host.cmd(f"> {logfile_burst}")   
#             # Blocking send
#             p = src_host.popen(
#                 f'ITGSend -T UDP -a {dst_ip} '
#                 f'-rp 9000 '
#                 f'-c {PKT_SIZE} -C {STEADY_RATE} '
#                 f'-t {STEADY_DURATION_MS} -l /dev/null',
#                 shell=True
#             )
#             p.wait()

#             try:
#                 dst_host.cmd(f"cp {logfile} /tmp/tmp.log")
#                 result = dst_host.cmd("ITGDec /tmp/tmp.log")
#                 save_itg_session_to_db_from_text(result)
#             except Exception as e:
#                 info(f"!!! DB SAVE FAILED: {e}\n")

#             time.sleep(RESTART_DELAY)

#         except Exception as e:
#             info(f"*** Traffic loop error: {e}\n")
#             break

def keep_steady_traffic(src_host, dst_host, dst_ip):
    logfile = "/tmp/recv_steady.log"
    logfile_burst = "/tmp/recv_burst.log"

    while True:
        try:
            # === PASTIKAN ITGRecv HIDUP ===
            if not dst_host.cmd("pgrep ITGRecv").strip():
                info("*** ITGRecv DEAD → starting again\n")
                dst_host.cmd(f"ITGRecv -Sp 9000 -l {logfile} &")
                dst_host.cmd(f"ITGRecv -Sp 9001 -l {logfile_burst} &")
                time.sleep(1)

            # === CEK STEADY ITGSEND ===
            if not itgsend_alive(src_host):
                info("*** STEADY DEAD → starting ITGSend\n")
                src_host.cmd(
                    f"ITGSend -T UDP -a {dst_ip} "
                    f"-rp 9000 -c {PKT_SIZE} -C {STEADY_RATE} "
                    f"-t 0 -l /dev/null &"
                )
            else:
                info("*** STEADY running\n")

            time.sleep(2)

        except Exception as e:
            info(f"!!! Steady watchdog error: {e}\n")
            time.sleep(2)

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
        h2.cmd('ITGRecv -Sp 9000 -l /tmp/recv_steady.log &')
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