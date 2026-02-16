#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import threading
import itertools
import subprocess
import psycopg2
import random

# CONFIG
STEADY_DURATION_MS = 60000 
RESTART_DELAY = 1
traffic_lock = threading.Lock()

def check_tools():
    ditg = subprocess.run(['which', 'ITGSend'], capture_output=True).returncode == 0
    iperf = subprocess.run(['which', 'iperf3'], capture_output=True).returncode == 0
    return ditg, iperf

def save_itg_session_to_db(log_file="/tmp/recv_steady.log"):
    # Fungsi ini tetap sama untuk VoIP stats
    try:
        result = subprocess.check_output(["ITGDec", log_file], text=True)
        data = {}
        in_target_flow = False
        for line in result.splitlines():
            if "TOTAL RESULTS" in line: break
            if line.strip().startswith("From "):
                src_ip = line.strip().split()[1].split(":")[0]
                in_target_flow = (src_ip == "10.0.0.1")
                continue
            if not in_target_flow: continue
            if "Total time" in line: data["duration"] = float(line.split("=")[1].split()[0])
            elif "Total packets" in line: data["total_packets"] = int(line.split("=")[1])
            elif "Packets dropped" in line: data["dropped"] = int(line.split("=")[1].split()[0])
            elif "Average delay" in line: data["avg_delay"] = float(line.split("=")[1].split()[0]) * 1000
            elif "Average jitter" in line: data["avg_jitter"] = float(line.split("=")[1].split()[0]) * 1000
            elif "Maximum delay" in line: data["max_delay"] = float(line.split("=")[1].split()[0]) * 1000
            elif "Average bitrate" in line: data["bitrate"] = float(line.split("=")[1].split()[0])
            elif "Average packet rate" in line: data["pps"] = float(line.split("=")[1].split()[0])

        if "total_packets" not in data: return
        data["loss"] = (data["dropped"] / data["total_packets"] * 100 if data["total_packets"] > 0 else 0)

        conn = psycopg2.connect(host="127.0.0.1", dbname="development", user="dev_one", password="hijack332.")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO traffic.itg_session_results (src_ip, dst_ip, duration_sec, total_packets, 
            packets_dropped, loss_percent, avg_delay_ms, avg_jitter_ms, max_delay_ms, avg_bitrate_kbps, avg_pps)
            VALUES ('10.0.0.1', '10.0.0.2', %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data["duration"], data["total_packets"], data["dropped"], data["loss"], 
              data["avg_delay"], data["avg_jitter"], data["max_delay"], data["bitrate"], data["pps"]))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        info(f"!!! DB SAVE ERROR: {e}\n")

def keep_steady_traffic(src_host, dst_host, dst_ip):
    for i in itertools.count(1):
        try:
            session_ts = int(time.time())
            logfile_voip = f"/tmp/recv_steady_{session_ts}.log"

            info(f"\n*** [SESSION {i}] Starting Traffic Generation\n")

            with traffic_lock:
                # 1. CLEANUP OLD PROCESSES
                src_host.cmd("pkill -9 ITGSend")
                src_host.cmd("pkill -9 iperf3")
                dst_host.cmd("pkill -9 ITGRecv")
                dst_host.cmd("pkill -9 iperf3")
                time.sleep(1)

                # 2. START RECEIVERS (H2)
                # D-ITG for VoIP (9000)
                dst_host.cmd(f"ITGRecv -Sp 9000 -l {logfile_voip} &")
                # Iperf3 for Background/Burst (9001 & 9003)
                dst_host.cmd("iperf3 -s -p 9001 -D")
                dst_host.cmd("iperf3 -s -p 9003 -D")
                time.sleep(1)

            # 3. CONFIGURE TRAFFIC PARAMS
            voip_rate = max(30, 50 + random.randint(-10, 15))
            voip_pkt_size = random.randint(140, 200)
            duration_s = int(STEADY_DURATION_MS / 1000)
            
            # 4. START SENDERS (H1)
            with traffic_lock:
                # VoIP via D-ITG (9000)
                src_host.cmd(f"ITGSend -T UDP -a {dst_ip} -rp 9000 -c {voip_pkt_size} -C {voip_rate} -t {STEADY_DURATION_MS} -l /dev/null &")
                
                # Background TCP via Iperf3 (9003)
                # -b 2M (misal limit 2Mbps), -t (durasi)
                tcp_bw = f"{random.randint(1, 3)}M"
                src_host.cmd(f"iperf3 -c {dst_ip} -p 9003 -b {tcp_bw} -t {duration_s} --no-delay &")
                
                info(f"*** VoIP: {voip_rate}pps | TCP 9003: {tcp_bw}\n")

            # 5. WAIT & SAVE
            time.sleep(duration_s + 2)
            save_itg_session_to_db(logfile_voip)
            time.sleep(RESTART_DELAY)

        except Exception as e:
            info(f"*** Traffic loop error: {e}\n")
            break

def run():
    info("*** Starting Spine-Leaf Topology (iperf3 + D-ITG)\n")
    net = Mininet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    net.addController('c0', ip='127.0.0.1', port=6653)

    spines = [net.addSwitch(f's{i}', dpid=f'{i:016x}', protocols='OpenFlow13') for i in range(1, 4)]
    leaves = [net.addSwitch(f'l{i}', dpid=f'{i+3:016x}', protocols='OpenFlow13') for i in range(1, 4)]

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
    time.sleep(2)
    net.pingAll()

    ditg_ok, iperf_ok = check_tools()
    if ditg_ok and iperf_ok:
        info("*** Starting Traffic Watchdog (h1 -> h2)\n")
        t = threading.Thread(target=keep_steady_traffic, args=(h1, h2, h2.IP()))
        t.daemon = True
        t.start()
    else:
        info(f"!!! Tools missing: D-ITG:{ditg_ok}, iperf3:{iperf_ok}\n")

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()