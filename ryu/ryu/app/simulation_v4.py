#!/usr/bin/python3
"""
Simulasi Data Center + VoIP di Mininet (FULL VERSION)
- Iperf untuk layanan (web/api/db/cache)
- SIPp untuk VoIP (SIP/UAC + uas)
- Routing Gateway VoIP (192.168.10.254)
- Namespace Linking untuk Collector
"""
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
import threading
import random
import time
import subprocess
import math
import signal
import sys
import os

# ---------------------- GLOBAL LOCK & STOP EVENT ----------------------
cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd, timeout=15):
    """Execute node.cmd() safely with lock. Returns output or None."""
    if stop_event.is_set():
        return None
    with cmd_lock:
        if stop_event.is_set():
            return None
        try:
            return node.cmd(cmd, timeout=timeout)
        except Exception:
            return None

# ---------------------- ROUTER & TOPOLOGI ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        if not stop_event.is_set():
            safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class DataCenterTopo(Topo):
    def build(self):
        info("*** Membangun Topologi Data Center...\n")
        
        # Core Router (L3 Boundary)
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        
        # Switch Eksternal (Tempat user terhubung)
        ext_sw = self.addSwitch('ext_sw', dpid='1') 
        
        # Switch Internal DC (Top-of-Rack)
        tor1 = self.addSwitch('tor1', dpid='2')
        tor2 = self.addSwitch('tor2', dpid='3') 
        
        # --- Klien Eksternal (User Biasa) ---
        user1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        user2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        user3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        
        # --- Klien VoIP (Gateway 10.254) ---
        voip1 = self.addHost('voip1', ip='192.168.10.11/24', mac='00:00:00:00:10:11', defaultRoute='via 192.168.10.254')
        voip2 = self.addHost('voip2', ip='192.168.10.12/24', mac='00:00:00:00:10:12', defaultRoute='via 192.168.10.254')
        
        # --- Server Internal DC ---
        # Rack 1 (Web & Cache Tier)
        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')
        
        # Rack 2 (App & DB Tier)
        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        # Links Klien Eksternal
        self.addLink(user1, ext_sw, bw=10)
        self.addLink(user2, ext_sw, bw=10)
        self.addLink(user3, ext_sw, bw=10)
        
        # Links VoIP
        self.addLink(voip1, ext_sw, bw=10)
        self.addLink(voip2, ext_sw, bw=10)
        
        # Links Server Internal
        self.addLink(web1, tor1, bw=20)
        self.addLink(web2, tor1, bw=20)
        self.addLink(cache1, tor1, bw=20)
        self.addLink(app1, tor2, bw=20)
        self.addLink(db1, tor2, bw=20)
        
        # Link Router
        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ---------------------- LOG PARSER ----------------------
def _log_iperf(client_name, server_ip, output, burst_time_str, bw_str):
    if not output: return
    try:
        lines = output.strip().split('\n')
        csv_line = None
        for line in reversed(lines): 
            if ',' in line and len(line.split(',')) > 7: 
                csv_line = line
                break 
        if csv_line:
            parts = csv_line.split(',')
            actual_bytes = int(parts[7])
            info(f"TRAFFIC: {client_name} -> {server_ip} SENT {actual_bytes:,} bytes (Target: {bw_str})\n")
    except Exception:
        pass

# ---------------------- DAILY PATTERN GENERATOR ----------------------
def daily_pattern(hour, base_min, base_max, seed_val=0, category='data'):
    phase = (seed_val % 10) * 0.02
    if category == 'web': offset = -0.5; amplitude = 1.0
    elif category == 'api': offset = -0.3; amplitude = 0.8
    elif category == 'voip': offset = -0.2; amplitude = 0.9
    elif category == 'east_west': offset = -0.1; amplitude = 0.45
    elif category == 'db': offset = -0.25; amplitude = 0.6
    else: offset = -0.3; amplitude = 0.6

    sine_val = 0.5 * (1 + math.sin(((hour / 24.0) * 2 * math.pi) - (math.pi/2) + phase + offset))
    baseline = 0.15 * base_max
    scaled = baseline + (sine_val * (base_max - baseline) * amplitude)
    jitter = random.uniform(0.90, 1.10)
    return max(base_min, min(base_max, scaled * jitter))

# ---------------------- TRAFFIC GENERATORS ----------------------
def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed, category='data'):
    rng = random.Random(seed)
    info(f"Start Thread: {client.name} -> {server_ip}:{port} ({category})\n")
    while not stop_event.is_set():
        try:
            hour = int(time.strftime("%H"))
            target_bw = daily_pattern(hour, base_min_bw, base_max_bw, seed, category)
            bw_str = f"{max(0.01, target_bw):.2f}M"
            
            # Durasi burst & pause
            if category == 'web': burst = rng.uniform(1.0, 3.0)
            elif category == 'east_west': burst = rng.uniform(0.5, 1.5)
            elif category == 'db': burst = rng.uniform(0.6, 2.5)
            else: burst = rng.uniform(0.5, 2.0)
            
            burst_str = f"{burst:.1f}"
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {burst_str} -y C"
            output = safe_cmd(client, cmd)
            _log_iperf(client.name, server_ip, output, burst_str, bw_str)

            if stop_event.is_set(): break
            pause = rng.uniform(0.5, 2.0)
            stop_event.wait(pause)
        except Exception:
            if stop_event.is_set(): break
            stop_event.wait(1)

def generate_voip_calls(client, server_ip, seed, category='voip'):
    rng = random.Random(seed)
    info(f"Start VoIP Thread: {client.name} -> {server_ip}\n")
    while not stop_event.is_set():
        hour = int(time.strftime("%H"))
        dur = int(max(2, min(120, daily_pattern(hour, 4, 40, seed, 'voip'))))
        
        # SIPp UAC mode without trace logging to save disk space
        cmd = f"sipp {server_ip} -sn uac -s 1000 -d {dur*1000} -nd -i {client.IP()}"
        safe_cmd(client, cmd)

        next_call_delay = rng.uniform(2, 10)
        stop_event.wait(next_call_delay)

# ---------------------- START SIMULATION ----------------------
def start_traffic(net):
    user1, user2, user3 = net.get('user1', 'user2', 'user3')
    voip1, voip2 = net.get('voip1', 'voip2')
    web1, web2, cache1 = net.get('web1', 'web2', 'cache1')
    app1, db1 = net.get('app1', 'db1')
    cr1 = net.get('cr1')

    # 1. Konfigurasi Gateway Tambahan (Untuk VoIP 192.168.10.x)
    info("\n*** Config VoIP Gateway on Router...\n")
    safe_cmd(cr1, "ip addr add 192.168.10.254/24 dev cr1-eth1")
    
    # 2. Link Namespaces (CRITICAL for Collector)
    info("\n*** Linking Namespaces for Collector...\n")
    subprocess.run("sudo mkdir -p /var/run/netns", shell=True)
    all_hosts = [user1, user2, user3, voip1, voip2, web1, web2, cache1, app1, db1]
    
    for host in all_hosts:
        if not host: continue
        try:
            pid = host.pid
            # Hapus link lama jika ada biar bersih
            subprocess.run(['sudo', 'rm', '-f', f'/var/run/netns/{host.name}'], check=False)
            cmd = ['sudo', 'ip', 'netns', 'attach', host.name, str(pid)]
            subprocess.run(cmd, check=True)
        except Exception:
            pass

    # 3. Start Servers
    info("\n*** Starting Services (Iperf & SIPp)...\n")
    # Iperf Servers
    safe_cmd(web1, "iperf -s -u -p 443 -i 1 &")
    safe_cmd(web2, "iperf -s -u -p 80 -i 1 &")
    safe_cmd(app1, "iperf -s -u -p 8080 -i 1 &")
    safe_cmd(cache1, "iperf -s -u -p 6379 -i 1 &")
    safe_cmd(db1, "iperf -s -u -p 5432 -i 1 &")
    
    # SIPp Servers (VoIP Receivers)
    safe_cmd(app1, "sipp -sn uas -i 10.10.2.1 -p 5060 -bg &")
    safe_cmd(web1, "sipp -sn uas -i 10.10.1.1 -p 5060 -bg &")

    # 4. Wait & Ping
    info("\n*** Waiting for connectivity...\n")
    net.waitConnected()
    # PingAll singkat untuk ARP resolution
    net.pingAll(timeout='0.5')

    # 5. Start Threads
    info("\n*** Generating Traffic...\n")
    threads = []
    
    # User Traffic
    threads.append(threading.Thread(target=generate_client_traffic, args=(user1, '10.10.1.1', 443, 1.5, 8.0, 1, 'web')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(user2, '10.10.2.1', 8080, 0.8, 4.0, 2, 'api')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(user3, '10.10.1.2', 80, 0.3, 2.0, 3, 'web')))

    # East-West Traffic (YANG DULU HILANG)
    threads.append(threading.Thread(target=generate_client_traffic, args=(web1, '10.10.2.1', 8080, 0.5, 3.0, 4, 'east_west')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(web2, '10.10.1.3', 6379, 0.2, 2.5, 5, 'cache')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(app1, '10.10.2.2', 5432, 0.8, 6.0, 6, 'db')))

    # VoIP Traffic
    threads.append(threading.Thread(target=generate_voip_calls, args=(voip1, '10.10.1.1', 7)))
    threads.append(threading.Thread(target=generate_voip_calls, args=(voip2, '10.10.2.1', 8)))

    for t in threads: t.start()
    return threads

# ---------------------- CLEANUP ----------------------
def cleanup_all(net, host_names):
    info("*** Cleaning up...\n")
    for name in host_names:
        try:
            h = net.get(name)
            if h:
                h.cmd("pkill -f iperf")
                h.cmd("pkill -f sipp")
        except: pass
    
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True)

if __name__ == "__main__":
    setLogLevel("info")
    topo = DataCenterTopo()
    net = Mininet(topo=topo, switch=OVSSwitch, controller=lambda name: RemoteController(name, ip="127.0.0.1"), link=TCLink)
    
    info("*** Starting Mininet...\n")
    net.start()
    
    threads = start_traffic(net)
    
    info("*** Simulation Running (Ctrl+C to stop)...\n")
    
    def signal_handler(sig, frame):
        stop_event.set()
    signal.signal(signal.SIGINT, signal_handler)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        cleanup_all(net, ['user1', 'user2', 'user3', 'voip1', 'voip2', 'web1', 'web2', 'cache1', 'app1', 'db1'])
        net.stop()