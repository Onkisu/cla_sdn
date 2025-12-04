#!/usr/bin/python3
"""
Simulasi Data Center + VoIP di Mininet (FULL VERSION - FIXED VOIP TRAFFIC)
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
    if stop_event.is_set(): return None
    with cmd_lock:
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
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        ext_sw = self.addSwitch('ext_sw', dpid='1') 
        tor1 = self.addSwitch('tor1', dpid='2')
        tor2 = self.addSwitch('tor2', dpid='3') 
        
        user1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        user2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        user3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        
        voip1 = self.addHost('voip1', ip='192.168.10.11/24', mac='00:00:00:00:10:11', defaultRoute='via 192.168.10.254')
        voip2 = self.addHost('voip2', ip='192.168.10.12/24', mac='00:00:00:00:10:12', defaultRoute='via 192.168.10.254')
        
        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')
        
        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        for h in [user1, user2, user3, voip1, voip2]: self.addLink(h, ext_sw, bw=10)
        for h in [web1, web2, cache1]: self.addLink(h, tor1, bw=20)
        for h in [app1, db1]: self.addLink(h, tor2, bw=20)
        
        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ---------------------- UTILS ----------------------
def daily_pattern(hour, base_min, base_max, seed_val=0, category='data'):
    phase = (seed_val % 10) * 0.02
    sine_val = 0.5 * (1 + math.sin(((hour / 24.0) * 2 * math.pi) - (math.pi/2) + phase))
    baseline = 0.15 * base_max
    return max(base_min, min(base_max, baseline + (sine_val * (base_max - baseline))))

def generate_client_traffic(client, server_ip, port, base_min_bw, base_max_bw, seed, category='data'):
    rng = random.Random(seed)
    info(f"Start: {client.name} -> {server_ip}:{port} ({category})\n")
    while not stop_event.is_set():
        try:
            hour = int(time.strftime("%H"))
            target_bw = daily_pattern(hour, base_min_bw, base_max_bw, seed, category)
            bw_str = f"{max(0.01, target_bw):.2f}M"
            dur = rng.uniform(1.0, 3.0)
            
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {dur:.1f}"
            safe_cmd(client, cmd)
            stop_event.wait(rng.uniform(0.5, 2.0))
        except: break

# ---------------------- FIXED VOIP GENERATOR ----------------------
def generate_voip_calls(client, server_ip, seed, category='voip'):
    rng = random.Random(seed)
    info(f"Start VoIP (SIP+RTP): {client.name} -> {server_ip}\n")
    
    # Port untuk simulasi RTP (Audio stream)
    rtp_port = 6000 + int(client.name[-1]) # voip1->6001, voip2->6002

    while not stop_event.is_set():
        hour = int(time.strftime("%H"))
        call_duration = int(max(5, min(60, daily_pattern(hour, 10, 60, seed, 'voip'))))
        
        # 1. SIP Signaling (Ringan)
        sip_cmd = f"sipp {server_ip} -sn uac -s 1000 -d 1000 -nd -i {client.IP()} -r 1 -rp 1000"
        
        # 2. RTP Simulation (Audio Stream - UDP 64kbps) - WAJIB AGAR TRAFFIC TEREKAM
        # Kita pakai Iperf UDP bitrate rendah konstan (Codec G.711)
        rtp_cmd = f"iperf -u -c {server_ip} -p {rtp_port} -b 64k -t {call_duration}"

        # Jalankan RTP stream (Blocking sampai call selesai)
        # Note: Kita jalankan SIP di background (karena simulation), dan RTP di foreground
        safe_cmd(client, f"{sip_cmd} > /dev/null 2>&1 &")
        safe_cmd(client, rtp_cmd)
        
        # Idle time antar panggilan
        stop_event.wait(rng.uniform(2, 5))

# ---------------------- START ----------------------
def start_traffic(net):
    user1, user2, user3 = net.get('user1', 'user2', 'user3')
    voip1, voip2 = net.get('voip1', 'voip2')
    web1, web2, cache1 = net.get('web1', 'web2', 'cache1')
    app1, db1 = net.get('app1', 'db1')
    cr1 = net.get('cr1')

    info("\n*** Config VoIP Gateway...\n")
    safe_cmd(cr1, "ip addr add 192.168.10.254/24 dev cr1-eth1")
    
    info("\n*** Link Netns...\n")
    subprocess.run("sudo mkdir -p /var/run/netns", shell=True)
    all_hosts = [user1, user2, user3, voip1, voip2, web1, web2, cache1, app1, db1]
    for host in all_hosts:
        try:
            subprocess.run(f"sudo rm -f /var/run/netns/{host.name}", shell=True)
            subprocess.run(f"sudo ip netns attach {host.name} {host.pid}", shell=True)
        except: pass

    info("\n*** Start Services...\n")
    for h in [web1, web2, app1, db1, cache1]:
        # Listener untuk Traffic Data
        ports = [80, 443, 8080, 5432, 6379, 5001]
        for p in ports: h.cmd(f"iperf -s -u -p {p} &")
        
        # Listener untuk VoIP RTP (6001-6005)
        for p in range(6000, 6010): h.cmd(f"iperf -s -u -p {p} &")
        
        # Listener SIP
        h.cmd(f"sipp -sn uas -i {h.IP()} -p 5060 -bg &")

    net.waitConnected()
    net.pingAll(timeout='0.5')

    threads = []
    # Data Traffic
    threads.append(threading.Thread(target=generate_client_traffic, args=(user1, '10.10.1.1', 443, 1, 5, 1, 'web')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(user2, '10.10.2.1', 8080, 0.5, 3, 2, 'api')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(user3, '10.10.1.2', 80, 0.2, 2, 3, 'web')))
    # East-West
    threads.append(threading.Thread(target=generate_client_traffic, args=(web1, '10.10.2.1', 8080, 0.5, 3, 4, 'east_west')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(web2, '10.10.1.3', 6379, 0.2, 2, 5, 'cache')))
    threads.append(threading.Thread(target=generate_client_traffic, args=(app1, '10.10.2.2', 5432, 0.8, 4, 6, 'db')))
    # VoIP Traffic (Fixed)
    threads.append(threading.Thread(target=generate_voip_calls, args=(voip1, '10.10.1.1', 7)))
    threads.append(threading.Thread(target=generate_voip_calls, args=(voip2, '10.10.2.1', 8)))

    for t in threads: t.start()
    return threads

def cleanup_all(net):
    subprocess.run("sudo pkill -f iperf", shell=True)
    subprocess.run("sudo pkill -f sipp", shell=True)
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True)

if __name__ == "__main__":
    setLogLevel("info")
    topo = DataCenterTopo()
    net = Mininet(topo=topo, switch=OVSSwitch, controller=lambda name: RemoteController(name, ip="127.0.0.1"), link=TCLink)
    net.start()
    try:
        ts = start_traffic(net)
        while not stop_event.is_set(): time.sleep(1)
    except KeyboardInterrupt: pass
    finally:
        stop_event.set()
        cleanup_all(net)
        net.stop()