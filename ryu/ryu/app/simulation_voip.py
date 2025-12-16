#!/usr/bin/python3
"""
Simulasi VoIP: Normal Traffic (Randomized) + Periodic Congestion (Burst)
FIXED: Robust Cleanup & Metadata Checking
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
# Pastikan shared_config ada di folder yang sama
try:
    from config_voip import HOST_INFO
except ImportError:
    print("Error: shared_config.py not found or invalid.")
    sys.exit(1)

cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd):
    if stop_event.is_set(): return None
    with cmd_lock:
        try: return node.cmd(cmd)
        except: return None

# ---------------------- TOPOLOGI ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class DataCenterTopo(Topo):
    def build(self):
        info("*** Topologi Data Center...\n")
        cr1 = self.addNode('cr1', cls=LinuxRouter, ip='192.168.100.254/24')
        ext_sw = self.addSwitch('ext_sw', dpid='1')
        tor1 = self.addSwitch('tor1', dpid='2')
        tor2 = self.addSwitch('tor2', dpid='3')
        
        user1 = self.addHost('user1', ip='192.168.100.1/24', mac='00:00:00:00:01:01', defaultRoute='via 192.168.100.254')
        user2 = self.addHost('user2', ip='192.168.100.2/24', mac='00:00:00:00:01:02', defaultRoute='via 192.168.100.254')
        user3 = self.addHost('user3', ip='192.168.100.3/24', mac='00:00:00:00:01:03', defaultRoute='via 192.168.100.254')
        
        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')
        
        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        self.addLink(user1, ext_sw, bw=20); self.addLink(user2, ext_sw, bw=20); self.addLink(user3, ext_sw, bw=20)
        self.addLink(web1, tor1, bw=50); self.addLink(web2, tor1, bw=50); self.addLink(cache1, tor1, bw=50)
        self.addLink(app1, tor2, bw=50); self.addLink(db1, tor2, bw=50)
        
        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ---------------------- PATTERN GENERATOR ----------------------
def daily_pattern(hour, base_min, base_max, seed_val=0):
    jitter_phase = (seed_val % 10) * 0.02
    sine_val = 0.5 * (1 + math.sin(((hour / 24.0) * 2 * math.pi) - (math.pi/2) + jitter_phase))
    baseline = 0.2 * base_max
    scaled = baseline + (sine_val * (base_max - baseline))
    jitter = random.uniform(0.7, 1.3) 
    return max(base_min, min(base_max, scaled * jitter))

# ---------------------- TRAFFIC LOGIC ----------------------

def run_normal_voip(client, target_ip, target_port, seed):
    rng = random.Random(seed)
    info(f"[{client.name}] Normal VoIP loop started -> {target_ip}\n")
    
    while not stop_event.is_set():
        try:
            hour = int(time.strftime("%H"))
            # Call duration: 5s to 30s
            duration_ms = int(daily_pattern(hour, 5000, 30000, seed))
            
            # G.711 Codec Simulation
            cmd = f"ITGSend -T UDP -a {target_ip} -rp {target_port} -C 50 -c 160 -t {duration_ms} &"
            safe_cmd(client, cmd)
            
            wait_time = (duration_ms / 1000.0) + rng.uniform(1.0, 5.0)
            stop_event.wait(wait_time)
        except Exception:
            stop_event.wait(1)

def congestion_controller(net, victims):
    info("\n*** Starting Periodic Congestion Controller (30s Cycle) ***\n")
    while not stop_event.is_set():
        # PHASE 1: CALM (25 Detik)
        stop_event.wait(25)
        if stop_event.is_set(): break

        # PHASE 2: CONGESTION (5 Detik)
        info("\n!!! WARNING: INITIATING CONGESTION BURST (5s) !!!\n")
        
        for v_name in victims:
            node = net.get(v_name)
            meta = HOST_INFO.get(v_name)
            if not meta or 'dst_ip' not in meta: continue # Safety check
            
            target_ip = meta['dst_ip']
            target_port = meta.get('tp_dst', 10000)
            
            # Burst: 5000 pps, 1024 bytes
            cmd = f"ITGSend -T UDP -a {target_ip} -rp {target_port} -C 5000 -c 1024 -t 5000"
            threading.Thread(target=safe_cmd, args=(node, cmd)).start()
            
        time.sleep(5)
        info("--- Congestion Ended. Cooling down. ---\n")

# ---------------------- MAIN ----------------------
def start_traffic(net):
    # Setup Namespace
    info("\n*** Setup Namespaces...\n")
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    for h in net.hosts:
        if h.name in HOST_INFO:
            subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

    # Start ITGRecv
    info("*** Starting ITGRecv...\n")
    receivers = set()
    for h_name, meta in HOST_INFO.items():
        # Safety Check: Pastikan dst_ip ada
        if 'dst_ip' not in meta:
            print(f"[WARNING] Host {h_name} skipped: Missing 'dst_ip' in config.")
            continue
            
        dst_ip = meta['dst_ip']
        for h in net.hosts:
            if h.IP() == dst_ip: receivers.add(h)
            
    for h in receivers:
        safe_cmd(h, "ITGRecv &")
    time.sleep(2)

    threads = []
    i = 0
    victim_list = []
    
    # Start Traffic Threads
    for h_name, meta in HOST_INFO.items():
        # Safety Check lagi
        if 'dst_ip' not in meta: continue

        src_node = net.get(h_name)
        dst_ip = meta['dst_ip']
        dst_port = meta.get('tp_dst', 5060)
        
        t = threading.Thread(target=run_normal_voip, 
                             args=(src_node, dst_ip, dst_port, 1000+i),
                             daemon=False)
        threads.append(t)
        
        if h_name in ['user1', 'user2', 'web1']: 
            victim_list.append(h_name)
        i += 1

    # Start Congestion Controller
    ct = threading.Thread(target=congestion_controller, args=(net, victim_list), daemon=False)
    threads.append(ct)

    for t in threads: t.start()
    return threads

def cleanup(net):
    info("*** Cleaning up...\n")
    # Kill process
    subprocess.run("sudo killall -9 ITGSend ITGRecv 2>/dev/null", shell=True)
    
    # PENTING: Umount dulu sebelum remove untuk hindari 'Device busy'
    subprocess.run("sudo umount /var/run/netns/* 2>/dev/null", shell=True)
    
    # Hapus files
    subprocess.run("sudo rm -rf /var/run/netns/*", shell=True)

if __name__ == "__main__":
    setLogLevel("info")
    topo = DataCenterTopo()
    net = Mininet(topo=topo, switch=OVSSwitch, controller=lambda n: RemoteController(n, ip="127.0.0.1"), link=TCLink)
    
    try:
        net.start()
        net.pingAll()
        threads = start_traffic(net)
        
        info("\n*** Simulation Running. Press Ctrl+C to stop.\n")
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        cleanup(net)
        net.stop()