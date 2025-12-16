#!/usr/bin/python3
"""
Simulasi Data Center dengan Traffic VoIP Only menggunakan D-ITG
- Menggunakan ITGSend & ITGRecv
- Pola trafik randomized berdasarkan jam harian
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
from config_voip import HOST_INFO

# ---------------------- GLOBAL LOCK & STOP EVENT ----------------------
cmd_lock = threading.Lock()
stop_event = threading.Event()

def safe_cmd(node, cmd):
    if stop_event.is_set(): return None
    with cmd_lock:
        try:
            return node.cmd(cmd)
        except:
            return None

# ---------------------- ROUTER & TOPOLOGI (SAMA SEPERTI SEBELUMNYA) ----------------------
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        safe_cmd(self, "sysctl -w net.ipv4.ip_forward=1")
    def terminate(self):
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
        
        web1 = self.addHost('web1', ip='10.10.1.1/24', mac='00:00:00:00:0A:01', defaultRoute='via 10.10.1.254')
        web2 = self.addHost('web2', ip='10.10.1.2/24', mac='00:00:00:00:0A:02', defaultRoute='via 10.10.1.254')
        cache1 = self.addHost('cache1', ip='10.10.1.3/24', mac='00:00:00:00:0A:03', defaultRoute='via 10.10.1.254')
        
        app1 = self.addHost('app1', ip='10.10.2.1/24', mac='00:00:00:00:0B:01', defaultRoute='via 10.10.2.254')
        db1 = self.addHost('db1', ip='10.10.2.2/24', mac='00:00:00:00:0B:02', defaultRoute='via 10.10.2.254')

        self.addLink(user1, ext_sw, bw=10); self.addLink(user2, ext_sw, bw=10); self.addLink(user3, ext_sw, bw=10)
        self.addLink(web1, tor1, bw=20); self.addLink(web2, tor1, bw=20); self.addLink(cache1, tor1, bw=20)
        self.addLink(app1, tor2, bw=20); self.addLink(db1, tor2, bw=20)
        
        self.addLink(cr1, ext_sw, intfName1='cr1-eth1', params1={'ip': '192.168.100.254/24'})
        self.addLink(cr1, tor1, intfName1='cr1-eth2', params1={'ip': '10.10.1.254/24'})
        self.addLink(cr1, tor2, intfName1='cr1-eth3', params1={'ip': '10.10.2.254/24'})

# ---------------------- DAILY PATTERN GENERATOR ----------------------
def daily_pattern(hour, base_min, base_max, seed_val=0):
    jitter_phase = (seed_val % 10) * 0.02
    sine_val = 0.5 * (1 + math.sin(((hour / 24.0) * 2 * math.pi) - (math.pi/2) + jitter_phase))
    baseline = 0.2 * base_max
    scaled = baseline + (sine_val * (base_max - baseline))
    jitter = random.uniform(0.8, 1.2) # Jitter lebih besar untuk VoIP call duration
    value = max(base_min, min(base_max, scaled * jitter))
    return value

# ---------------------- D-ITG VOIP GENERATOR ----------------------
def generate_ditg_voip(client, target_ip, target_port, seed):
    """
    Mengirim trafik D-ITG mensimulasikan VoIP (G.711 / G.729).
    Karakteristik: UDP, Packet Size kecil (~160 bytes), Rate konstan (~50 pps).
    """
    rng = random.Random(seed)
    info(f"Start D-ITG VoIP: {client.name} -> {target_ip}:{target_port}\n")
    
    while not stop_event.is_set():
        try:
            hour = int(time.strftime("%H"))
            
            # Durasi panggilan (call duration) mengikuti pola harian
            # Jam sibuk = panggilan lebih lama
            duration_ms = int(daily_pattern(hour, 5000, 60000, seed)) # 5 detik s.d 60 detik
            
            # Parameter VoIP (Simulasi G.711)
            # -T UDP : Protokol UDP
            # -a : Destination IP
            # -rp : Destination Port
            # -C 50 : 50 packets per second
            # -c 160 : 160 bytes payload size
            # -t : duration in ms
            
            # Randomize start delay sedikit
            time.sleep(rng.uniform(0.5, 2.0))
            
            cmd = f"ITGSend -T UDP -a {target_ip} -rp {target_port} -C 50 -c 160 -t {duration_ms} &"
            safe_cmd(client, cmd)
            
            # Tunggu durasi panggilan + jeda antar panggilan
            wait_time = (duration_ms / 1000.0) + daily_pattern(hour, 2, 10, seed+1)
            stop_event.wait(wait_time)
            
        except Exception as e:
            info(f"Error in thread {client.name}: {e}\n")
            stop_event.wait(1)

# ---------------------- MAIN TRAFFIC CONTROL ----------------------
def start_traffic(net):
    # Setup Host Namespace Links untuk Collector
    info("\n*** Setup Network Namespace Link for Collector...\n")
    subprocess.run(['sudo', 'mkdir', '-p', '/var/run/netns'], check=False)
    for h in net.hosts:
        if h.name in HOST_INFO:
            subprocess.run(['sudo', 'ip', 'netns', 'attach', h.name, str(h.pid)], check=False)

    info("\n*** Starting ITGRecv (Receiver) on all targets...\n")
    # Setiap host yang menjadi 'dst_ip' di config harus menjalankan ITGRecv
    receivers = set()
    for h_name, meta in HOST_INFO.items():
        # Cari host object berdasarkan IP tujuan
        target_ip = meta['dst_ip']
        for h_obj in net.hosts:
            if h_obj.IP() == target_ip:
                receivers.add(h_obj)
    
    for h in receivers:
        # Jalankan ITGRecv di background
        safe_cmd(h, "ITGRecv &")
    
    time.sleep(2)
    info("*** Receivers ready.\n")

    threads = []
    # Mulai thread pengirim (Sender) berdasarkan HOST_INFO
    i = 0
    for h_name, meta in HOST_INFO.items():
        src_node = net.get(h_name)
        dst_ip = meta['dst_ip']
        dst_port = meta['tp_dst'] # Menggunakan port dari metadata
        
        t = threading.Thread(target=generate_ditg_voip, 
                             args=(src_node, dst_ip, dst_port, 1000 + i),
                             daemon=False)
        threads.append(t)
        i += 1

    for t in threads: t.start()
    return threads

def cleanup(net):
    info("*** Cleaning up D-ITG processes...\n")
    subprocess.run("sudo killall -9 ITGSend ITGRecv", shell=True)
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