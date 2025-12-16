import sys
import os
import time
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel

# ==========================================
# 1. EMBEDDED SCRIPTS (SENDER & RECEIVER)
# ==========================================

# Script Sender (Disimpan sebagai string)
SENDER_SCRIPT = """
import socket
import time
import random
import struct
import sys

TARGET_IP = sys.argv[1] if len(sys.argv) > 1 else '10.0.0.2'
TARGET_PORT = 5050

# Config Payload
NORMAL_PAYLOAD_SIZE = 160   # VoIP Normal (G.711)
BURST_PAYLOAD_SIZE = 1400   # Congestion / Micro-burst

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"[*] Sending VoIP traffic to {TARGET_IP}:{TARGET_PORT}")
print("[*] Pattern: 30s Cycle (5s Burst, 25s Normal)")

try:
    start_time = time.time()
    seq_num = 0
    
    while True:
        current_time = time.time()
        elapsed = current_time - start_time
        cycle_position = elapsed % 30
        
        # Logika Burst: Detik 0-5
        if cycle_position < 5:
            mode = "BURST"
            payload = b'B' * BURST_PAYLOAD_SIZE
            sleep_time = 0.0005 # High Rate
        else:
            mode = "NORMAL"
            payload = b'N' * NORMAL_PAYLOAD_SIZE
            sleep_time = random.uniform(0.015, 0.025) # 15-25ms jitter

        # Header: Seq + Timestamp
        header = struct.pack('!I d', seq_num, current_time)
        data = header + payload
        
        sock.sendto(data, (TARGET_IP, TARGET_PORT))
        seq_num += 1
        
        if seq_num % 1000 == 0:
            print(f"Seq: {seq_num} | Mode: {mode}")
            
        time.sleep(sleep_time)

except KeyboardInterrupt:
    sock.close()
"""

# Script Receiver (Disimpan sebagai string)
RECEIVER_SCRIPT = """
import socket

BIND_IP = '0.0.0.0'
BIND_PORT = 5050

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((BIND_IP, BIND_PORT))

print(f"[*] Receiver Listening on {BIND_IP}:{BIND_PORT}")

try:
    while True:
        data, addr = sock.recvfrom(4096)
        # Sink (terima & buang)
except KeyboardInterrupt:
    sock.close()
"""

# ==========================================
# 2. TOPOLOGY DEFINITION (SPINE-LEAF)
# ==========================================

class SpineLeafTopo(Topo):
    def build(self):
        # Spines
        spine1 = self.addSwitch('s1', dpid='0000000000000001')
        spine2 = self.addSwitch('s2', dpid='0000000000000002')
        # Leafs
        leaf1 = self.addSwitch('s3', dpid='0000000000000003')
        leaf2 = self.addSwitch('s4', dpid='0000000000000004')
        # Hosts
        h1 = self.addHost('h1', ip='10.0.0.1', mac='00:00:00:00:00:01')
        h2 = self.addHost('h2', ip='10.0.0.2', mac='00:00:00:00:00:02')

        # Link Host -> Leaf (High BW)
        self.addLink(h1, leaf1, bw=100)
        self.addLink(h2, leaf2, bw=100)
        
        # Mesh Spine-Leaf (Redundant Links)
        self.addLink(leaf1, spine1, bw=20)
        self.addLink(leaf1, spine2, bw=20)
        self.addLink(leaf2, spine1, bw=20)
        self.addLink(leaf2, spine2, bw=20)

# ==========================================
# 3. MAIN RUNNER
# ==========================================

def create_scripts():
    """Membuat file python sementara dari string di atas"""
    with open("temp_sender.py", "w") as f:
        f.write(SENDER_SCRIPT)
    with open("temp_receiver.py", "w") as f:
        f.write(RECEIVER_SCRIPT)
    os.chmod("temp_sender.py", 0o755)
    os.chmod("temp_receiver.py", 0o755)

def clean_scripts():
    """Menghapus file sementara"""
    if os.path.exists("temp_sender.py"): os.remove("temp_sender.py")
    if os.path.exists("temp_receiver.py"): os.remove("temp_receiver.py")

def run():
    # 1. Buat file script dulu
    create_scripts()
    
    # 2. Setup Mininet
    topo = SpineLeafTopo()
    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    
    print("[*] Starting Mininet Spine-Leaf Topology...")
    net.start()

    # 3. Config OVS (PENTING untuk Ryu)
    for s in ['s1', 's2', 's3', 's4']:
        os.system(f'ovs-vsctl set bridge {s} protocols=OpenFlow13')

    h1 = net.get('h1')
    h2 = net.get('h2')

    print("[*] Waiting 5s for STP convergence (Ryu)...")
    time.sleep(5)

    # 4. Jalankan Traffic Generator
    print("[*] Starting Receiver on h2...")
    h2.cmd('python3 temp_receiver.py &')
    
    print("[*] Starting VoIP Sender on h1 (Background)...")
    h1.cmd('python3 temp_sender.py 10.0.0.2 > traffic.log 2>&1 &')

    print("\n[INFO] Simulation Running.")
    print("[INFO] Data flow: H1 -> [Network] -> H2")
    print("[INFO] Ryu Collector should be capturing this now.")
    
    # 5. Masuk ke CLI
    CLI(net)
    
    # 6. Cleanup
    print("[*] Stopping Network...")
    net.stop()
    clean_scripts()

if __name__ == '__main__':
    setLogLevel('info')
    try:
        run()
    except Exception as e:
        print(f"[!] Error: {e}")
        clean_scripts()