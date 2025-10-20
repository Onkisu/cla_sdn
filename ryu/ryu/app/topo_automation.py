#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import time
import subprocess
import threading
import random
from datetime import datetime

# Kelas LinuxRouter dan ComplexTopo tetap sama persis
class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class ComplexTopo(Topo):
    def build(self):
        # Router
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # Switch
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        # Hosts
        h1 = self.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
        h2 = self.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')
        h3 = self.addHost('h3', ip='10.0.0.3/24', defaultRoute='via 10.0.0.254')

        h4 = self.addHost('h4', ip='10.0.1.1/24', defaultRoute='via 10.0.1.254')
        h5 = self.addHost('h5', ip='10.0.1.2/24', defaultRoute='via 10.0.1.254')
        h6 = self.addHost('h6', ip='10.0.1.3/24', defaultRoute='via 10.0.1.254')

        h7 = self.addHost('h7', ip='10.0.2.1/24', defaultRoute='via 10.0.2.254')

        # Link hosts ke switch
        self.addLink(h1, s1, bw=5); self.addLink(h2, s1, bw=5); self.addLink(h3, s1, bw=5)
        self.addLink(h4, s2, bw=10, delay='32ms', loss=2); self.addLink(h5, s2, bw=10, delay='47ms', loss=2); self.addLink(h6, s2, bw=10)
        self.addLink(h7, s3, bw=2, delay='50ms', loss=2)

        # Link router ke masing-masing switch (gateway tiap subnet)
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'}) 
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

# --- PERUBAHAN UTAMA DIMULAI DI SINI ---

def generate_client_traffic(client, server_ip, port, base_bw_off_peak, base_bw_peak):
    """
    Fungsi ini berjalan dalam thread terpisah untuk satu klien.
    Secara terus-menerus menghasilkan traffic dengan bandwidth yang bervariasi
    berdasarkan waktu dan faktor acak.
    """
    info(f"Starting dynamic traffic for {client.name} -> {server_ip}\n")
    while True:
        try:
            current_hour = datetime.now().hour
            
            # Tentukan base bandwidth berdasarkan jam (pola harian)
            # Jam 18:00 - 23:00 dianggap jam sibuk (peak hours)
            if 18 <= current_hour < 24:
                base_bw = base_bw_peak
            else:
                base_bw = base_bw_off_peak

            # Tambahkan faktor acak (-20% s/d +20% dari base) agar tidak sama persis
            random_factor = random.uniform(0.8, 1.2)
            target_bw = base_bw * random_factor
            
            # Format bandwidth untuk iperf (misal: 3.87M)
            bw_str = f"{target_bw:.2f}M"
            
            # Konstruksi dan jalankan perintah iperf
            duration = 10 # durasi iperf dalam detik
            cmd = f"iperf -u -c {server_ip} -p {port} -b {bw_str} -t {duration}"
            
            # info(f"Executing for {client.name}: {cmd}\n") # Uncomment untuk debugging
            client.cmd(cmd)
            
            # Jeda antar pengiriman traffic
            time.sleep(random.uniform(1, 5)) # Jeda acak antara 1-5 detik

        except Exception as e:
            info(f"Error in traffic generation for {client.name}: {e}\n")
            time.sleep(10) # Jika error, tunggu sebentar sebelum mencoba lagi

def start_traffic(net):
    """Jalankan traffic otomatis menggunakan thread untuk setiap klien."""
    h1, h2, h3 = net.get('h1', 'h2', 'h3')
    h4, h5, h6 = net.get('h4', 'h5', 'h6')
    h7 = net.get('h7')

    info("*** Starting iperf servers\n")
    # YouTube server
    h4.cmd("iperf -s -u -p 443 &")
    # Netflix server  
    h5.cmd("iperf -s -u -p 443 &")
    # Twitch server
    h7.cmd("iperf -u -s -p 1935 &")
    
    time.sleep(1) # Beri waktu server untuk siap

    info("*** Starting dynamic iperf clients in background threads\n")
    
    # Klien 1 (YouTube)
    # Siang/Pagi: ~1.5 Mbps, Malam: ~4 Mbps
    t1 = threading.Thread(target=generate_client_traffic, 
                          args=(h1, '10.0.1.1', 443, 1.5, 4.0), daemon=True)
                          
    # Klien 2 (Netflix)
    # Siang/Pagi: ~1 Mbps, Malam: ~2.5 Mbps
    t2 = threading.Thread(target=generate_client_traffic, 
                          args=(h2, '10.0.1.2', 443, 1.0, 2.5), daemon=True)
                          
    # Klien 3 (Twitch)
    # Siang/Pagi: ~0.5 Mbps, Malam: ~1.5 Mbps
    t3 = threading.Thread(target=generate_client_traffic, 
                          args=(h3, '10.0.2.1', 1935, 0.5, 1.5), daemon=True)

    # Jalankan semua thread
    t1.start()
    t2.start()
    t3.start()

# --- PERUBAHAN UTAMA SELESAI ---

def run_forecast_loop():
    """Loop tiap 15 menit panggil forecast.py"""
    while True:
        info("\n*** Running AI Forecast...\n")
        try:
            subprocess.call(["sudo", "python3", "forecast.py"])
        except Exception as e:
            info(f"*** Forecast error: {e}\n")
        time.sleep(900)  # 15 menit

if __name__ == "__main__":
    setLogLevel("info")
    net = Mininet(topo=ComplexTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    # Mulai generate traffic
    start_traffic(net)

    # Jalankan forecast loop di background
    t_forecast = threading.Thread(target=run_forecast_loop, daemon=True)
    t_forecast.start()

    CLI(net)
    net.stop()