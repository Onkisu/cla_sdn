#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.nodelib import NAT
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import subprocess
import os

def get_vps_iface():
    """Detect VPS interface dengan default route"""
    result = subprocess.run(
        "ip route | grep default | awk '{print $5}'",
        shell=True,
        capture_output=True,
        text=True
    )
    iface = result.stdout.strip()
    if not iface:
        raise Exception("Gagal deteksi interface VPS!")
    return iface

def prepare_vps():
    """Aktifkan IP forwarding & bersihkan firewall"""
    info('*** Mengaktifkan IP forwarding\n')
    os.system('sudo sysctl -w net.ipv4.ip_forward=1')
    info('*** Membersihkan iptables\n')
    os.system('sudo iptables -F')
    os.system('sudo iptables -t nat -F')
    os.system('sudo iptables -X')

def run():
    prepare_vps()

    net = Mininet(controller=RemoteController, link=TCLink)

    info('*** Menambahkan controller Ryu\n')
    c0 = net.addController('c0', ip='127.0.0.1', port=6633)

    info('*** Menambahkan switch\n')
    s1 = net.addSwitch('s1')

    iface = get_vps_iface()
    info(f'*** Menggunakan interface VPS: {iface}\n')

    info('*** Menambahkan NAT host\n')
    nat0 = net.addHost('nat0', cls=NAT, ip='10.0.0.254/24', inetIntf=iface)

    info('*** Menambahkan host Mininet\n')
    h1 = net.addHost('h1', ip='10.0.0.1/24', defaultRoute='via 10.0.0.254')
    h2 = net.addHost('h2', ip='10.0.0.2/24', defaultRoute='via 10.0.0.254')

    info('*** Membuat link\n')
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(nat0, s1)

    info('*** Memulai jaringan Mininet\n')
    net.start()

    info('*** Konfigurasi NAT otomatis\n')
    nat0.configDefault()

    info('*** Jaringan siap, masuk CLI\n')
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
