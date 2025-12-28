#!/usr/bin/python

import time
import os
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch, Host
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink

def run_experiment():
    # 1. Initialize Mininet
    # Hapus konfigurasi controller IP disini, kita set manual di bawah
    net = Mininet(controller=None, switch=OVSKernelSwitch, link=TCLink)

    info('*** Adding Controller\n')
    # Connect ke Ryu Controller
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    info('*** Adding Leaf and Spine Switches (OpenFlow 1.3)\n')
    # Pastikan protocols='OpenFlow13'
    s1 = net.addSwitch('s1', cls=OVSKernelSwitch, protocols='OpenFlow13')
    s2 = net.addSwitch('s2', cls=OVSKernelSwitch, protocols='OpenFlow13')
    s3 = net.addSwitch('s3', cls=OVSKernelSwitch, protocols='OpenFlow13')

    l1 = net.addSwitch('l1', cls=OVSKernelSwitch, protocols='OpenFlow13')
    l2 = net.addSwitch('l2', cls=OVSKernelSwitch, protocols='OpenFlow13')
    l3 = net.addSwitch('l3', cls=OVSKernelSwitch, protocols='OpenFlow13')

    info('*** Adding Hosts\n')
    h1 = net.addHost('h1', mac='00:00:00:00:00:01', ip='10.0.0.1/24')
    h2 = net.addHost('h2', mac='00:00:00:00:00:02', ip='10.0.0.2/24')

    info('*** Creating Links (Leaf-Spine Full Mesh)\n')
    # Full mesh connections
    leaves = [l1, l2, l3]
    spines = [s1, s2, s3]

    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine)

    net.addLink(h1, l1)
    net.addLink(h2, l3)

    info('*** Starting Network\n')
    net.build()
    c0.start()
    for sw in spines + leaves:
        sw.start([c0])
        # [PENTING] JANGAN aktifkan OVS STP disini ('ovs-vsctl set Bridge ... stp_enable=true')
        # Biarkan Ryu controller yang mengatur port blocking via OpenFlow.

    info('*** Waiting for Controller STP convergence (45 seconds)...\n')
    # Ryu STP butuh waktu agak lama untuk kalkulasi topology
    time.sleep(45)

    info('*** Testing connectivity (Ping)...\n')
    # Lakukan ping manual untuk memicu ARP learning path
    net.ping([h1, h2])

    info('*** Preparing Output File & TShark\n')
    output_file = "traffic_result.txt"
    if os.path.exists(output_file):
        os.remove(output_file)

    # Command TShark sama seperti sebelumnya
    tshark_cmd = (
        "tshark -i h2-eth0 -n -l -Y 'udp.port==8999' "
        "-T fields "
        "-e frame.time_relative "
        "-e ip.src "
        "-e _ws.col.Protocol "
        "-e frame.len "
        "-e frame.time "
        "-e _ws.col.Info "
        "-e frame.number "
        "-e ip.dst "
        "> %s 2>/dev/null &" % output_file
    )

    info('*** Starting ITGRecv and TShark on h2\n')
    h2.cmd('ITGRecv &')
    time.sleep(2)
    h2.cmd(tshark_cmd)
    
    info('*** Generating Traffic from h1 to h2 using D-ITG\n')
    h1.cmd('ITGSend -T UDP -a 10.0.0.2 -t 5000 -C 10 -c 100 -rp 8999 &')
    
    info('*** Traffic generation in progress (Wait 10s)...\n')
    time.sleep(10)

    info('*** Stopping processes\n')
    h2.cmd('killall ITGRecv')
    h2.cmd('killall tshark')

    info('*** Formatting Output (Header + Content)\n')
    header = "time\tsource\tprotocol\tlength\tArrival Time\tinfo\tNo.\tdestination\n"
    
    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
        with open(output_file, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(header + content)
            
        info('*** Uploading to PostgreSQL...\n')
        os.system('python3 pcap_psql.py')
    else:
        info('*** WARNING: File output kosong. Cek apakah Ping berhasil.\n')

    info('*** Stopping Network\n')
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run_experiment()