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
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)

    info('*** Adding Controller\n')
    # Pastikan ryu dijalankan secara terpisah atau biarkan Mininet connect ke port 6633 default
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    info('*** Adding Leaf and Spine Switches\n')
    # 3 Spines
    s1 = net.addSwitch('s1', cls=OVSKernelSwitch)
    s2 = net.addSwitch('s2', cls=OVSKernelSwitch)
    s3 = net.addSwitch('s3', cls=OVSKernelSwitch)

    # 3 Leaves
    l1 = net.addSwitch('l1', cls=OVSKernelSwitch)
    l2 = net.addSwitch('l2', cls=OVSKernelSwitch)
    l3 = net.addSwitch('l3', cls=OVSKernelSwitch)

    info('*** Adding Hosts\n')
    # Host 1 di Leaf 1, Host 2 di Leaf 3 (supaya traffic lewat spine)
    h1 = net.addHost('h1', mac='00:00:00:00:00:01', ip='10.0.0.1/24')
    h2 = net.addHost('h2', mac='00:00:00:00:00:02', ip='10.0.0.2/24')

    info('*** Creating Links (Leaf-Spine Full Mesh)\n')
    # Connect setiap Leaf ke SEMUA Spine
    leaves = [l1, l2, l3]
    spines = [s1, s2, s3]

    for leaf in leaves:
        for spine in spines:
            net.addLink(leaf, spine)

    # Connect Hosts ke Leaf
    net.addLink(h1, l1)
    net.addLink(h2, l3)

    info('*** Starting Network\n')
    net.build()
    c0.start()
    for sw in spines + leaves:
        sw.start([c0])
        # PENTING: Mengaktifkan Spanning Tree Protocol (STP) karena topologi memiliki loop
        sw.cmd('ovs-vsctl set Bridge %s stp_enable=true' % sw.name)

    info('*** Waiting for STP convergence (15 seconds)...\n')
    time.sleep(15)

    info('*** Testing connectivity (Ping)...\n')
    net.ping([h1, h2])

    info('*** Preparing Output File & TShark\n')
    output_file = "traffic_result.txt"
    # Perintah TShark untuk menangkap paket UDP dan memformat output sesuai permintaan
    # Fields: time_relative, source, protocol, length, time_absolute, info, frame_num, destination
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
    time.sleep(2) # Tunggu ITGRecv siap
    h2.cmd(tshark_cmd)
    tshark_pid = h2.cmd('echo $!') # Simpan PID untuk kill nanti
    
    info('*** Generating Traffic from h1 to h2 using D-ITG\n')
    # Kirim traffic UDP ke port 8999, durasi 5 detik, rate 1000 pkt/sec, size 100 bytes payload
    # Parameter: -T UDP, -a (dest IP), -t (duration ms), -C (rate), -c (payload size), -rp (dest port)
    h1.cmd('ITGSend -T UDP -a 10.0.0.2 -t 5000 -C 10 -c 100 -rp 8999 &')
    
    info('*** Traffic generation in progress (Wait 10s)...\n')
    time.sleep(10)

    info('*** Stopping processes\n')
    h2.cmd('killall ITGRecv')
    h2.cmd('killall tshark')

    info('*** Formatting Output (Header + Content)\n')
    # Menambahkan header manual agar sesuai format permintaan
    header = "time\tsource\tprotocol\tlength\tArrival Time\tinfo\tNo.\tdestination\n"
    with open(output_file, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(header + content)

    info(f'*** DONE. Result saved in {output_file}\n')
    info('*** Displaying first 5 lines of result:\n')
    os.system(f'head -n 6 {output_file}')
    
    # --- TAMBAHAN: Panggil script upload ke DB ---
    info('*** Uploading to PostgreSQL...\n')
    os.system('python3 pcap_psql.py')

    info('*** Stopping Network\n')
    net.stop()

    

if __name__ == '__main__':
    setLogLevel('info')
    run_experiment()