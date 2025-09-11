#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.nodelib import NAT
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import threading, subprocess, time

class LinuxRouter(Node):
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super(LinuxRouter, self).terminate()

class InternetTopo(Topo):
    def build(self):
        # Router internal
        r1 = self.addNode('r1', cls=LinuxRouter, ip='10.0.0.254/24')

        # NAT keluar ke internet
        nat = self.addNode('nat0', cls=NAT, ip='192.168.0.1/24', inNamespace=False)
        self.addLink(r1, nat, intfName1='r1-eth0', params1={'ip': '192.168.0.2/24'})

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

        # Link host ke switch
        self.addLink(h1, s1); self.addLink(h2, s1); self.addLink(h3, s1)
        self.addLink(h4, s2); self.addLink(h5, s2); self.addLink(h6, s2)
        self.addLink(h7, s3)

        # Link router ke switch-subnet
        self.addLink(r1, s1, intfName1='r1-eth1', params1={'ip': '10.0.0.254/24'})
        self.addLink(r1, s2, intfName1='r1-eth2', params1={'ip': '10.0.1.254/24'})
        self.addLink(r1, s3, intfName1='r1-eth3', params1={'ip': '10.0.2.254/24'})

def run_forecast_loop():
    while True:
        info("\n*** Running AI Forecast...\n")
        subprocess.call(["sudo","python3","forecast.py"])
        time.sleep(900)


if __name__=="__main__":
    setLogLevel("info")
    net = Mininet(topo=InternetTopo(),
                  switch=OVSSwitch,
                  controller=lambda name: RemoteController(name, ip="127.0.0.1", port=6633),
                  link=TCLink)
    net.start()

    # # NAT config
    # nat = net.get('nat0')
    # nat.configDefault()
    # nat.cmd("iptables -t nat -A POSTROUTING -o ens3 -j MASQUERADE")
    # nat.cmd("iptables -A FORWARD -i ens3 -o nat0-eth0 -m state --state RELATED,ESTABLISHED -j ACCEPT")
    # nat.cmd("iptables -A FORWARD -o ens3 -i nat0-eth0 -j ACCEPT")
    # nat.cmd("ifconfig nat0-eth0 up")


    # # Router config
    # r1 = net.get('r1')
    # r1.cmd("ifconfig r1-eth0 192.168.0.2/24 up")
    # r1.cmd("ip route add default via 192.168.0.1")

    # info("\n*** Testing internet from h1...\n")
    # h1 = net.get('h1')
    # print(h1.cmd("ping -c 3 8.8.8.8"))

        # ====== NAT & routing minimal (patch) ======
    # ambil objek
    nat = net.get('nat0')
    r1 = net.get('r1')

    # init nat node (buat iptables default dll)
    nat.configDefault()

    # detect uplink interface on host (root namespace)
    import subprocess, shlex
    def detect_uplink_iface():
        try:
            out = subprocess.check_output(shlex.split("ip route | grep default")).decode()
            parts = out.split()
            if "dev" in parts:
                i = parts.index("dev")
                if i+1 < len(parts):
                    return parts[i+1]
        except Exception:
            pass
        return "ens3"  # fallback
    uplink_if = detect_uplink_iface()

    # ensure host kernel forwards packets
    subprocess.call(["sysctl", "-w", "net.ipv4.ip_forward=1"])

    # create MASQUERADE on host root so Mininet traffic can go out
    # run iptables on root namespace (not inside nat.cmd) for reliability
    subprocess.call(["iptables", "-t", "nat", "-C", "POSTROUTING", "-o", uplink_if, "-j", "MASQUERADE"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) or \
    subprocess.call(["iptables", "-t", "nat", "-A", "POSTROUTING", "-o", uplink_if, "-j", "MASQUERADE"])

    # forward rules: accept established and allow mininet <-> uplink
    subprocess.call(["iptables", "-C", "FORWARD", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) or \
    subprocess.call(["iptables", "-A", "FORWARD", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"])

    # nat0-eth0 is veth on root created by Mininet; allow forward from it to uplink
    subprocess.call(["iptables", "-C", "FORWARD", "-i", "nat0-eth0", "-o", uplink_if, "-j", "ACCEPT"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) or \
    subprocess.call(["iptables", "-A", "FORWARD", "-i", "nat0-eth0", "-o", uplink_if, "-j", "ACCEPT"])

    subprocess.call(["iptables", "-C", "FORWARD", "-i", uplink_if, "-o", "nat0-eth0", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) or \
    subprocess.call(["iptables", "-A", "FORWARD", "-i", uplink_if, "-o", "nat0-eth0", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"])

    # ensure interfaces up (inside Mininet namespaces)
    nat.cmd("ifconfig nat0-eth0 up")
    r1.cmd("ifconfig r1-eth0 192.168.0.2/24 up")

    # add default route on r1 via nat
    # remove existing default if any to avoid duplicate errors
    r1.cmd("ip route del default 2>/dev/null || true")
    r1.cmd("ip route add default via 192.168.0.1")

    # push public DNS into Mininet hosts so domain resolution works
    for hname in ("h1","h2","h3","h4","h5","h6","h7"):
        try:
            h = net.get(hname)
            h.cmd("bash -c 'echo \"nameserver 8.8.8.8\" > /etc/resolv.conf'")
        except Exception:
            pass

    # short verification prints (non-blocking)
    print("NAT uplink interface:", uplink_if)
    print("iptables MASQUERADE & FORWARD rules applied on host root.")
    # ====== end patch ======

        # ====== Set default gateway sesuai subnet untuk semua host ======
    hosts_gateway = {
        "h1": "10.0.0.254",
        "h2": "10.0.0.254",
        "h3": "10.0.0.254",
        "h4": "10.0.1.254",
        "h5": "10.0.1.254",
        "h6": "10.0.1.254",
        "h7": "10.0.2.254",
    }

    for hname, gw in hosts_gateway.items():
        h = net.get(hname)
        # hapus default lama kalau ada
        h.cmd("ip route del default 2>/dev/null || true")
        # tambahin default sesuai gateway subnetnya
        h.cmd(f"ip route add default via {gw}")
        # DNS supaya bisa resolve google.com
        h.cmd("bash -c 'echo \"nameserver 8.8.8.8\" > /etc/resolv.conf'")


    # forecast loop background
    t = threading.Thread(target=run_forecast_loop, daemon=True)
    t.start()

    CLI(net)
    net.stop()

