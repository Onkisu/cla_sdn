#!/usr/bin/python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info


class FatTreeTopo(Topo):
    def build(self):
        info('*** Building FatTree Topology\n')

        # Controller parameters
        controller_ip = '127.0.0.1'
        controller_port = 6633

        # Core switches
        cores = []
        for i in range(2):
            cores.append(
                self.addSwitch(
                    f'c{i+1}',
                    dpid=f'{100+i:016x}',
                    protocols='OpenFlow13',
                    failMode='secure'
                )
            )

        # Aggregation and Edge switches (2 pods)
        for p in range(2):
            aggs_pod = []
            edges_pod = []

            for a in range(2):
                aggs_pod.append(
                    self.addSwitch(
                        f'a{p+1}_{a+1}',
                        dpid=f'{200+p*10+a:016x}',
                        protocols='OpenFlow13',
                        failMode='secure'
                    )
                )
            for e in range(2):
                edges_pod.append(
                    self.addSwitch(
                        f'e{p+1}_{e+1}',
                        dpid=f'{300+p*10+e:016x}',
                        protocols='OpenFlow13',
                        failMode='secure'
                    )
                )

            # Connect agg <-> edge (in same pod)
            for agg in aggs_pod:
                for edge in edges_pod:
                    self.addLink(agg, edge, cls=TCLink, bw=10)

            # Connect core <-> aggregation (cross pod)
            for i, core in enumerate(cores):
                self.addLink(core, aggs_pod[i % len(aggs_pod)], cls=TCLink, bw=10)

            # Add hosts under each edge
            for idx, edge in enumerate(edges_pod):
                for h in range(2):
                    host = self.addHost(f'h{p+1}{idx+1}{h+1}', ip=f'10.0.{p}{idx}{h+1}/24')
                    self.addLink(edge, host, cls=TCLink, bw=10)


def run():
    setLogLevel('info')
    info('*** Starting network\n')

    topo = FatTreeTopo()
    net = Mininet(
        topo=topo,
        controller=None,
        switch=OVSSwitch,
        link=TCLink,
        build=False
    )

    info('*** Adding remote controller\n')
    c0 = net.addController(
        'c0',
        controller=RemoteController,
        ip='127.0.0.1',
        port=6633
    )

    net.build()
    net.start()

    info('*** Network is ready!\n')
    CLI(net)
    net.stop()


if __name__ == '__main__':
    run()
