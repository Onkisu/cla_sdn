from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp, ipv4
from ryu.lib.packet import ether_types
from ryu.topology import event, api as topo_api
from ryu.app.gui_topology import gui_topology


from collections import defaultdict, deque
import ipaddress

class FullRouter(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    # Router MAC per subnet (boleh sama, tapi saya bedakan biar jelas)
    ROUTER_MAC = {
        '10.0.0.0/24': '00:00:00:00:aa:00',
        '10.0.1.0/24': '00:00:00:00:aa:01',
        '10.0.2.0/24': '00:00:00:00:aa:02',
    }
    GATEWAYS = {
        '10.0.0.0/24': '10.0.0.254',
        '10.0.1.0/24': '10.0.1.254',
        '10.0.2.0/24': '10.0.2.254',
    }

    def __init__(self, *args, **kwargs):
        super(FullRouter, self).__init__(*args, **kwargs)
        # host_db: ip -> {'mac':..., 'dpid':..., 'port':...}
        self.name = "FullRouterApp123x"   # biar unik, ga bentrok
        self.host_db = {}
        # adjacency: dpid -> {neighbor_dpid: (out_port, in_port)}
        self.adjacency = defaultdict(dict)
        # datapaths: dpid -> datapath
        self.datapaths = {}

    # ---------- Switch & Topology events ----------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser
        self.datapaths[dp.id] = dp

        # table-miss: kirim ke controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, match, actions)
        self.logger.info(f"Switch {dp.id} connected, table-miss installed")

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change(self, ev):
        dp = ev.datapath
        if not dp:
            return
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[dp.id] = dp
        elif ev.state == DEAD_DISPATCHER:
            if dp.id in self.datapaths:
                del self.datapaths[dp.id]

    # Dengerin event link discovery (butuh gui_topology/simple_switch_13 aktif)
    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src
        dst = ev.link.dst
        # simpan adjacency: (dpid -> neighbor -> (out_port, in_port))
        self.adjacency[src.dpid][dst.dpid] = (src.port_no, dst.port_no)
        self.adjacency[dst.dpid][src.dpid] = (dst.port_no, src.port_no)
        self.logger.info(f"Link {src.dpid}:{src.port_no} <-> {dst.dpid}:{dst.port_no}")

    # ---------- Util ----------
    def add_flow(self, dp, priority, match, actions, idle_timeout=120, hard_timeout=0):
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=dp, priority=priority,
                                match=match, instructions=inst,
                                idle_timeout=idle_timeout, hard_timeout=hard_timeout)
        dp.send_msg(mod)

    def ip_subnet_of(self, ip):
        ipaddr = ipaddress.ip_address(ip)
        for cidr in self.ROUTER_MAC.keys():
            if ipaddr in ipaddress.ip_network(cidr):
                return cidr
        return None

    def bfs_path(self, src_dpid, dst_dpid):
        """Shortest path (list of dpids) via BFS."""
        if src_dpid == dst_dpid:
            return [src_dpid]
        visited = set([src_dpid])
        q = deque([[src_dpid]])
        while q:
            path = q.popleft()
            u = path[-1]
            for v in self.adjacency[u].keys():
                if v in visited: 
                    continue
                visited.add(v)
                newp = path + [v]
                if v == dst_dpid:
                    return newp
                q.append(newp)
        return None

    # ---------- PacketIn ----------
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        parser = dp.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        # Learn source MAC/IP/port kalau ada IPv4/ARP
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        if arp_pkt:
            # Learn sender (who is asking)
            self.host_db[arp_pkt.src_ip] = {'mac': arp_pkt.src_mac, 'dpid': dp.id, 'port': in_port}
            # Reply ARP kalau nanya gateway
            gw_ips = set(self.GATEWAYS.values())
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in gw_ips:
                self.reply_arp(dp, in_port, arp_pkt)
            return

        if ip_pkt:
            # Learn source
            self.host_db[ip_pkt.src] = {'mac': eth.src, 'dpid': dp.id, 'port': in_port}

            # Kalau tujuan belum dikenal → flood untuk memancing respons (ARP/ICMP) sehingga controller bisa learn dst
            if ip_pkt.dst not in self.host_db:
                # Flood the packet so destination host (or its gateway) can reply and controller learns its location
                ofp = dp.ofproto
                actions = [parser.OFPActionOutput(ofp.OFPP_FLOOD)]
                out = parser.OFPPacketOut(datapath=dp,
                                        buffer_id=msg.buffer_id,
                                        in_port=in_port,
                                        actions=actions,
                                        data=msg.data if msg.buffer_id == dp.ofproto.OFP_NO_BUFFER else None)
                dp.send_msg(out)
                return


            dst_info = self.host_db[ip_pkt.dst]
            path = self.bfs_path(dp.id, dst_info['dpid'])
            if not path:
                self.logger.warning(f"No path {dp.id} -> {dst_info['dpid']}")
                return

            # Pasang flow di semua hop for forward (src->dst)
            self.install_path(path, ip_pkt.src, ip_pkt.dst, dst_info)

            # Kirim packet sekarang (fast path) dari switch sekarang ke next hop
            out_port = self.next_hop_port(path, dp.id)
            if out_port is None:
                # same switch (direct host)
                actions = [parser.OFPActionOutput(dst_info['port'])]
            else:
                actions = [parser.OFPActionOutput(out_port)]
            out = parser.OFPPacketOut(datapath=dp,
                                    buffer_id=msg.buffer_id,
                                    in_port=in_port,
                                    actions=actions,
                                    data=msg.data if msg.buffer_id == dp.ofproto.OFP_NO_BUFFER else None)
            dp.send_msg(out)

    # ---------- ARP ----------
    def reply_arp(self, dp, in_port, arp_req):
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        subnet = self.ip_subnet_of(arp_req.dst_ip)
        if not subnet:
            return
        src_mac = self.ROUTER_MAC[subnet]

        e = ethernet.ethernet(dst=arp_req.src_mac, src=src_mac, ethertype=ether_types.ETH_TYPE_ARP)
        a = arp.arp(opcode=arp.ARP_REPLY,
                    src_mac=src_mac, src_ip=arp_req.dst_ip,
                    dst_mac=arp_req.src_mac, dst_ip=arp_req.src_ip)
        p = packet.Packet()
        p.add_protocol(e); p.add_protocol(a); p.serialize()

        actions = [parser.OFPActionOutput(in_port)]
        out = parser.OFPPacketOut(datapath=dp, buffer_id=ofp.OFP_NO_BUFFER,
                                  in_port=ofp.OFPP_CONTROLLER, actions=actions, data=p.data)
        dp.send_msg(out)

    # ---------- Path install ----------
    def next_hop_port(self, path, curr_dpid):
        """Port dari curr_dpid menuju next dpid dalam path."""
        if len(path) == 1:
            return None
        idx = path.index(curr_dpid)
        next_dpid = path[idx+1]
        out_port, _ = self.adjacency[curr_dpid][next_dpid]
        return out_port

    def prev_hop_port(self, path, curr_dpid):
        """Port dari curr_dpid menuju prev dpid dalam path."""
        idx = path.index(curr_dpid)
        if idx == 0:
            return None
        prev_dpid = path[idx-1]
        out_port, _ = self.adjacency[curr_dpid][prev_dpid]
        return out_port

    def install_path(self, path, ip_src, ip_dst, dst_info):
        """
        Pasang flow per-hop:
        - Hop tengah: match IP dst & src → OUTPUT ke next hop.
        - Hop terakhir (switch yang pegang host dst): rewrite MAC seperti router + DEC_TTL + OUTPUT ke port host.
        - Pasang juga reverse path (dst -> src).
        """
        dst_dpid = dst_info['dpid']
        dst_port = dst_info['port']
        dst_mac  = dst_info['mac']

        src_subnet = self.ip_subnet_of(ip_src)
        dst_subnet = self.ip_subnet_of(ip_dst)
        mac_src_router = self.ROUTER_MAC[src_subnet] if src_subnet else '00:00:00:00:aa:ff'
        mac_dst_router = self.ROUTER_MAC[dst_subnet] if dst_subnet else '00:00:00:00:aa:ff'

        # FORWARD (src -> dst)
        for dpid in path:
            dp = self.datapaths.get(dpid)
            if not dp:
                continue
            parser = dp.ofproto_parser

            if dpid == dst_dpid:
                # egress switch: rewrite MAC seperti router
                actions = [
                    parser.OFPActionSetField(eth_src=mac_dst_router),
                    parser.OFPActionSetField(eth_dst=dst_mac),
                    parser.OFPActionDecNwTtl(),
                    parser.OFPActionOutput(dst_port)
                ]
            else:
                # hop tengah: kirim ke next hop
                out_port = self.next_hop_port(path, dpid)
                actions = [parser.OFPActionOutput(out_port)]

            match = parser.OFPMatch(eth_type=0x0800, ipv4_src=ip_src, ipv4_dst=ip_dst)
            self.add_flow(dp, priority=50, match=match, actions=actions)

        # REVERSE (dst -> src)
        # butuh info host sumber juga (sudah ke-learn dari packet masuk)
        src_info = self.host_db.get(ip_src)
        if not src_info:
            return
        rev_path = list(reversed(path))
        src_dpid = src_info['dpid']
        src_port = src_info['port']
        src_mac  = src_info['mac']

        for dpid in rev_path:
            dp = self.datapaths.get(dpid)
            if not dp:
                continue
            parser = dp.ofproto_parser

            if dpid == src_dpid:
                actions = [
                    parser.OFPActionSetField(eth_src=mac_src_router),
                    parser.OFPActionSetField(eth_dst=src_mac),
                    parser.OFPActionDecNwTtl(),
                    parser.OFPActionOutput(src_port)
                ]
            else:
                out_port = self.next_hop_port(rev_path, dpid)
                actions = [parser.OFPActionOutput(out_port)]

            match = parser.OFPMatch(eth_type=0x0800, ipv4_src=ip_dst, ipv4_dst=ip_src)
            self.add_flow(dp, priority=50, match=match, actions=actions)
