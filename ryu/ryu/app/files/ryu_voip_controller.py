#!/usr/bin/env python3
"""
RYU CONTROLLER WITH FILTERING
- Hanya merekam traffic UDP > 800 Bytes (VoIP Traffic).
- Memberikan label 'voip-ditg'.
- Mengabaikan ICMP/ARP/Control Traffic (Noise).
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, arp
import psycopg2
from datetime import datetime
import time

# PostgreSQL Configuration
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class VoIPTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPTrafficMonitor, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.datapaths = {}
        self.db_conn = None
        self.last_bytes = {} 
        
        self.connect_database()
        self.monitor_thread = hub.spawn(self._monitor)
        self.logger.info("VoIP Monitor Started (Filter: UDP Only & Label: voip-ditg)")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}. Running without DB storage.")
            self.db_conn = None

    def insert_flow_data(self, flow_data):
        if not self.db_conn: return
        try:
            cursor = self.db_conn.cursor()
            query = """
            INSERT INTO traffic.flow_stats_ 
            (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
             ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
             duration_sec, traffic_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, flow_data)
            self.db_conn.commit()
            cursor.close()
        except Exception as e:
            self.db_conn.rollback()

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst, 
                                idle_timeout=idle_timeout, buffer_id=buffer_id if buffer_id else ofproto.OFP_NO_BUFFER)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP or eth.ethertype == ether_types.ETH_TYPE_IPV6: return

        dst = eth.dst
        src = eth.src
        is_leaf = dpid >= 4
        is_spine = dpid <= 3

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            if arp_pkt.opcode == arp.ARP_REQUEST:
                if arp_pkt.dst_ip in self.ip_to_mac:
                    target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                    e = ethernet.ethernet(dst=src, src=target_mac, ethertype=ether_types.ETH_TYPE_ARP)
                    a = arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=arp_pkt.dst_ip, dst_mac=src, dst_ip=arp_pkt.src_ip)
                    p = packet.Packet()
                    p.add_protocol(e)
                    p.add_protocol(a)
                    p.serialize()
                    actions = [parser.OFPActionOutput(in_port)]
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                    datapath.send_msg(out)
                    return

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = []
        if out_port == ofproto.OFPP_FLOOD:
            if is_leaf:
                if in_port <= 3: actions.extend([parser.OFPActionOutput(4), parser.OFPActionOutput(5)])
                else: actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
            elif is_spine:
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
        else:
            actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = format(datapath.id, '016x')
        timestamp = datetime.now()
        
        for stat in body:
            if stat.priority == 0: continue
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            if not src_ip or not dst_ip or dst_ip.endswith('.255'): continue
            
            ip_proto = match.get('ip_proto', 0)
            tp_src = match.get('udp_src', 0)
            tp_dst = match.get('udp_dst', 0)
            
            flow_key = f"{dpid}-{src_ip}-{dst_ip}-{tp_src}-{tp_dst}"
            current_bytes = stat.byte_count
            current_pkts = stat.packet_count
            
            if flow_key in self.last_bytes:
                last_b, last_p = self.last_bytes[flow_key]
                delta_bytes = current_bytes - last_b
                delta_pkts = current_pkts - last_p
            else:
                delta_bytes = current_bytes
                delta_pkts = current_pkts
            
            self.last_bytes[flow_key] = (current_bytes, current_pkts)
            
            # --- FILTERING LOGIC ---
            # 1. Harus UDP (ip_proto == 17)
            # 2. Bytes harus signifikan (> 800 bytes per detik) untuk dianggap VoIP aktif
            #    Paket ARP/Control biasanya cuma 60-300 bytes.
            
            if ip_proto == 17 and delta_bytes > 800:
                traffic_label = 'voip-ditg'
                
                self.logger.info(f"CAPTURED {traffic_label}: {src_ip}->{dst_ip} | {delta_bytes} Bytes")
                
                flow_data = (
                    timestamp, dpid,
                    src_ip, dst_ip,
                    match.get('eth_src', ''), match.get('eth_dst', ''),
                    ip_proto, tp_src, tp_dst,
                    delta_bytes, 0, delta_pkts, 0,
                    1.0, 
                    traffic_label # Disimpan sebagai 'voip-ditg'
                )
                self.insert_flow_data(flow_data)
            
            # Jika tidak memenuhi syarat (ICMP, atau traffic kecil), kita SKIP (tidak insert ke DB)

    def _resolve_ip(self, mac):
        if not mac: return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: return ip
        return None