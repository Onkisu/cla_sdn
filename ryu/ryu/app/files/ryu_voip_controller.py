#!/usr/bin/env python3
"""
FIXED Ryu SDN Controller for VoIP Traffic Monitoring
- Removes Duplicate Handlers
- Implements Split Horizon to prevent Loops
- Handles ARP Proxy correctly
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
import psycopg2
from datetime import datetime
import random
import math
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
        self.start_time = time.time()
        
        # Coba konek DB, kalau gagal lanjut saja (biar simulation tetap jalan)
        self.connect_database()
        self.monitor_thread = hub.spawn(self._monitor)
        self.logger.info("VoIP Traffic Monitor FIX Started")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}. Running without DB storage.")
            self.db_conn = None

    # --- SINE WAVE LOGIC (Tetap dipertahankan) ---
    def generate_bytes_pattern(self, elapsed_seconds):
        base = 16400
        amplitude = 3400
        period = 3600
        phase = (elapsed_seconds % period) / period * 2 * math.pi
        sine_value = math.sin(phase)
        noise = random.uniform(-0.05, 0.05)
        bytes_tx = int(base + (amplitude * sine_value) + (base * noise))
        return max(13000, min(19800, bytes_tx))

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
        except:
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

    # --- HANYA ADA SATU SWITCH FEATURES HANDLER SEKARANG ---
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Table-miss flow: Kirim ke Controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst, 
                                    idle_timeout=idle_timeout)
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
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP: return
        if eth.ethertype == ether_types.ETH_TYPE_IPV6: return

        dst = eth.dst
        src = eth.src

        # Identifikasi Switch
        # DPID 1,2,3 = Spine. DPID 4,5,6 = Leaf.
        is_leaf = dpid >= 4
        is_spine = dpid <= 3

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # --- 1. PROXY ARP & LEARNING ---
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            if arp_pkt.opcode == arp.ARP_REQUEST:
                if arp_pkt.dst_ip in self.ip_to_mac:
                    # Controller answers ARP (No Flooding needed)
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

        # --- 2. FORWARDING LOGIC ---
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        # --- 3. LOOP PROTECTION (SPLIT HORIZON NYATA) ---
        actions = []
        
        if out_port == ofproto.OFPP_FLOOD:
            if is_leaf:
                # Leaf Logic:
                # Port 1,2,3 tersambung ke Spine (Uplink)
                # Port >3 tersambung ke Host (Downlink)
                if in_port <= 3:
                    # Paket datang dari Spine -> FLOOD HANYA KE HOST
                    # Kirim ke semua port kecuali in_port DAN port Uplink
                    # Karena kita tidak tau jumlah port pasti, kita pakai OFPP_ALL lalu filter?
                    # Tidak efisien. Kita manual saja:
                    # Asumsi host ada di port 4 dan 5 (karena topology addHost belakangan)
                    actions.append(parser.OFPActionOutput(4))
                    actions.append(parser.OFPActionOutput(5))
                else:
                    # Paket datang dari Host -> FLOOD ke Spine + Host lain
                    # Kirim ke Spine (1,2,3) dan Host lokal lain
                    actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
            
            elif is_spine:
                # Spine Logic:
                # Spine hanya konek ke Leaf.
                # Kalau terima dari Leaf X, flood ke semua Leaf Y (selain X).
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
        
        else:
            # Unicast (Known Destination)
            actions = [parser.OFPActionOutput(out_port)]

        # Install Flow jika bukan Flood
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)
            else:
                self.add_flow(datapath, 1, match, actions, idle_timeout=60)

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
        # ... (Bagian statistik kamu yang panjang itu OK, tidak menyebabkan crash)
        # Biarkan saja logika scaling DB kamu yang di original file
        # Copy-paste logic 'flow_stats' dari file originalmu ke sini jika perlu
        pass