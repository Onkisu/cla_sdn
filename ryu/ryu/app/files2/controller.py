#!/usr/bin/env python3
"""
PURE MONITORING CONTROLLER (SMART DROPS EDITION)
- Fix: Menghitung Drops berdasarkan saturasi Link.
- Logic: Jika Throughput > 10 Mbps, selisihnya dianggap sebagai Packet Loss.
- Fix Update: Insert Delta Bytes/Packets ke DB agar tidak akumulatif selamanya.
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types
import psycopg2
import time

# Credentials
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Kapasitas Link Fisik (Sesuai simulation.py = 10 Mbps)
# Kita set warning di 9.5 Mbps untuk mulai menghitung drop
LINK_CAPACITY_BPS = 10000000 
SAFE_THRESHOLD_BPS = 9500000

class RealTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(RealTrafficMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.mac_to_port = {}
        self.prev_stats = {} 
        self.prev_port_stats = {} 
        self.monitor_thread = hub.spawn(self._monitor)
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            self.logger.info("✅ Database Connected")
        except Exception as e:
            self.logger.error(f"❌ DB Error: {e}")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
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
        
        dst = eth.dst
        src = eth.src
        
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions)

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
        # Port stats tetap diminta untuk verifikasi
        req_port = parser.OFPPortStatsRequest(datapath)
        datapath.send_msg(req_port)

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    # --- HANDLER: FLOW STATS (Smart Calculation) ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        for stat in body:
            if stat.priority == 0: continue 
            
            match_str = str(stat.match)
            prev_key = (dpid, match_str)
            
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            if prev_key in self.prev_stats:
                last_bytes, last_pkts, last_time = self.prev_stats[prev_key]
                time_delta = now - last_time
                if time_delta <= 0: continue
                
                delta_bytes = byte_count - last_bytes
                delta_pkts = packet_count - last_pkts
                
                throughput_bps = int((delta_bytes * 8) / time_delta)
                pps = int(delta_pkts / time_delta)
                
                # --- LOGIC BARU: HITUNG DROPS ---
                calculated_drops = 0
                
                if throughput_bps > SAFE_THRESHOLD_BPS:
                    excess_bps = throughput_bps - LINK_CAPACITY_BPS
                    if excess_bps > 0:
                        avg_pkt_size_bits = (delta_bytes * 8) / delta_pkts if delta_pkts > 0 else 8000
                        calculated_drops = int(excess_bps / avg_pkt_size_bits)
                        
                        self.logger.warning(f"⚠️ CONGESTION DPID {dpid}: {throughput_bps/1e6:.1f} Mbps | Est. Drops: {calculated_drops}")

                if throughput_bps > 1000:
                    # PERBAIKAN DI SINI:
                    # Masukkan delta_bytes dan delta_pkts, BUKAN byte_count/packet_count kumulatif.
                    # Ini akan membuat angka di DB merepresentasikan volume per detik (atau per interval polling),
                    # sehingga tidak terus bertambah selamanya.
                    self.insert_stats(dpid, throughput_bps, pps, delta_bytes, delta_pkts, drops=calculated_drops)
            
            self.prev_stats[prev_key] = (byte_count, packet_count, now)

    # --- HANDLER: PORT STATS (Backup Real Drops) ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        for stat in body:
            port_no = stat.port_no
            current_drops = stat.tx_dropped + stat.rx_dropped
            key = (dpid, port_no)
            
            if key in self.prev_port_stats:
                last_drops, last_time = self.prev_port_stats[key]
                delta_drops = current_drops - last_drops
                
                # Jika hardware/OVS melaporkan drop, kita masukkan juga
                if delta_drops > 0:
                    self.logger.warning(f"HARDWARE DROP on Switch {dpid}: {delta_drops} pkts")
                    self.insert_stats(dpid, 0, 0, 0, 0, drops=delta_drops)
            
            self.prev_port_stats[key] = (current_drops, now)

    def insert_stats(self, dpid, bps, pps, bytes_curr, pkts_curr, drops=0):
        if not self.conn: return
        try:
            # Cegah insert drops negatif
            drops = max(0, drops)
            
            # bytes_curr dan pkts_curr sekarang berisi nilai Delta (volume saat ini), bukan total kumulatif.
            query = """
            INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s) 
            """
            self.cur.execute(query, (str(dpid), bps, pps, bytes_curr, pkts_curr, drops))
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"DB Insert Error: {e}")
            self.conn.rollback()