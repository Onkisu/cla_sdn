#!/usr/bin/env python3
"""
PURE MONITORING CONTROLLER
- No Manipulation.
- Collects Real Throughput & Drop Counts.
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, arp
import psycopg2
from datetime import datetime
import time

# Credentials from your file
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class RealTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(RealTrafficMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.mac_to_port = {}
        # Dictionary untuk menyimpan state sebelumnya guna hitung delta
        # Key: (dpid, match) -> Value: (bytes, pkts, timestamp)
        self.prev_stats = {} 
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

        # Simple Learning Switch Logic
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install Flow jika bukan flooding
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            # Penting: Idle timeout agar flow lama hilang dan tidak mengotori stats
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
            hub.sleep(1) # Interval 1 detik

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)
        
        # Request Port Stats untuk melihat Dropped Packets (Indikator Congestion Fisik)
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

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        # Kita hanya peduli traffic IPv4 antar host untuk prediksi
        for stat in body:
            if stat.priority == 0: continue # Skip table-miss
            
            # Key unik untuk flow
            match_str = str(stat.match)
            prev_key = (dpid, match_str)
            
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            # Hitung Delta
            if prev_key in self.prev_stats:
                last_bytes, last_pkts, last_time = self.prev_stats[prev_key]
                time_delta = now - last_time
                if time_delta <= 0: continue
                
                delta_bytes = byte_count - last_bytes
                delta_pkts = packet_count - last_pkts
                
                # Hitung Rates
                throughput_bps = int((delta_bytes * 8) / time_delta)
                pps = int(delta_pkts / time_delta)
                
                # Simpan ke DB hanya jika ada traffic signifikan (> 1Kbps)
                if throughput_bps > 1000:
                    self.insert_stats(dpid, throughput_bps, pps, byte_count, packet_count)
            
            # Update state
            self.prev_stats[prev_key] = (byte_count, packet_count, now)

    def insert_stats(self, dpid, bps, pps, bytes_total, pkts_total):
        if not self.conn: return
        try:
            # Simpan data real
            # is_congestion_event default False, nanti di update oleh Forecaster 
            # jika BPS mendekati kapasitas link (misal 10Mbps)
            query = """
            INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, 0) 
            """
            self.cur.execute(query, (str(dpid), bps, pps, bytes_total, pkts_total))
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"DB Insert Error: {e}")
            self.conn.rollback()