#!/usr/bin/env python3
"""
REAL MONITOR V2
- Reads PORT STATS for real congestion drops (tx_dropped).
- Calculates DELTAS (no more cumulative increasing numbers).
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
import psycopg2
import time

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
        # Penyimpanan state sebelumnya untuk hitung Delta
        # format: self.prev_stats[dpid][match_str] = (bytes, pkts, timestamp)
        self.flow_prev = {} 
        # format: self.port_prev[dpid][port_no] = (tx_dropped, timestamp)
        self.port_prev = {} 
        # Buffer drops untuk digabungkan ke flow stats
        self.current_port_drops = {} 
        
        self.monitor_thread = hub.spawn(self._monitor)
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            print("✅ Database Connected")
        except Exception as e:
            print(f"❌ DB Error: {e}")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.datapaths[datapath.id] = datapath
        # Install Table-Miss Flow
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # (Simple L2 Switch Logic - Dipersingkat)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
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
        # 1. Request Port Stats DULU (untuk dapat data drops)
        req_port = parser.OFPPortStatsRequest(datapath)
        datapath.send_msg(req_port)
        # 2. Request Flow Stats
        req_flow = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req_flow)

    # --- HANDLE PORT STATS (Untuk Dropped Packets) ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        self.port_prev.setdefault(dpid, {})
        self.current_port_drops.setdefault(dpid, {})

        for stat in body:
            port_no = stat.port_no
            # tx_dropped: packet didrop karena queue switch penuh (Congestion Real)
            current_dropped = stat.tx_dropped 
            
            # Hitung Delta Drops
            prev_dropped = self.port_prev[dpid].get(port_no, 0)
            delta_dropped = current_dropped - prev_dropped
            
            # Simpan delta untuk dipakai di Flow Stats nanti
            if delta_dropped > 0:
                self.current_port_drops[dpid][port_no] = delta_dropped
            else:
                self.current_port_drops[dpid][port_no] = 0
            
            self.port_prev[dpid][port_no] = current_dropped

    # --- HANDLE FLOW STATS (Untuk Throughput) ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        self.flow_prev.setdefault(dpid, {})

        for stat in body:
            if stat.priority == 0: continue # Skip table-miss
            
            # Cari tahu flow ini output ke port mana (untuk mapping drops)
            out_port = None
            if stat.instructions:
                for inst in stat.instructions:
                    for action in inst.actions:
                        if action.type == ofproto_v1_3.OFPAT_OUTPUT:
                            out_port = action.port
                            break
            
            # Key unik flow
            match_str = str(stat.match)
            
            # Hitung Delta Throughput & Packets
            if match_str in self.flow_prev[dpid]:
                last_bytes, last_pkts, last_time = self.flow_prev[dpid][match_str]
                time_delta = now - last_time
                
                if time_delta > 0:
                    delta_bytes = stat.byte_count - last_bytes
                    delta_pkts = stat.packet_count - last_pkts
                    
                    throughput_bps = int((delta_bytes * 8) / time_delta)
                    packet_rate_pps = int(delta_pkts / time_delta)
                    
                    # Ambil Delta Drop dari Port terkait
                    dropped_val = 0
                    if out_port and out_port in self.current_port_drops.get(dpid, {}):
                        dropped_val = self.current_port_drops[dpid][out_port]

                    # Simpan ke DB (Hanya jika ada traffic)
                    if throughput_bps > 100:
                        # Kita simpan delta_bytes dan delta_pkts, BUKAN total
                        self.insert_stats(dpid, throughput_bps, packet_rate_pps, delta_bytes, delta_pkts, dropped_val)
            
            # Update State
            self.flow_prev[dpid][match_str] = (stat.byte_count, stat.packet_count, now)

    def insert_stats(self, dpid, bps, pps, delta_bytes, delta_pkts, drops):
        try:
            query = """
            INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s) 
            """
            # Byte count & Packet count sekarang isinya DELTA (per detik), bukan akumulasi
            self.cur.execute(query, (str(dpid), bps, pps, delta_bytes, delta_pkts, drops))
            self.conn.commit()
        except Exception as e:
            print(f"DB Error: {e}")
            self.conn.rollback()