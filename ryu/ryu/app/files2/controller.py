#!/usr/bin/env python3
"""
DEBUG VERSION: Ryu Real Monitor
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
import psycopg2
import time
import sys

# DATABASE CONFIG
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
        self.flow_prev = {} 
        self.port_prev = {} 
        self.current_port_drops = {} 
        self.monitor_thread = hub.spawn(self._monitor)
        
        # Initial DB Connection
        self.conn = None
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            print("✅ [INIT] Database Connected Successfully")
        except Exception as e:
            print(f"❌ [INIT] DB Connection Failed: {e}")
            sys.exit(1) # Stop jika DB gagal

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath
        # Install Table-Miss Flow (Allow Controller Communication)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        print(f"ℹ️  [SWITCH] Switch {datapath.id} Connected")

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # L2 Learning Switch Sederhana
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor(self):
        print("🚀 [MONITOR] Thread Started...")
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1) # Interval 1 detik

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        # Request Port Stats (untuk Drops)
        datapath.send_msg(parser.OFPPortStatsRequest(datapath))
        # Request Flow Stats (untuk Throughput)
        datapath.send_msg(parser.OFPFlowStatsRequest(datapath))

    # --- 1. PROSES DROPS DARI PORT ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        body = ev.msg.body
        
        self.port_prev.setdefault(dpid, {})
        self.current_port_drops.setdefault(dpid, {})

        for stat in body:
            port_no = stat.port_no
            current_dropped = stat.tx_dropped # Drop karena antrian penuh
            
            # Hitung Delta Drops
            prev = self.port_prev[dpid].get(port_no, 0)
            delta = current_dropped - prev
            
            if delta > 0:
                print(f"🔥 [DROP DETECTED] Switch: {dpid} Port: {port_no} Drops: {delta}")
                self.current_port_drops[dpid][port_no] = delta
            else:
                self.current_port_drops[dpid][port_no] = 0
            
            self.port_prev[dpid][port_no] = current_dropped

    # --- 2. PROSES THROUGHPUT DARI FLOW ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        self.flow_prev.setdefault(dpid, {})
        
        # Debug: Cek apakah flow stats diterima
        # print(f"DEBUG: Received stats from SW {dpid}, flows: {len(body)}")

        for stat in body:
            if stat.priority == 0: continue 
            
            # Cari Out Port untuk mapping drops
            out_port = None
            if stat.instructions:
                for inst in stat.instructions:
                    for action in inst.actions:
                        if action.type == ofproto_v1_3.OFPAT_OUTPUT:
                            out_port = action.port
                            break
            
            match_str = str(stat.match)
            
            # Hitung Delta
            if match_str in self.flow_prev[dpid]:
                last_bytes, last_pkts, last_time = self.flow_prev[dpid][match_str]
                time_delta = now - last_time
                
                if time_delta > 0:
                    delta_bytes = stat.byte_count - last_bytes
                    delta_pkts = stat.packet_count - last_pkts
                    
                    throughput_bps = int((delta_bytes * 8) / time_delta)
                    pps = int(delta_pkts / time_delta)
                    
                    # Ambil Drop info jika ada
                    dropped_val = 0
                    if out_port and out_port in self.current_port_drops.get(dpid, {}):
                        dropped_val = self.current_port_drops[dpid][out_port]

                    # Filter: Hanya insert jika ada traffic nyata (> 1kbps) ATAU ada drop
                    if throughput_bps > 1000 or dropped_val > 0:
                        self.insert_stats(dpid, throughput_bps, pps, delta_bytes, delta_pkts, dropped_val)
            
            # Update state
            self.flow_prev[dpid][match_str] = (stat.byte_count, stat.packet_count, now)

    def insert_stats(self, dpid, bps, pps, d_bytes, d_pkts, drops):
        if not self.conn: return
        try:
            query = """
            INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s) 
            """
            self.cur.execute(query, (str(dpid), bps, pps, d_bytes, d_pkts, drops))
            self.conn.commit()
            
            # Print sukses sesekali (setiap 100 packet atau jika ada drop)
            if drops > 0:
                print(f"💾 [DB SAVE] Congestion Data! Rate: {bps/1e6:.2f} Mbps | Drops: {drops}")
            elif d_pkts > 1000: # Print hanya kalau traffic agak besar supaya tidak spam log
                 print(f"💾 [DB SAVE] SW:{dpid} | {bps/1e6:.2f} Mbps | {pps} pps")
                 
        except Exception as e:
            print(f"❌ [DB ERROR] {e}")
            self.conn.rollback()
            # Coba reconnect jika connection closed
            if "closed" in str(e) or "terminat" in str(e):
                self.connect_db()