#!/usr/bin/env python3
"""
SIMPLE & ROBUST MONITOR
- Menangkap Traffic & Drops
- Tanpa filter aneh-aneh (Pokoknya simpan)
- Auto-Reconnect Database
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
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

class SimpleRealMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleRealMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        # Penyimpanan state sebelumnya
        self.flow_prev = {} 
        self.port_prev = {} 
        self.port_drops_cache = {} # Cache drops per switch/port
        
        self.conn = None
        self.cur = None
        
        # Connect DB di awal
        self.connect_db()
        
        # Thread Monitor
        self.monitor_thread = hub.spawn(self._monitor)

    def connect_db(self):
        try:
            if self.conn: self.conn.close()
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            print("✅ [DB] Connected Successfully!")
        except Exception as e:
            print(f"❌ [DB ERROR] {e}")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath
        
        # Install Table-Miss (Supaya Ping jalan)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        print(f"ℹ️  [SWITCH] SW:{datapath.id} Ready")

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # Logic L2 Switch sederhana
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor(self):
        print("🚀 [MONITOR] Thread Started...")
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        # Request Port & Flow Stats
        datapath.send_msg(parser.OFPPortStatsRequest(datapath))
        datapath.send_msg(parser.OFPFlowStatsRequest(datapath))

    # --- 1. AMBIL DATA DROP DARI PORT ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        body = ev.msg.body
        
        self.port_prev.setdefault(dpid, {})
        self.port_drops_cache.setdefault(dpid, {})

        for stat in body:
            port_no = stat.port_no
            current_dropped = stat.tx_dropped # Ambil data drop real
            
            # Hitung selisih (Delta)
            prev = self.port_prev[dpid].get(port_no, 0)
            
            # Jika Mininet direstart, counter jadi 0 lagi, kita reset
            if current_dropped < prev:
                prev = 0
            
            delta = current_dropped - prev
            
            # Simpan ke cache untuk dipakai Flow Stats
            if delta > 0:
                self.port_drops_cache[dpid][port_no] = delta
                print(f"🔥 [DROP DETECTED] SW:{dpid} Port:{port_no} Drop:{delta}")
            else:
                self.port_drops_cache[dpid][port_no] = 0
                
            self.port_prev[dpid][port_no] = current_dropped

    # --- 2. AMBIL DATA THROUGHPUT DARI FLOW ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        self.flow_prev.setdefault(dpid, {})

        for stat in body:
            if stat.priority == 0: continue # Skip table-miss
            
            match_str = str(stat.match)
            
            # Cari flow ini output ke port mana (untuk mapping drops)
            out_port = None
            if stat.instructions:
                for inst in stat.instructions:
                    for action in inst.actions:
                        if action.type == ofproto_v1_3.OFPAT_OUTPUT:
                            out_port = action.port
                            break
            
            if match_str in self.flow_prev[dpid]:
                last_bytes, last_pkts, last_time = self.flow_prev[dpid][match_str]
                time_delta = now - last_time
                
                # Pastikan time_delta valid
                if time_delta >= 0.9: 
                    delta_bytes = stat.byte_count - last_bytes
                    delta_pkts = stat.packet_count - last_pkts
                    
                    # Handle restart Mininet (Counter reset)
                    if delta_bytes < 0: delta_bytes = 0
                    if delta_pkts < 0: delta_pkts = 0
                    
                    # Hitung BPS/PPS
                    bps = int((delta_bytes * 8) / time_delta)
                    pps = int(delta_pkts / time_delta)
                    
                    # Ambil Drop Cache
                    drops = 0
                    if out_port and out_port in self.port_drops_cache.get(dpid, {}):
                        drops = self.port_drops_cache[dpid][out_port]

                    # SIMPAN KE DB (Tanpa Filter Berlebihan)
                    # Hanya skip jika benar-benar 0 traffic DAN 0 drop
                    if bps > 0 or drops > 0:
                        self.insert_stats(dpid, bps, pps, delta_bytes, delta_pkts, drops)
            
            # Update state
            self.flow_prev[dpid][match_str] = (stat.byte_count, stat.packet_count, now)

    def insert_stats(self, dpid, bps, pps, d_bytes, d_pkts, drops):
        # Cek koneksi DB
        if not self.conn or self.conn.closed:
            print("⚠️ DB Connection Lost. Reconnecting...")
            self.connect_db()
            if not self.conn: return

        try:
            # Pastikan tabel sesuai!
            query = """
            INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s) 
            """
            self.cur.execute(query, (str(dpid), bps, pps, d_bytes, d_pkts, drops))
            self.conn.commit()
            
            # LOGGING AGAR JELAS
            if drops > 0:
                print(f"💾 [SAVED] CONGESTION! SW:{dpid} Rate:{bps/1e6:.1f}M Drop:{drops}")
            elif bps > 500000: # Print kalau traffic > 0.5 Mbps biar log gak penuh sampah
                print(f"💾 [SAVED] SW:{dpid} Rate:{bps/1e6:.1f} Mbps")
                
        except Exception as e:
            print(f"❌ [INSERT FAILED] {e}")
            self.conn.rollback() # Wajib rollback kalau error