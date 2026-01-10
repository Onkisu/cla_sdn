#!/usr/bin/env python3
"""
RYU CONTROLLER - SPINE LEAF MONITOR
- Connects to DB automatically.
- Monitors ALL switches (Spines & Leaves).
- Captures Dropped Packets from Port Stats.
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
import psycopg2
import time

# DB CONFIG
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class SpineLeafMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SpineLeafMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.prev_stats = {} 
        self.latest_drops = {}
        self.conn = None
        self.cur = None
        
        self.connect_db()
        self.monitor_thread = hub.spawn(self._monitor)

    def connect_db(self):
        try:
            if self.conn: self.conn.close()
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            self.logger.info("✅ DATABASE CONNECTED")
        except Exception as e:
            self.logger.error(f"❌ DB CONNECTION FAILED: {e}")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath
        self.latest_drops[datapath.id] = 0
        
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        
        # Install Table Miss
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=0, match=match, instructions=inst)
        datapath.send_msg(mod)
        self.logger.info(f"ℹ️  SWITCH {datapath.id} REGISTERED")

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
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
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        datapath.send_msg(parser.OFPPortStatsRequest(datapath))
        datapath.send_msg(parser.OFPFlowStatsRequest(datapath))

    # --- PORT STATS (Untuk DROP Packet) ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        # Sum semua drops di semua port pada switch ini
        total_drops = sum(stat.tx_dropped for stat in ev.msg.body)
        self.latest_drops[dpid] = total_drops

    # --- FLOW STATS (Untuk Throughput) ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        current_drops = self.latest_drops.get(dpid, 0)

        for stat in body:
            if stat.priority == 0: continue

            key = (dpid, str(stat.match))
            if key in self.prev_stats:
                last_b, last_p, last_t, last_d = self.prev_stats[key]
                dt = now - last_t
                
                # Filter noise interval
                if dt >= 0.9: 
                    d_bytes = stat.byte_count - last_b
                    d_pkts = stat.packet_count - last_p
                    d_drop = current_drops - last_d
                    
                    if d_bytes < 0: d_bytes = 0
                    if d_drop < 0: d_drop = 0
                    
                    bps = int((d_bytes * 8) / dt)
                    pps = int(d_pkts / dt)
                    
                    # LOGIC SIMPAN:
                    # Simpan jika ada traffic signigikan ATAU ada drop
                    if bps > 1000 or d_drop > 0:
                        self.save_data(dpid, bps, pps, d_bytes, d_pkts, d_drop)

            self.prev_stats[key] = (stat.byte_count, stat.packet_count, now, current_drops)

    def save_data(self, dpid, bps, pps, bytes_d, pkts_d, drops):
        if not self.conn or self.conn.closed:
            self.connect_db()
            if not self.conn: return

        try:
            sql = """
                INSERT INTO traffic.flow_stats_real 
                (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
                VALUES (NOW(), %s, %s, %s, %s, %s, %s)
            """
            self.cur.execute(sql, (str(dpid), bps, pps, bytes_d, pkts_d, drops))
            self.conn.commit()
            
            if drops > 0:
                self.logger.warning(f"🔥 SW:{dpid} CONGESTION! Drops: {drops}")
            elif bps > 500000:
                self.logger.info(f"💾 SW:{dpid} Saved: {bps/1e6:.2f} Mbps")
                
        except Exception as e:
            self.logger.error(f"DB Insert Failed: {e}")
            self.conn.rollback()