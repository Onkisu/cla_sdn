#!/usr/bin/env python3
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
import psycopg2
import time
import os
import sys

# CONFIG
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# HARDCODED INTERFACE TARGET
# Kita cuma peduli sama link yang macet: l1-eth3
TARGET_INTERFACE = "l1-eth3" 
TARGET_DPID = 2

class FixedController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(FixedController, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.prev_stats = {}      
        self.prev_kernel_drops = 0
        self.monitor_thread = hub.spawn(self._monitor)
        self.conn = None
        self.cur = None
        
        print("\n🚀 CONTROLLER STARTED. INITIATING SYSTEMS...")
        self.connect_db()
        self.check_file_access()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            print("✅ DATABASE: CONNECTED")
        except Exception as e:
            print(f"❌ DATABASE: FAILED ({e})")

    def check_file_access(self):
        path = f"/sys/class/net/{TARGET_INTERFACE}/statistics/tx_dropped"
        # Kita tunggu file ini muncul (karena Mininet mungkin baru start belakangan)
        print(f"🔍 Waiting for interface {TARGET_INTERFACE}...")

    def get_real_drops(self):
        # BACA LANGSUNG DARI FILE KERNEL
        path = f"/sys/class/net/{TARGET_INTERFACE}/statistics/tx_dropped"
        try:
            with open(path, 'r') as f:
                return int(f.read().strip())
        except:
            return 0 # Kalau file belum ada (mininet belum start), return 0

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        
        # Install Table-Miss Flow
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=0, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor(self):
        while True:
            for dp in list(self.datapaths.values()):
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

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
        
        # AGGREGATE THROUGHPUT
        total_bytes = 0
        total_pkts = 0
        for stat in body:
            if stat.priority == 0: continue
            total_bytes += stat.byte_count
            total_pkts += stat.packet_count

        key = dpid
        if key in self.prev_stats:
            last_bytes, last_pkts, last_time = self.prev_stats[key]
            time_delta = now - last_time
            
            if time_delta >= 0.5:
                # HITUNG SPEED
                delta_bytes = total_bytes - last_bytes
                delta_pkts = total_pkts - last_pkts
                bps = int((delta_bytes * 8) / time_delta)
                pps = int(delta_pkts / time_delta)
                if bps > 2000000000: bps = 2000000000 # Sanity check

                # HITUNG DROPS (KHUSUS SWITCH 2 / l1)
                delta_drop = 0
                if dpid == TARGET_DPID:
                    curr_drop = self.get_real_drops()
                    delta_drop = curr_drop - self.prev_kernel_drops
                    
                    if delta_drop > 0:
                        print(f"🔥 KERNEL DROP DETECTED: {delta_drop} packets!")
                    
                    self.prev_kernel_drops = curr_drop

                # INSERT DB
                if bps > 1000 or delta_drop > 0:
                    self.insert_db(dpid, bps, pps, delta_bytes, delta_pkts, delta_drop)

                self.prev_stats[key] = (total_bytes, total_pkts, now)
        else:
            self.prev_stats[key] = (total_bytes, total_pkts, now)
            if dpid == TARGET_DPID:
                self.prev_kernel_drops = self.get_real_drops()

    def insert_db(self, dpid, bps, pps, b_delta, p_delta, drops):
        if not self.conn or self.conn.closed:
            self.connect_db()
        try:
            drops = max(0, drops)
            query = """INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count) 
            VALUES (NOW(), %s, %s, %s, %s, %s, %s)"""
            self.cur.execute(query, (str(dpid), bps, pps, b_delta, p_delta, drops))
            self.conn.commit()
            # Feedback visual biar lu tau dia kerja
            sys.stdout.write(".")
            sys.stdout.flush()
        except Exception:
            if self.conn: self.conn.rollback()