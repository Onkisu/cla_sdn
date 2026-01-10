#!/usr/bin/env python3
"""
HYBRID STABLE CONTROLLER
- Base: Kode lama Anda (Stabil & Konek DB)
- Fitur: Real Port Drops (Bukan 0 lagi)
- Fix: Data Delta (Tidak akumulatif)
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types
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
        # Cache untuk menyimpan Drop Packet terkini per DPID
        self.latest_drops = {} 
        # Cache untuk hitung delta throughput
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
        self.datapaths[datapath.id] = datapath
        self.latest_drops[datapath.id] = 0 # Init drop counter
        
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
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
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        if eth.ethertype == ether_types.ETH_TYPE_LLDP: return

        # Simple Flood
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
        # Request keduanya: Port (untuk Drop) dan Flow (untuk Traffic)
        datapath.send_msg(parser.OFPPortStatsRequest(datapath))
        datapath.send_msg(parser.OFPFlowStatsRequest(datapath))

    # --- 1. TANGKAP REAL DROP (PORT STATS) ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        total_drops_now = 0
        for stat in body:
            # tx_dropped adalah indikator congestion fisik di switch
            total_drops_now += stat.tx_dropped

        # Kita simpan total drops switch ini ke memori global
        # Nanti Flow Stats yang akan mengambil nilainya
        self.latest_drops[dpid] = total_drops_now

    # --- 2. TANGKAP TRAFFIC & GABUNGKAN DATA ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()
        
        # Ambil data drop terbaru dari cache (Safe handling)
        current_switch_drop_total = self.latest_drops.get(dpid, 0)

        for stat in body:
            if stat.priority == 0: continue 

            match_str = str(stat.match)
            prev_key = (dpid, match_str)

            byte_count = stat.byte_count
            packet_count = stat.packet_count

            if prev_key in self.prev_stats:
                last_bytes, last_pkts, last_time, last_switch_drop = self.prev_stats[prev_key]
                time_delta = now - last_time
                
                if time_delta > 0:
                    # Hitung Delta (Selisih)
                    delta_bytes = byte_count - last_bytes
                    delta_pkts = packet_count - last_pkts
                    
                    # Hitung Delta Drop (Drop sekarang - Drop saat flow ini terakhir dicek)
                    delta_drop = current_switch_drop_total - last_switch_drop
                    if delta_drop < 0: delta_drop = 0 # Handle restart

                    throughput_bps = int((delta_bytes * 8) / time_delta)
                    pps = int(delta_pkts / time_delta)

                    # Simpan ke DB jika ada aktivitas
                    # Kita pakai delta_bytes supaya data di DB tidak kumulatif "Nambah Terus"
                    if throughput_bps > 500 or delta_drop > 0:
                        self.insert_stats(dpid, throughput_bps, pps, delta_bytes, delta_pkts, delta_drop)

            # Update state (termasuk drop terakhir yang kita lihat)
            self.prev_stats[prev_key] = (byte_count, packet_count, now, current_switch_drop_total)

    def insert_stats(self, dpid, bps, pps, d_bytes, d_pkts, d_drop):
        if not self.conn: return
        try:
            # Pastikan tabel Anda punya kolom dropped_count!
            query = """
            INSERT INTO traffic.flow_stats_real 
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s) 
            """
            self.cur.execute(query, (str(dpid), bps, pps, d_bytes, d_pkts, d_drop))
            self.conn.commit()
            
            if d_drop > 0:
                self.logger.info(f"🚨 CONGESTION SAVED: {d_drop} packets dropped!")
                
        except Exception as e:
            self.logger.error(f"DB Error: {e}")
            self.conn.rollback()
            # Reconnect logic simple
            if self.conn.closed:
                self.connect_db()