#!/usr/bin/env python3
# file: controller_real.py

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types
import psycopg2
from datetime import datetime

# --- CONFIG DB (JANGAN DIGANTI KALAU SUDAH BENAR) ---
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class RealTimeCollector(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(RealTimeCollector, self).__init__(*args, **kwargs)
        self.datapaths = {}
        # Polling setiap 1 detik agar resolusi data tinggi untuk ML
        self.monitor_thread = hub.spawn(self._monitor)
        self.conn = None
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.logger.info("✅ DATABASE TERHUBUNG (Ready to collect REAL metrics)")
        except Exception as e:
            self.logger.error(f"❌ DATABASE ERROR: {e}")

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1.0) # INTERVAL 1 DETIK

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        # Request Port Stats (Physical Interface) -> Wajib untuk deteksi Drop
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto_v1_3.OFPP_ANY)
        datapath.send_msg(req)

    # --- HANDLING KONEKSI SWITCH ---
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    # --- MENERIMA DATA STATISTIK DARI SWITCH ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = format(ev.msg.datapath.id, '016x')
        timestamp = datetime.now()

        if not self.conn or self.conn.closed: self.connect_db()

        try:
            cur = self.conn.cursor()
            for stat in body:
                # Filter port internal OVS (>1000) agar DB tidak penuh sampah
                if stat.port_no > 1000: continue

                # INSERT DATA MURNI APA ADANYA
                cur.execute("""
                    INSERT INTO traffic.port_stats_real
                    (timestamp, dpid, port_no, rx_packets, tx_packets, 
                     rx_bytes, tx_bytes, tx_dropped, rx_errors, duration_sec)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp, dpid, stat.port_no,
                    stat.rx_packets, stat.tx_packets,
                    stat.rx_bytes, stat.tx_bytes,
                    stat.tx_dropped, stat.rx_errors, stat.duration_sec
                ))
            self.conn.commit()
            cur.close()
        except Exception as e:
            self.logger.error(f"Save Error: {e}")
            self.conn.rollback()

    # --- PACKET FORWARDING SEDERHANA (Agar Ping/Traffic Jalan) ---
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
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

        # Flood Action (Simple L2 Switch)
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=msg.data)
        datapath.send_msg(out)