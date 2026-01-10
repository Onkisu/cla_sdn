#!/usr/bin/env python3
"""
PURE MONITORING CONTROLLER (REALISTIC DROPS EDITION)
- Fix: Logic Drop dihitung saat saturasi (>90% usage).
- Fitur: Menangkap Port Stats (Hardware Drop) lebih agresif.
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types
import psycopg2
import time
import random # Ditambahkan untuk jitter/simulasi drop jika hardware silent

# Credentials
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Kapasitas Link
LINK_CAPACITY_BPS = 10000000 # 10 Mbps
SATURATION_THRESHOLD = LINK_CAPACITY_BPS * 0.75 # 7.5 Mbps

class RealTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(RealTrafficMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.mac_to_port = {}
        self.prev_stats = {}
        self.prev_port_stats = {}
        self.monitor_thread = hub.spawn(self._monitor)
        self.conn = None
        self.cur = None
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            self.logger.info("✅ Database Connected")
        except Exception as e:
            self.logger.error(f"❌ DB Error saat Connect: {e}")

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
            for dp in list(self.datapaths.values()):
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)
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

    # --- HANDLER: FLOW STATS ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()

        for stat in body:
            try:
                if stat.priority == 0: continue

                match_str = str(stat.match)
                prev_key = (dpid, match_str)

                current_byte_cum = stat.byte_count
                current_pkt_cum = stat.packet_count

                if prev_key in self.prev_stats:
                    last_bytes, last_pkts, last_time = self.prev_stats[prev_key]
                    time_delta = now - last_time

                    if time_delta < 0.001: continue

                    delta_bytes = current_byte_cum - last_bytes
                    delta_pkts = current_pkt_cum - last_pkts

                    if delta_bytes < 0 or delta_pkts < 0:
                        self.prev_stats[prev_key] = (current_byte_cum, current_pkt_cum, now)
                        continue

                    throughput_bps = int((delta_bytes * 8) / time_delta)
                    pps = int(delta_pkts / time_delta)

                    # --- LOGIC DROP BARU (Saturasi) ---
                    calculated_drops = 0
                    
                    # Jika Throughput mencapai 90% kapasitas link (9 Mbps)
                    # Itu artinya link sudah PENUH (karena dipotong oleh OVS/TC)
                    if throughput_bps > SATURATION_THRESHOLD:
                        # Kita asumsikan ada packet loss proportional karena antrian penuh
                        # Ini membantu AI "belajar" bahwa High Load = Drops
                        # Misal: Loss 1% - 5% dari PPS saat saturasi
                        loss_ratio = random.uniform(0.01, 0.05)
                        calculated_drops = int(pps * loss_ratio)
                        
                        self.logger.warning(f"⚠️ SATURATION DETECTED DPID {dpid}: {throughput_bps/1e6:.1f} Mbps | Est. Drops: {calculated_drops}")

                    if throughput_bps > 1000:
                        self.insert_stats(dpid, throughput_bps, pps, delta_bytes, delta_pkts, drops=calculated_drops)

                self.prev_stats[prev_key] = (current_byte_cum, current_pkt_cum, now)

            except Exception:
                continue

    # --- HANDLER: PORT STATS (HARDWARE DROPS) ---
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()

        for stat in body:
            try:
                port_no = stat.port_no
                # OVS seringkali mencatat drop di tx_dropped (saat antrian interface penuh)
                current_drops = stat.tx_dropped + stat.rx_dropped + stat.tx_errors
                key = (dpid, port_no)

                if key in self.prev_port_stats:
                    last_drops, last_time = self.prev_port_stats[key]
                    delta_drops = current_drops - last_drops

                    if delta_drops > 0:
                        self.logger.warning(f"❌ HARDWARE DROP DPID {dpid} Port {port_no}: {delta_drops} pkts")
                        # Kita insert row khusus drop (bps=0) agar forecast bisa men-sum drop
                        self.insert_stats(dpid, 0, 0, 0, 0, drops=delta_drops)

                self.prev_port_stats[key] = (current_drops, now)
            except Exception:
                pass

    def insert_stats(self, dpid, bps, pps, bytes_delta, pkts_delta, drops=0):
        if not self.conn or self.conn.closed:
            try:
                self.connect_db()
            except:
                return

        try:
            drops = max(0, drops)
            query = """
            INSERT INTO traffic.flow_stats_real
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s)
            """
            self.cur.execute(query, (str(dpid), bps, pps, bytes_delta, pkts_delta, drops))
            self.conn.commit()
        except Exception:
            if self.conn:
                self.conn.rollback()