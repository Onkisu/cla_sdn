#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FIXED SAPU JAGAT)
- Fix: Explicit Revert Logic (No implicit Packet-In dependence)
- Fix: Traffic Doubling preventions (Strict Flow Cleanup)
- Fix: Explicit Priority Management (Normal=100, Reroute=200)
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
from ryu.topology import event, api
from ryu.topology.api import get_switch, get_link
import networkx as nx
import psycopg2
from datetime import datetime
import random
import math
import time

# PostgreSQL Configuration
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Threshold Congestion
BURST_THRESHOLD_BPS = 120000 
LOWER_THRESHOLD_BPS = 90000 # Hysteresis bawah
COOLDOWN_PERIOD = 30 

# --- CONSTANT PORTS & PRIORITY ---
# Asumsi Topologi Mininet Spine-Leaf Default:
# Leaf 1 (DPID 4) Ports: 1->Spine1, 2->Spine2, 3->Spine3, 4->Host1
PORT_TO_SPINE_1 = 1 # Jalur Reroute (Alternatif)
PORT_TO_SPINE_2 = 2 # (Jarang dipakai di skenario ini)
PORT_TO_SPINE_3 = 3 # Jalur Default (Rawan Tabrakan)

PRIO_NORMAL = 100
PRIO_REROUTE = 200
PRIO_DEFAULT_MAC = 1

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph()
        self.last_bytes = {}
        
        # --- STATE ---
        self.congestion_active = False 
        self.last_congestion_time = 0
        self.current_reroute_path = None
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("✅ VoIP Smart Controller (FIXED SAPU JAGAT) Started")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}")
            self.db_conn = None
    
    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception:
            return None

    # =================================================================
    # 1. TOPOLOGY DISCOVERY
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            self.net.clear()
            switches = get_switch(self, None)
            for s in switches: self.net.add_node(s.dp.id)
            links = get_link(self, None)
            for l in links:
                self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    # =================================================================
    # 2. LOGIC REROUTE & REVERT (THE FIX)
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                result = cur.fetchone()
                cur.close()
                conn.close()

                if not result: continue
                pred_bps = result[0]
                now = time.time()

                # --- LOGIC TRIGGER ---
                # 1. CONGESTION DETECTED -> REROUTE
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"🚀 TRIGGER REROUTE (Pred: {pred_bps:.0f} bps)")
                    self.apply_reroute_logic(pred_bps)
                    
                # 2. TRAFFIC NORMAL -> REVERT
                elif self.congestion_active:
                    delta_t = now - self.last_congestion_time
                    if pred_bps < LOWER_THRESHOLD_BPS and delta_t > COOLDOWN_PERIOD:
                        self.logger.info(f"🔄 TRIGGER REVERT (Pred: {pred_bps:.0f} bps, Stable: {int(delta_t)}s)")
                        self.revert_logic()
                    
            except Exception as e:
                self.logger.error(f"Forecast Error: {e}")
                if conn: conn.close()

    def apply_reroute_logic(self, trigger_val):
        """Pindah H1 ke Spine 1 (Port 1 di Leaf 1)"""
        dp_src = self.datapaths.get(4) # Leaf 1 (H1 location)
        if not dp_src: return

        # 1. BERSIHKAN FLOW LAMA (PENTING AGAR TIDAK DOUBLE)
        self.clean_h1_flows_global()

        # 2. PASANG FLOW REROUTE (Priority TINGGI: 200)
        # H1 -> H2 lewat Port 1 (Spine 1)
        parser = dp_src.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_1)] # Ke Spine 1
        
        self.add_flow(dp_src, PRIO_REROUTE, match, actions)
        
        # Update State
        self.congestion_active = True
        self.last_congestion_time = time.time()
        self.insert_event_log("REROUTE_ACTIVE", "H1 moved to Spine 1 (High Prio)", trigger_val)
        self.logger.info("✅ REROUTE APPLIED: H1 -> Spine 1 (DPID 4 Port 1)")

    def revert_logic(self):
        """Kembali H1 ke Spine 3 (Port 3 di Leaf 1)"""
        dp_src = self.datapaths.get(4)
        if not dp_src: return

        # 1. BERSIHKAN FLOW REROUTE (PENTING AGAR TIDAK KONFLIK)
        self.clean_h1_flows_global()

        # 2. PASANG FLOW DEFAULT (Priority NORMAL: 100)
        # H1 -> H2 lewat Port 3 (Spine 3 - Default)
        # Kita pasang Explicitly agar tidak perlu Packet-In
        parser = dp_src.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_3)] # Ke Spine 3
        
        self.add_flow(dp_src, PRIO_NORMAL, match, actions)
        
        # Update State
        self.congestion_active = False
        self.insert_event_log("REROUTE_REVERT", "H1 returned to Spine 3 (Normal Prio)", 0)
        self.logger.info("✅ REVERT APPLIED: H1 -> Spine 3 (DPID 4 Port 3)")

    def clean_h1_flows_global(self):
        """SAPU JAGAT: Hapus flow H1->H2 di SEMUA switch"""
        self.logger.info("🧹 Cleaning H1->H2 flows on ALL switches...")
        for dpid, dp in self.datapaths.items():
            ofp = dp.ofproto
            parser = dp.ofproto_parser
            
            # Match spesifik H1->H2
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            
            # Hapus semua flow yang cocok (Strict or not, kita pakai non-strict agar kena semua priority)
            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofp.OFPFC_DELETE, # Delete all matches regardless of priority
                out_port=ofp.OFPP_ANY,
                out_group=ofp.OFPG_ANY,
                match=match
            )
            dp.send_msg(mod)
            
            # Barrier Request: Pastikan switch selesai menghapus sebelum lanjut
            self.send_barrier_request(dp)
            
    def send_barrier_request(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPBarrierRequest(datapath)
        datapath.send_msg(req)

    def insert_event_log(self, event_type, desc, val=0):
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, desc, val))
            conn.commit()
            conn.close()
        except: pass

    # =================================================================
    # 3. OPENFLOW HANDLERS
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                # Saat switch connect, pasang default rule H1->H2 (Spine 3) dengan Priority 100
                if datapath.id == 4: # Only on Leaf 1
                    self.revert_logic() # Init default route
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority, 
                                    match=match, instructions=inst, idle_timeout=idle_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, 
                                    match=match, instructions=inst, idle_timeout=idle_timeout)
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
        if eth.ethertype == ether_types.ETH_TYPE_IPV6: return

        dst = eth.dst
        src = eth.src
        
        # Learn MAC
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # --- CRITICAL FIX FOR H1->H2 ---
            # Jika paket adalah H1->H2 dan sampai ke Controller,
            # Berarti flow statis belum terpasang atau expired.
            # Kita pasang flow "Normal" (Priority 100) sebagai fallback.
            # JANGAN pasang flow priority 1 di sini untuk H1->H2, 
            # karena akan menimpa logic reroute jika tidak hati-hati.
            
            if dpid == 4 and src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                # Cek mode
                out_p = PORT_TO_SPINE_1 if self.congestion_active else PORT_TO_SPINE_3
                prio = PRIO_REROUTE if self.congestion_active else PRIO_NORMAL
                
                actions = [parser.OFPActionOutput(out_p)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                self.add_flow(datapath, prio, match, actions)
                
                # Kirim packet out
                data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                          in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return

        # --- STANDARD SWITCHING (FLOODING/MAC LEARNING) ---
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install Flow (Priority rendah = 1)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, PRIO_DEFAULT_MAC, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 4. MONITORING
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()

        for stat in body:
            if stat.priority == 0: continue # Skip table-miss flow

            match = stat.match
            src_ip = match.get('ipv4_src')
            dst_ip = match.get('ipv4_dst')

            # Filter hanya trafik menarik
            if dst_ip != '10.0.0.2': continue
            if src_ip not in ['10.0.0.1', '10.0.0.3']: continue

            # Calc Delta
            flow_key = f"{dpid}-{src_ip}-{dst_ip}"
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            delta_b = max(0, byte_count - prev_b)
            delta_p = max(0, packet_count - prev_p)
            self.last_bytes[flow_key] = (byte_count, packet_count)

            if delta_b <= 0: continue

            # Insert DB
            conn = self.get_db_conn()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO traffic.flow_stats_
                        (timestamp, dpid, src_ip, dst_ip, bytes_tx, pkts_tx, traffic_label)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (timestamp, dpid, src_ip, dst_ip, delta_b, delta_p, 
                          'voip' if src_ip == '10.0.0.1' else 'bursty'))
                    conn.commit()
                    conn.close()
                except: pass