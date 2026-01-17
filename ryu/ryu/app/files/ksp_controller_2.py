#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FIXED SAPU JAGAT)
- Fix: Explicit Revert Logic (No implicit Packet-In dependence)
- Fix: Traffic Doubling preventions (Barrier Request + Strict Cleanup)
- Fix: Explicit Priority Management (Normal=100, Reroute=200)
- Features: All DB Logging and Forecasting retained.
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

# Threshold Congestion (Sesuai forecast_2.py)
BURST_THRESHOLD_BPS = 120000 
LOWER_THRESHOLD_BPS = 90000 # Hysteresis agar tidak flip-flop
COOLDOWN_PERIOD = 15        # Detik menunggu sebelum revert diizinkan

# --- CONSTANT PORTS & PRIORITY ---
# Asumsi Topologi Mininet Spine-Leaf:
# Leaf 1 (DPID 4) Ports: 1->Spine1, 2->Spine2, 3->Spine3, 4->Host1
PORT_TO_SPINE_1 = 1 # Jalur Reroute (Alternatif)
PORT_TO_SPINE_3 = 3 # Jalur Default (Rawan Tabrakan)

PRIO_REROUTE = 200    # Prioritas Tertinggi
PRIO_NORMAL = 100     # Prioritas Menengah (Default Path H1->H2)
PRIO_DEFAULT_MAC = 1  # Prioritas Terendah (Learning Switch)

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.net = nx.DiGraph()
        self.last_bytes = {}
        
        # --- STATE ---
        self.congestion_active = False 
        self.last_state_change_time = 0
        
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
                # Ambil prediksi terbaru
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                result = cur.fetchone()
                cur.close()
                conn.close()

                if not result: continue
                pred_bps = result[0]
                now = time.time()
                time_since_last_change = now - self.last_state_change_time

                # --- LOGIC TRIGGER ---
                
                # CASE 1: PREDIKSI TINGGI -> REROUTE
                if pred_bps > BURST_THRESHOLD_BPS:
                    if not self.congestion_active:
                        self.logger.warning(f"🚀 TRIGGER REROUTE (Pred: {pred_bps:.0f} bps)")
                        self.apply_reroute_logic(pred_bps)

                # CASE 2: PREDIKSI RENDAH -> REVERT
                # (Pakai Hysteresis & Cooldown agar tidak flip-flop)
                elif pred_bps < LOWER_THRESHOLD_BPS:
                    if self.congestion_active and time_since_last_change > COOLDOWN_PERIOD:
                        self.logger.info(f"🔄 TRIGGER REVERT (Pred: {pred_bps:.0f} bps)")
                        self.revert_logic()
                    
            except Exception as e:
                self.logger.error(f"Forecast Logic Error: {e}")
                if conn: conn.close()

    def clean_h1_flows_strict(self, datapath):
        """
        SAPU JAGAT: Menghapus flow spesifik H1->H2 dengan Barrier Request.
        Ini memastikan switch BERSIH sebelum dipasang rule baru.
        Mencegah Traffic Doubling.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Match spesifik H1->H2
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        
        # Delete flow (Strict off, agar kena priority berapapun)
        mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            match=match
        )
        datapath.send_msg(mod)
        
        # KIRIM BARRIER: Tunggu switch selesai menghapus baru lanjut
        # Ini kunci agar trafik tidak dobel.
        barrier = parser.OFPBarrierRequest(datapath)
        datapath.send_msg(barrier)

    def apply_reroute_logic(self, trigger_val):
        """Pindah H1 ke Spine 1 (Port 1 di Leaf 1)"""
        # Target: Leaf 1 (DPID 4) tempat H1 berada
        dp_src = self.datapaths.get(4) 
        if not dp_src: return

        # 1. BERSIHKAN DULU (PENTING!)
        self.clean_h1_flows_strict(dp_src)

        # 2. PASANG FLOW REROUTE (Priority 200)
        # H1 -> H2 lewat Port 1 (Spine 1)
        parser = dp_src.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_1)] # Ke Spine 1
        
        self.add_flow(dp_src, PRIO_REROUTE, match, actions)
        
        # Update State
        self.congestion_active = True
        self.last_state_change_time = time.time()
        
        self.insert_event_log("REROUTE_ACTIVE", "H1 moved to Spine 1 (High Prio)", trigger_val)
        self.logger.info("✅ REROUTE APPLIED: H1 -> Spine 1 (DPID 4 Port 1)")

    def revert_logic(self):
        """Kembali H1 ke Spine 3 (Port 3 di Leaf 1)"""
        dp_src = self.datapaths.get(4)
        if not dp_src: return

        # 1. BERSIHKAN FLOW REROUTE (PENTING!)
        self.clean_h1_flows_strict(dp_src)

        # 2. PASANG FLOW DEFAULT (Priority 100)
        # BEDA UTAMA: Kita pasang EXPLICITLY, tidak menunggu Packet-In
        parser = dp_src.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_3)] # Ke Spine 3 (Default)
        
        self.add_flow(dp_src, PRIO_NORMAL, match, actions)
        
        # Update State
        self.congestion_active = False
        self.last_state_change_time = time.time()
        
        self.insert_event_log("REROUTE_REVERT", "H1 returned to Spine 3 (Normal Prio)", 0)
        self.logger.info("✅ REVERT APPLIED: H1 -> Spine 3 (DPID 4 Port 3)")

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
                # Saat switch connect, init jalur default untuk H1->H2 di Leaf 1
                if datapath.id == 4: 
                    self.revert_logic() 
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
        
        # Filter protocol standard
        if eth.ethertype == ether_types.ETH_TYPE_LLDP: return
        if eth.ethertype == ether_types.ETH_TYPE_IPV6: return

        dst = eth.dst
        src = eth.src
        
        # Learn MAC Location
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        # --- LOGIC HANDLING KHUSUS H1->H2 ---
        # Ini untuk menangani kasus jika flow statis terhapus atau belum terpasang.
        # Kita force pasang jalur yang benar sesuai status congestion saat ini.
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            if dpid == 4 and src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                # Tentukan Output berdasarkan State Controller
                out_p = PORT_TO_SPINE_1 if self.congestion_active else PORT_TO_SPINE_3
                prio = PRIO_REROUTE if self.congestion_active else PRIO_NORMAL
                
                actions = [parser.OFPActionOutput(out_p)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                
                # Pasang Flow Permanen (idle_timeout=0)
                self.add_flow(datapath, prio, match, actions)
                
                # Forward paket ini agar tidak drop
                data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                          in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return

        # --- STANDARD SWITCHING (Fallback) ---
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install Flow Biasa (Priority 1) dengan Timeout
        # Agar tidak menumpuk sampah flow
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
            if stat.priority == 0: continue 

            match = stat.match
            src_ip = match.get('ipv4_src')
            dst_ip = match.get('ipv4_dst')

            # Filter hanya trafik menarik untuk DB
            if dst_ip != '10.0.0.2': continue
            if src_ip not in ['10.0.0.1', '10.0.0.3']: continue

            flow_key = f"{dpid}-{src_ip}-{dst_ip}"
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            
            # Hitung delta
            delta_b = byte_count - prev_b
            delta_p = packet_count - prev_p
            
            # Update cache
            self.last_bytes[flow_key] = (byte_count, packet_count)

            # Skip jika stats baru direset atau tidak ada trafik
            if delta_b <= 0: continue

            # Insert DB (Fitur yg harus dipertahankan)
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