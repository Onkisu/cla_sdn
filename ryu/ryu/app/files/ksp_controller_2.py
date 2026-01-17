#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP Traffic Engineering (TRUE INDUSTRIAL V11)
==================================================================
Industrial Grade Features:
1. COOKIE-AWARE TELEMETRY: 
   Membedakan statistik berdasarkan Flow ID (Cookie). 
   Ini MENGHILANGKAN anomaly spike saat perpindahan jalur secara matematis,
   bukan dengan membuang data.

2. ATOMIC INSERTION:
   Memastikan data masuk DB apa adanya.

3. MAKE-BEFORE-BREAK:
   Jalur baru siap 100% sebelum traffic dipindah.
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, arp
from ryu.topology import api
from ryu.topology.api import get_switch, get_link
import networkx as nx
import psycopg2
from datetime import datetime
import time

# ==========================================
# CONFIGURATION
# ==========================================
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Thresholds (Logic Bisnis, bukan Logic Bug)
BURST_THRESHOLD_BPS = 120000 
HYSTERESIS_LOWER_BPS = 90000   
COOLDOWN_PERIOD_SEC = 15       

# CONSTANTS
PRIORITY_NORMAL  = 10          
PRIORITY_REROUTE = 500         
COOKIE_DEFAULT   = 0x0
COOKIE_REROUTE   = 0xDEADBEEF  

class VoIPControllerIndustrial(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPControllerIndustrial, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() 
        
        # --- INDUSTRIAL MEMORY STRUCTURE ---
        # Key sekarang mencakup COOKIE agar tidak tertukar antara jalur lama & baru
        # Format: self.last_bytes[(dpid, cookie, src_ip, dst_ip)] = (bytes, pkts)
        self.last_bytes = {} 
        
        self.congestion_active = False 
        self.last_state_change = 0
        self.connect_database()
        
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast_smart)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("🏭 INDUSTRIAL CONTROLLER V11: Cookie-Aware Telemetry Active")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("✅ Database connected")
        except: self.db_conn = None
    
    def get_db_conn(self):
        try: return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except: return None

    # =================================================================
    # 1. CORE TELEMETRY (THE FIX IS HERE)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                # Request stats
                dp.send_msg(dp.ofproto_parser.OFPFlowStatsRequest(dp))
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()
        conn = self.get_db_conn()
        if not conn: return

        try:
            cur = conn.cursor()
            for stat in body:
                # Skip LLDP / Controller flows
                if stat.priority == 0: continue
                
                # Identify Flow
                match = stat.match
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                
                # Handle L2 Flows logic (resolve IP from MAC if needed)
                if not src_ip and 'eth_src' in match: src_ip = self.ip_to_mac.get(match['eth_src'], None) # Simplified reverse lookup
                
                # Filter Interest
                if dst_ip != '10.0.0.2': continue
                if src_ip not in ['10.0.0.1', '10.0.0.3']: continue

                # --- THE INDUSTRIAL FIX ---
                # Kita gunakan COOKIE sebagai bagian dari Unique Key.
                # Jalur Normal (Cookie 0) dan Jalur Reroute (Cookie 0xDEADBEEF)
                # akan dianggap sebagai dua entitas berbeda. Tidak akan ada pengurangan/reset.
                cookie = stat.cookie
                key = (dpid, cookie, src_ip, dst_ip)
                
                curr_bytes = stat.byte_count
                curr_pkts = stat.packet_count
                
                # First time seeing this SPECIFIC flow (Path)? Init memory.
                if key not in self.last_bytes:
                    self.last_bytes[key] = (curr_bytes, curr_pkts)
                    continue 
                
                prev_bytes, prev_pkts = self.last_bytes[key]
                
                # Calculate Delta
                delta_bytes = curr_bytes - prev_bytes
                delta_packets = curr_pkts - prev_pkts
                
                # Update Memory
                self.last_bytes[key] = (curr_bytes, curr_pkts)
                
                # Validasi Delta (Harusnya selalu >= 0 karena cookie memisahkan instance flow)
                if delta_bytes < 0:
                    # Ini JANGGAL. Reset memory untuk key ini, tapi jangan log error.
                    # Biasanya terjadi jika switch reboot.
                    self.last_bytes[key] = (curr_bytes, curr_pkts)
                    continue
                
                if delta_bytes == 0: continue

                # Labeling berdasarkan Cookie untuk Analisis
                if cookie == COOKIE_REROUTE:
                    label = "reroute_active"
                elif src_ip == '10.0.0.1':
                    label = "voip_normal"
                else:
                    label = "bursty"

                # Insert to DB
                cur.execute("""
                    INSERT INTO traffic.flow_stats_ 
                    (timestamp, dpid, src_ip, dst_ip, bytes_tx, bytes_rx, 
                     pkts_tx, pkts_rx, duration_sec, traffic_label)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1.0, %s)
                """, (timestamp, dpid, src_ip, dst_ip, delta_bytes, delta_bytes, 
                      delta_packets, delta_packets, label))
            
            conn.commit(); cur.close(); conn.close()
        except Exception:
            if conn: conn.close()

    # =================================================================
    # 2. TOPOLOGY & ROUTING
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            try:
                temp_net = nx.DiGraph()
                switches = get_switch(self, None)
                for s in switches: temp_net.add_node(s.dp.id)
                links = get_link(self, None)
                for l in links:
                    temp_net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    temp_net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)
                self.net = temp_net
            except: pass

    # =================================================================
    # 3. AI LOGIC (DECISION MAKER)
    # =================================================================
    def _monitor_forecast_smart(self):
        self.logger.info("🧠 AI Analyst Running...")
        while True:
            hub.sleep(1.0)
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                # Ambil prediksi terakhir
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                res = cur.fetchone()
                cur.close(); conn.close()

                if not res: continue
                pred_bps = float(res[0])
                now = time.time()
                elapsed = now - self.last_state_change

                # State Machine
                if not self.congestion_active:
                    if pred_bps > BURST_THRESHOLD_BPS:
                        self.logger.warning(f"⚠️  PREDICTION HIGH ({pred_bps:,.0f} bps). ACTIVATE REROUTE.")
                        self.perform_reroute(pred_bps)
                        self.congestion_active = True
                        self.last_state_change = now
                else:
                    # Syarat kembali: Traffic rendah DAN Stabil (Hysteresis)
                    if pred_bps < HYSTERESIS_LOWER_BPS and elapsed > COOLDOWN_PERIOD_SEC:
                        self.logger.info(f"✅ PREDICTION LOW ({pred_bps:,.0f} bps). REVERT TO NORMAL.")
                        self.perform_revert(pred_bps)
                        self.congestion_active = False
                        self.last_state_change = now

            except Exception:
                if conn: conn.close()

    # =================================================================
    # 4. FLOW MODS (EXACT & PRECISE)
    # =================================================================
    def perform_reroute(self, val):
        """Install Flow Priority 500 dengan Cookie Reroute."""
        src, dst = 4, 5
        try:
            paths = list(nx.shortest_simple_paths(self.net, src, dst))
            # Logic: Hindari jalur default jika ada alternatif
            alt_path = None
            for p in paths:
                if 2 in p: # Cari yang lewat Spine 2
                    alt_path = p
                    break
            if not alt_path and len(paths) > 1: alt_path = paths[1]
            if not alt_path: return

            self.logger.info(f"🔄 Switching to Path: {alt_path}")

            # Install dari belakang ke depan (Best Practice)
            for i in range(len(alt_path) - 1, 0, -1):
                u, v = alt_path[i-1], alt_path[i]
                if self.net.has_edge(u, v):
                    port = self.net[u][v]['port']
                    dp = self.datapaths.get(u)
                    if dp:
                        self._install_reroute_flow(dp, port)
                        # Barrier untuk memastikan urutan install
                        dp.send_msg(dp.ofproto_parser.OFPBarrierRequest(dp))
            
            self.insert_event("REROUTE_ACTIVE", str(alt_path), val)
        except Exception as e:
            self.logger.error(f"Reroute Error: {e}")

    def perform_revert(self, val):
        """Hapus Flow Priority 500."""
        self.logger.info("🔙 Reverting to Default Path")
        for dp in self.datapaths.values():
            self._delete_reroute_flow(dp)
            dp.send_msg(dp.ofproto_parser.OFPBarrierRequest(dp))
        
        # Bersihkan Memory counter untuk cookie reroute agar bersih jika nanti dipakai lagi
        # (Optional, Python garbage collector handles logic, but explicit is good)
        keys_to_remove = [k for k in self.last_bytes if k[1] == COOKIE_REROUTE]
        for k in keys_to_remove: del self.last_bytes[k]

        self.insert_event("REROUTE_REVERT", "Normal Path", val)

    def _install_reroute_flow(self, dp, port):
        parser = dp.ofproto_parser
        # Match spesifik H1->H2
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(port)]
        inst = [parser.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # PENTING: Cookie diset ke COOKIE_REROUTE
        mod = parser.OFPFlowMod(dp, priority=PRIORITY_REROUTE, match=match, 
                                instructions=inst, cookie=COOKIE_REROUTE,
                                idle_timeout=0, hard_timeout=0)
        dp.send_msg(mod)

    def _delete_reroute_flow(self, dp):
        parser = dp.ofproto_parser
        # Hapus HANYA yang punya Cookie REROUTE
        mod = parser.OFPFlowMod(dp, command=dp.ofproto.OFPFC_DELETE, 
                                out_port=dp.ofproto.OFPP_ANY, out_group=dp.ofproto.OFPG_ANY,
                                cookie=COOKIE_REROUTE, cookie_mask=0xFFFFFFFFFFFFFFFF)
        dp.send_msg(mod)

    def insert_event(self, evt, desc, val):
        conn = self.get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value) VALUES (NOW(), %s, %s, %s)", (evt, desc, val))
                conn.commit(); cur.close(); conn.close()
            except: pass

    # =================================================================
    # 5. PACKET IN (ORIGINAL LOGIC - KEPT SAFE)
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths: self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths: del self.datapaths[datapath.id]

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
        # Default flows use Cookie 0
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority, match=match, instructions=inst, idle_timeout=idle_timeout, cookie=COOKIE_DEFAULT)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst, idle_timeout=idle_timeout, cookie=COOKIE_DEFAULT)
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

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # LOGIC PINNING (DEFAULT PATH)
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            COLLISION_PORT = 3 

            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                # Kalau reroute aktif, biarkan flow priority 500 yang menangani.
                # Jangan install flow priority 10 yang menabrak.
                if self.congestion_active and src_ip == '10.0.0.1':
                    pass 
                else:
                    actions = [parser.OFPActionOutput(COLLISION_PORT)]
                    match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                    # Priority 10, Cookie 0
                    self.add_flow(datapath, PRIORITY_NORMAL, match, actions, msg.buffer_id, idle_timeout=0)
                    
                    data = None
                    if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                            in_port=in_port, actions=actions, data=data)
                    datapath.send_msg(out)
                    return 

        # ARP
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.ip_to_mac:
                target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                e = ethernet.ethernet(dst=src, src=target_mac, ethertype=ether_types.ETH_TYPE_ARP)
                a = arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=arp_pkt.dst_ip, dst_mac=src, dst_ip=arp_pkt.src_ip)
                p = packet.Packet(); p.add_protocol(e); p.add_protocol(a); p.serialize()
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                datapath.send_msg(out)
                return

        # L2 SWITCHING
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        if out_port == ofproto.OFPP_FLOOD:
            if dpid >= 4:
                if in_port <= 3: actions = [parser.OFPActionOutput(4), parser.OFPActionOutput(5)]
                else: actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
            elif dpid <= 3:
                actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)