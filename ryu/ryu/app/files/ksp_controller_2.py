#!/usr/bin/env python3
"""
Ryu SDN Controller: Enterprise VoIP Traffic Engineering
======================================================
Features:
1. Auto-Topology Discovery (NetworkX) - No Hardcoded Ports
2. Hybrid ARP Proxy (Instant Ping + Anti-Loop)
3. Predictive Rerouting (PostgreSQL + AI Forecast)
4. Make-Before-Break Routing (Zero Downtime)
5. Atomic Flow Management (Cookies + Barriers)

Author: Gemini (Based on User Architecture)
"""

import time
import json
import random
import math
import networkx as nx
import psycopg2
from datetime import datetime
from operator import attrgetter

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
from ryu.topology import event, api
from ryu.topology.api import get_switch, get_link

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

# Threshold (Harus sinkron dengan forecast_2.py)
BURST_THRESHOLD_BPS = 120000
HYSTERESIS_LOWER_BPS = 100000  # Traffic harus turun di bawah ini untuk revert
COOLDOWN_SEC = 20              # Waktu tunggu minimal antar perubahan jalur

# Konstanta Identifikasi Flow
COOKIE_DEFAULT = 0x100        # Flow normal
COOKIE_REROUTE = 0xDEADBEEF   # Flow reroute (Priority Tinggi)

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        
        # --- Data Structures ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     # ARP Cache (IP -> MAC)
        self.datapaths = {}     # Active Switches (dpid -> datapath obj)
        self.net = nx.DiGraph() # Topology Graph
        self.last_bytes = {}    # Untuk hitung delta throughput
        
        # --- State Machine ---
        self.congestion_active = False
        self.last_state_change = time.time()
        self.active_path = "DEFAULT"
        
        # --- Database Connection ---
        self.check_db_connection()

        # --- Spawning Threads ---
        # 1. Topology Discovery (NetworkX)
        self.topology_thread = hub.spawn(self._discover_topology)
        # 2. Traffic Stats Collector (To DB)
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        # 3. Forecast Watchdog (From DB -> Action)
        self.ai_thread = hub.spawn(self._ai_watchdog)

        self.logger.info("🟢 INDUSTRIAL CONTROLLER STARTED: Full Automation Enabled.")

    def check_db_connection(self):
        conn = self.get_db_conn()
        if conn:
            self.logger.info("✅ Database Connection: ESTABLISHED")
            conn.close()
        else:
            self.logger.error("❌ Database Connection: FAILED")

    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception as e:
            # Silent error to prevent log spamming in loops
            return None

    # =================================================================
    # 1. DYNAMIC TOPOLOGY DISCOVERY
    # =================================================================
    def _discover_topology(self):
        """
        Membangun graph jaringan secara otomatis.
        Tidak perlu hardcode port 1, 2, atau 3. Controller akan mencarinya.
        """
        while True:
            hub.sleep(5)
            try:
                # Create temp graph
                temp_net = nx.DiGraph()
                
                # Add Switches
                switches = get_switch(self, None)
                for s in switches:
                    temp_net.add_node(s.dp.id)
                
                # Add Links with Port Info
                links = get_link(self, None)
                for l in links:
                    temp_net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    temp_net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)
                
                # Atomic Update
                self.net = temp_net
                # self.logger.debug(f"🌐 Topology Updated: {self.net.number_of_nodes()} Nodes")
                
            except Exception as e:
                self.logger.error(f"Topology Error: {e}")

    # =================================================================
    # 2. AI WATCHDOG (OTOMATISASI PENUH)
    # =================================================================
    def _ai_watchdog(self):
        """
        Membaca hasil prediksi forecast_2.py dari DB dan mengambil keputusan.
        """
        self.logger.info("👁️  AI Watchdog: Watching forecast_1h table...")
        
        while True:
            hub.sleep(1.0) # Real-time check
            
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                # Ambil 1 data prediksi terbaru
                cur.execute("""
                    SELECT y_pred, ts_created 
                    FROM forecast_1h 
                    ORDER BY ts_created DESC 
                    LIMIT 1
                """)
                res = cur.fetchone()
                cur.close(); conn.close()

                if not res: continue

                pred_bps = float(res[0])
                now = time.time()
                time_since_change = now - self.last_state_change

                # --- DECISION LOGIC (STATE MACHINE) ---
                
                # STATE: NORMAL -> BURST (Reroute Trigger)
                if not self.congestion_active:
                    if pred_bps > BURST_THRESHOLD_BPS:
                        self.logger.warning(f"🚀 AI PREDICTION: Burst ({pred_bps:,.0f} bps). ENGAGING REROUTE.")
                        self.activate_ksp_reroute(pred_bps)
                        
                        self.congestion_active = True
                        self.last_state_change = now

                # STATE: BURST -> NORMAL (Revert Trigger)
                else:
                    # Syarat Revert: Traffic turun JAUH & Cooldown selesai
                    if pred_bps < HYSTERESIS_LOWER_BPS and time_since_change > COOLDOWN_SEC:
                        self.logger.info(f"✅ AI PREDICTION: Stable ({pred_bps:,.0f} bps). REVERTING TO DEFAULT.")
                        self.revert_routing(pred_bps)
                        
                        self.congestion_active = False
                        self.last_state_change = now

            except Exception as e:
                self.logger.error(f"Watchdog Loop Error: {e}")

    # =================================================================
    # 3. INDUSTRIAL REROUTING (MAKE-BEFORE-BREAK)
    # =================================================================
    def activate_ksp_reroute(self, trigger_val):
        """
        Mencari jalur alternatif untuk H1->H2 yang TIDAK melewati Spine 1.
        """
        # Node ID di Mininet (Decimal)
        # H1 ada di Leaf 1 (dpid 4), H2 ada di Leaf 2 (dpid 5)
        src_node, dst_node = 4, 5
        bad_node = 1 # Spine 1 (Asumsi macet)

        try:
            # 1. Hitung K-Shortest Paths
            paths = list(nx.shortest_simple_paths(self.net, src_node, dst_node))
            
            # 2. Filter path yang TIDAK lewat bad_node
            # Contoh Path: [4, 2, 5] (Leaf1 -> Spine2 -> Leaf2)
            alt_path = next((p for p in paths if bad_node not in p), None)

            if not alt_path:
                self.logger.error("❌ CRITICAL: No alternative path found!")
                return

            self.logger.info(f"🛣️  Calculated Path: {alt_path}")

            # 3. INSTALL FLOW (Make-Before-Break)
            # Kita install flow dari ujung (dst) ke pangkal (src)
            # Agar paket tidak sampai di tengah jalan lalu drop.
            for i in range(len(alt_path) - 1, 0, -1):
                u, v = alt_path[i-1], alt_path[i]
                
                # Cari port keluar di switch u menuju v
                if self.net.has_edge(u, v):
                    out_port = self.net[u][v]['port']
                    dp = self.datapaths.get(u)
                    
                    if dp:
                        self._install_reroute_flow(dp, out_port)
                        # Barrier: Tunggu switch selesai tulis ke hardware
                        self._send_barrier(dp)

            self.active_path = str(alt_path)
            self._log_event("REROUTE_ACTIVE", f"Switched to {alt_path}", trigger_val)

        except nx.NetworkXNoPath:
            self.logger.error("❌ NetworkX: No path between hosts")
        except Exception as e:
            self.logger.error(f"Reroute Logic Error: {e}")

    def revert_routing(self, trigger_val):
        """
        Menghapus flow reroute secara atomik menggunakan Cookie.
        """
        self.logger.info("🔙 Cleaning up reroute flows...")
        
        # Hapus flow dengan cookie REROUTE di semua switch
        for dpid, dp in self.datapaths.items():
            self._delete_reroute_flow(dp)
            self._send_barrier(dp)
        
        self.active_path = "DEFAULT"
        self._log_event("REROUTE_REVERT", "Back to Default Path", trigger_val)

    # --- Low Level Flow Ops ---

    def _install_reroute_flow(self, datapath, out_port):
        parser = datapath.ofproto_parser
        
        # Match H1 -> H2 Specific
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # PRIORITY TINGGI (30000) agar menimpa default
        # COOKIE REROUTE agar mudah dihapus massal
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=30000,
            match=match,
            instructions=inst,
            cookie=COOKIE_REROUTE,
            idle_timeout=0,
            hard_timeout=0
        )
        datapath.send_msg(mod)

    def _delete_reroute_flow(self, datapath):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        
        # Hapus berdasarkan Cookie (Sangat Cepat & Aman)
        mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            cookie=COOKIE_REROUTE,
            cookie_mask=0xFFFFFFFFFFFFFFFF # Exact match cookie
        )
        datapath.send_msg(mod)

    def _send_barrier(self, datapath):
        datapath.send_msg(datapath.ofproto_parser.OFPBarrierRequest(datapath))

    # =================================================================
    # 4. PACKET HANDLING (HYBRID ARP & IP)
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.datapaths[datapath.id] = datapath
        
        # Table-Miss: Send to Controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions, cookie=COOKIE_DEFAULT)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0, cookie=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout, cookie=cookie)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst, 
                                    idle_timeout=idle_timeout, cookie=cookie)
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
        if eth.ethertype == 34525: return # Ignore IPv6

        dst = eth.dst
        src = eth.src

        # --- A. HYBRID ARP HANDLING (THE FIX FOR ICMP) ---
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            self._handle_arp(datapath, in_port, pkt)
            return

        # --- B. IP HANDLING & DEFAULT ROUTING ---
        if eth.ethertype == ether_types.ETH_TYPE_IP:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)
            
            # Default Routing Logic:
            # Jika paket dari H1/H3 menuju H2, dan TIDAK ada reroute aktif
            # Paksa lewat jalur default (Spine 1).
            # Kita tidak hardcode port '3', tapi cari di graph.
            if ip_pkt and ip_pkt.dst == '10.0.0.2':
                # Asumsi default spine = 1
                default_spine = 1
                
                # Cek apakah switch ini punya link ke Spine 1?
                if self.net.has_edge(dpid, default_spine):
                    out_port = self.net[dpid][default_spine]['port']
                    
                    # Install Flow Priority Rendah (10)
                    actions = [parser.OFPActionOutput(out_port)]
                    match = parser.OFPMatch(eth_type=0x0800, ipv4_src=ip_pkt.src, ipv4_dst='10.0.0.2')
                    
                    # Jangan pasang jika reroute aktif (redundant, tapi safe)
                    if not self.congestion_active:
                         self.add_flow(datapath, 10, match, actions, msg.buffer_id, cookie=COOKIE_DEFAULT)
                    
                    # Forward packet
                    data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                            in_port=in_port, actions=actions, data=data)
                    datapath.send_msg(out)
                    return

        # --- C. L2 LEARNING FALLBACK ---
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, cookie=COOKIE_DEFAULT)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _handle_arp(self, datapath, port, pkt):
        """
        Logic ARP yang 'Pintar'.
        1. Catat IP->MAC (Snooping).
        2. Jika ada yang tanya (Req) dan kita tahu jawabannya -> Reply langsung (Proxy).
        3. Jika tidak tahu -> Flood.
        """
        pkt_arp = pkt.get_protocol(arp.arp)
        src_ip = pkt_arp.src_ip
        src_mac = pkt_arp.src_mac
        dst_ip = pkt_arp.dst_ip

        # Learn
        self.ip_to_mac[src_ip] = src_mac

        if pkt_arp.opcode == arp.ARP_REQUEST:
            if dst_ip in self.ip_to_mac:
                # HIT! Controller jadi Proxy ARP
                dst_mac = self.ip_to_mac[dst_ip]
                self._send_arp_reply(datapath, port, src_mac, src_ip, dst_mac, dst_ip)
            else:
                # MISS! Flood ke semua port (kecuali in_port)
                self._flood_packet(datapath, port, pkt)
        else:
            # ARP Reply packets -> Flood/Forward agar sampai ke tujuan
            self._flood_packet(datapath, port, pkt)

    def _send_arp_reply(self, datapath, port, target_mac, target_ip, sender_mac, sender_ip):
        # Buat paket ARP Reply palsu
        e = ethernet.ethernet(dst=target_mac, src=sender_mac, ethertype=ether_types.ETH_TYPE_ARP)
        a = arp.arp(opcode=arp.ARP_REPLY, src_mac=sender_mac, src_ip=sender_ip,
                    dst_mac=target_mac, dst_ip=target_ip)
        p = packet.Packet()
        p.add_protocol(e)
        p.add_protocol(a)
        p.serialize()
        
        actions = [datapath.ofproto_parser.OFPActionOutput(port)]
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=datapath.ofproto.OFP_NO_BUFFER,
            in_port=datapath.ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
        datapath.send_msg(out)

    def _flood_packet(self, datapath, in_port, pkt):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=in_port, actions=actions, data=pkt.data)
        datapath.send_msg(out)

    # =================================================================
    # 5. MONITORING & STATS (DATA FEEDER)
    # =================================================================
    def _monitor_traffic(self):
        """Meminta statistik flow setiap detik"""
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
        
        conn = self.get_db_conn()
        if not conn: return
        
        try:
            cur = conn.cursor()
            for stat in body:
                if stat.priority == 0: continue # Skip table-miss
                
                match = stat.match
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                
                # Kita hanya log trafik yang relevan (misal menuju H2)
                if src_ip and dst_ip and dst_ip == '10.0.0.2':
                    
                    # Generate Key untuk perhitungan Delta
                    key = f"{dpid}-{src_ip}-{dst_ip}"
                    last_b = self.last_bytes.get(key, 0)
                    delta_bytes = max(0, stat.byte_count - last_b)
                    self.last_bytes[key] = stat.byte_count
                    
                    # Tentukan Label Traffic (Opsional, tapi diminta user)
                    label = 'voip' if src_ip == '10.0.0.1' else 'bursty'

                    # Insert FULL IP Info (Sesuai request user)
                    cur.execute("""
                        INSERT INTO traffic.flow_stats_ 
                        (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                         ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                         pkts_tx, pkts_rx, duration_sec, traffic_label)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        timestamp, dpid, src_ip, dst_ip, 
                        match.get('eth_src'), match.get('eth_dst'),
                        match.get('ip_proto', 17), 0, 0, # Port info opsional kalau flow match-nya wildcard
                        delta_bytes, delta_bytes,
                        stat.packet_count, stat.packet_count,
                        1.0, label
                    ))
            conn.commit()
            cur.close(); conn.close()
        except Exception:
            # Log error once or silent
            pass

    def _log_event(self, evt, desc, val):
        conn = self.get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value) VALUES (NOW(), %s, %s, %s)", (evt, desc, val))
                conn.commit(); conn.close()
            except: pass