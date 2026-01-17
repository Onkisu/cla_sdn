#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (Final Industrial Merge)
- Base Logic: User's Proven Code (ARP/ICMP Works)
- Reroute Logic: Enterprise Make-Before-Break (Upgraded)
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
import json

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

BURST_THRESHOLD_BPS = 120000 
HYSTERESIS_LOWER_BPS = 80000   # Traffic harus turun ke sini baru revert
COOLDOWN_PERIOD_SEC = 20       # Waktu tunggu minimal antar switch

# FLOW CONSTANTS (INDUSTRIAL STANDARD)
PRIORITY_DEFAULT = 10          # Priority routing biasa (User Code)
PRIORITY_REROUTE = 500         # Priority reroute (Harus lebih tinggi)
COOKIE_REROUTE   = 0xDEADBEEF  # Penanda khusus flow reroute

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        # --- INIT DATA STRUCTURES (SAME AS YOURS) ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() 
        self.last_bytes = {}
        self.start_time = time.time()
        
        # --- STATE CONGESTION ---
        self.congestion_active = False 
        self.last_state_change_time = 0
        self.current_reroute_path = None
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast_smart) # Upgraded
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("🟢 VoIP Smart Controller (Industrial Edition) Started")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("✅ Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}. Running without DB storage.")
            self.db_conn = None
    
    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception as e:
            # Silent error to prevent spamming
            return None

    # =================================================================
    # 1. TOPOLOGY DISCOVERY (NETWORKX)
    # =================================================================
    def _discover_topology(self):
        """Memetakan Switch dan Link ke NetworkX Graph secara berkala"""
        while True:
            hub.sleep(3)
            try:
                # Rebuild graph to ensure freshness
                temp_net = nx.DiGraph()
                
                # Get Switches
                switches = get_switch(self, None)
                for s in switches:
                    temp_net.add_node(s.dp.id)
                
                # Get Links with Port Info
                links = get_link(self, None)
                for l in links:
                    temp_net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    temp_net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)
                
                self.net = temp_net
            except Exception as e:
                self.logger.error(f"Topology Discovery Error: {e}")

    # =================================================================
    # 2. FORECAST MONITORING (UPGRADED LOGIC)
    # =================================================================
    def _monitor_forecast_smart(self):
        """
        Logic Otomatis yang membaca DB Forecast dan melakukan Reroute
        dengan Hysteresis dan State Machine.
        """
        self.logger.info("👁️  AI Watchdog Active: Waiting for burst prediction...")
        
        while True:
            hub.sleep(1.0) # Cek setiap detik
            
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                # Ambil 1 prediksi terbaru
                cur.execute("""
                    SELECT y_pred 
                    FROM forecast_1h 
                    ORDER BY ts_created DESC 
                    LIMIT 1
                """)
                result = cur.fetchone()
                cur.close(); conn.close()

                if not result: continue

                pred_bps = float(result[0])
                now = time.time()
                time_since_change = now - self.last_state_change_time

                # --- STATE MACHINE ---
                
                # KONDISI 1: DETEKSI BURST (NORMAL -> REROUTE)
                if not self.congestion_active:
                    if pred_bps > BURST_THRESHOLD_BPS:
                        self.logger.warning(f"🚀 AI PREDICTION: Burst Incoming ({pred_bps:,.0f} bps). ENGAGING REROUTE!")
                        self.perform_industrial_reroute(trigger_val=pred_bps)
                        
                        self.congestion_active = True
                        self.last_state_change_time = now

                # KONDISI 2: RESTORE NORMAL (REROUTE -> NORMAL)
                else:
                    # Syarat: Traffic harus turun JAUH (Hysteresis) DAN Cooldown selesai
                    if pred_bps < HYSTERESIS_LOWER_BPS and time_since_change > COOLDOWN_PERIOD_SEC:
                        self.logger.info(f"✅ AI PREDICTION: Traffic Stable ({pred_bps:,.0f} bps). RESTORING PATH.")
                        self.perform_industrial_revert(trigger_val=pred_bps)
                        
                        self.congestion_active = False
                        self.last_state_change_time = now
                    # else:
                        # Waiting for stability...

            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")
                if conn and not conn.closed: conn.close()

    # =================================================================
    # 3. INDUSTRIAL REROUTING LOGIC (THE "KEREN" PART)
    # =================================================================
    def perform_industrial_reroute(self, trigger_val):
        """
        Melakukan Rerouting dengan teknik Make-Before-Break.
        1. Hitung jalur alternatif (hindari Spine 1).
        2. Install flow dari ujung ke pangkal (Reverse Install).
        3. Gunakan Priority Tinggi & Cookie Unik.
        """
        src_sw, dst_sw = 4, 5 # Leaf 1 -> Leaf 2
        bad_node = 1          # Spine 1 (Asumsi macet)

        try:
            # 1. Cari K-Shortest Path
            paths = list(nx.shortest_simple_paths(self.net, src_sw, dst_sw))
            
            # 2. Filter path yang TIDAK lewat bad_node
            # Contoh Path: [4, 2, 5] (Lewat Spine 2)
            alt_path = next((p for p in paths if bad_node not in p), None)

            if not alt_path:
                self.logger.error("❌ CRITICAL: No alternative path available!")
                return

            self.logger.info(f"🛣️  Calculated Bypass Path: {alt_path}")

            # 3. INSTALL FLOW (REVERSE ORDER)
            # Kita install di switch terakhir dulu (Leaf 2), lalu Spine 2, lalu Leaf 1.
            # Ini mencegah packet loss jika flow di Leaf 1 aktif duluan tapi Spine 2 belum siap.
            for i in range(len(alt_path) - 1, 0, -1):
                curr_node = alt_path[i-1]
                next_node = alt_path[i]
                
                # Cari port output dari curr ke next
                if self.net.has_edge(curr_node, next_node):
                    out_port = self.net[curr_node][next_node]['port']
                    dp = self.datapaths.get(curr_node)
                    
                    if dp:
                        self._install_reroute_flow(dp, out_port)
                        # Kirim Barrier Request (Tunggu switch selesai tulis ke hardware)
                        self._send_barrier(dp)

            self.current_reroute_path = alt_path
            self.insert_event_log("REROUTE_ACTIVE", f"Switched H1 to {alt_path}", trigger_val)

        except nx.NetworkXNoPath:
            self.logger.error("❌ NetworkX: No path found")
        except Exception as e:
            self.logger.error(f"Reroute Logic Error: {e}")

    def perform_industrial_revert(self, trigger_val):
        """
        Mengembalikan jalur dengan cara menghapus flow Reroute secara Atomic.
        """
        self.logger.info("🔙 Reverting to Default Path (Atomic Cleanup)...")
        
        # Hapus flow yang memiliki Cookie REROUTE di semua switch
        # Tidak perlu menghapus flow default, otomatis akan terpakai lagi karena priority lebih rendah
        for dpid, dp in self.datapaths.items():
            self._delete_reroute_flow_by_cookie(dp)
            self._send_barrier(dp)
        
        self.current_reroute_path = None
        self.insert_event_log("REROUTE_REVERT", "Restored to Default Path", trigger_val)

    # --- LOW LEVEL FLOW HELPERS ---

    def _install_reroute_flow(self, datapath, out_port):
        parser = datapath.ofproto_parser
        
        # Match H1 (10.0.0.1) -> H2 (10.0.0.2) SPECIFICALLY
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # PRIORITY LEBIH TINGGI DARI DEFAULT (500 > 10)
        # COOKIE DISERI AGAR MUDAH DIHAPUS
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=PRIORITY_REROUTE, 
            match=match,
            instructions=inst,
            cookie=COOKIE_REROUTE,
            idle_timeout=0, 
            hard_timeout=0
        )
        datapath.send_msg(mod)

    def _delete_reroute_flow_by_cookie(self, datapath):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        
        # Hapus flow dengan Cookie tertentu (Sangat bersih & aman)
        mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            cookie=COOKIE_REROUTE,
            cookie_mask=0xFFFFFFFFFFFFFFFF # Exact match
        )
        datapath.send_msg(mod)

    def _send_barrier(self, datapath):
        """Memastikan perintah flowmod sebelumnya selesai dieksekusi switch"""
        datapath.send_msg(datapath.ofproto_parser.OFPBarrierRequest(datapath))

    def insert_event_log(self, event_type, description, trigger_value=0):
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events
                (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, description, trigger_value))
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass

    # =================================================================
    # 3. STANDARD HANDLERS (PacketIn, SwitchFeatures)
    # SAYA TIDAK MENGUBAH LOGIC INI AGAR PING TETAP JALAN
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
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
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority, match=match, instructions=inst, idle_timeout=idle_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst, idle_timeout=idle_timeout)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # -----------------------------------------------------------
        # CODE INI SAMA PERSIS DENGAN REFERENSI ANDA
        # KARENA INI YANG MEMBUAT PING BERHASIL & DEFAULT ROUTING
        # -----------------------------------------------------------
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

        # 1. MAC LEARNING
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # 2. PROTOCOL EXTRACTION
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # =================================================================
        # LOGIKA DEFAULT ROUTE (PINNING KE SPINE 2) - KEEPING YOUR LOGIC
        # =================================================================
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Port 3 = Menuju Spine 2 (Jalur Tabrakan di Topologi Anda)
            COLLISION_PORT = 3 

            # Cek: Jika paket dari H1 (di DPID 4) ATAU H3 (di DPID 6)
            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                
                # SKIP jika sedang dalam mode reroute 
                # (Flow Priority 500 akan menangani ini, tapi ini double check)
                if self.congestion_active and src_ip == '10.0.0.1':
                    pass # Biarkan flow priority tinggi yang handle
                else:
                    actions = [parser.OFPActionOutput(COLLISION_PORT)]
                    # Priority 10 (Lebih rendah dari Reroute 500)
                    match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                    self.add_flow(datapath, PRIORITY_DEFAULT, match, actions, msg.buffer_id, idle_timeout=0)
                    
                    data = None
                    if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                        data = msg.data
                    
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                            in_port=in_port, actions=actions, data=data)
                    datapath.send_msg(out)
                    return 
        # =================================================================

        # --- ARP HANDLER (CRUCIAL FOR PING) ---
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.ip_to_mac:
                target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                e = ethernet.ethernet(dst=src, src=target_mac, ethertype=ether_types.ETH_TYPE_ARP)
                a = arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=arp_pkt.dst_ip, dst_mac=src, dst_ip=arp_pkt.src_ip)
                p = packet.Packet()
                p.add_protocol(e)
                p.add_protocol(a)
                p.serialize()
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                datapath.send_msg(out)
                return

        # --- STANDARD SWITCHING LOGIC ---
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = []
        is_leaf = dpid >= 4
        is_spine = dpid <= 3
        
        if out_port == ofproto.OFPP_FLOOD:
            if is_leaf:
                if in_port <= 3: 
                    actions.append(parser.OFPActionOutput(4))
                    actions.append(parser.OFPActionOutput(5))
                else: 
                    actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
            elif is_spine:
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
        else:
            actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 4. MONITORING TRAFFIC (TO DATABASE)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    def _resolve_ip(self, mac):
        if not mac: return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: return ip
        return None

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = datapath.id
        timestamp = datetime.now()
        
        conn = self.get_db_conn()
        if not conn: return

        try:
            cur = conn.cursor()
            for stat in body:
                if stat.priority == 0: continue
                
                match = stat.match
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                
                if not src_ip:
                    src_mac = match.get('eth_src')
                    src_ip = self._resolve_ip(src_mac) if src_mac else None
                
                if not dst_ip:
                    dst_mac = match.get('eth_dst')
                    dst_ip = self._resolve_ip(dst_mac) if dst_mac else None
                
                # Filter only target traffic
                if dst_ip != '10.0.0.2': continue
                if src_ip not in ['10.0.0.1', '10.0.0.3']: continue
                
                flow_key = f"{dpid}-{src_ip}-{dst_ip}"
                byte_count = stat.byte_count
                packet_count = stat.packet_count
                
                if flow_key in self.last_bytes:
                    last_bytes, last_packets = self.last_bytes[flow_key]
                    delta_bytes = max(0, byte_count - last_bytes)
                    delta_packets = max(0, packet_count - last_packets)
                else:
                    delta_bytes = byte_count
                    delta_packets = packet_count
                
                self.last_bytes[flow_key] = (byte_count, packet_count)
                
                if delta_bytes <= 0: continue

                # Insert to DB
                cur.execute("""
                    INSERT INTO traffic.flow_stats_ 
                    (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                    ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                    pkts_tx, pkts_rx, duration_sec, traffic_label)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    timestamp, dpid, src_ip, dst_ip,
                    match.get('eth_src'), match.get('eth_dst'),
                    match.get('ip_proto', 17), 0, 0,
                    delta_bytes, delta_bytes,
                    delta_packets, delta_packets,
                    1.0, 'voip' if src_ip == '10.0.0.1' else 'bursty'
                ))
            conn.commit()
            cur.close(); conn.close()
        except Exception:
            if conn: conn.close()