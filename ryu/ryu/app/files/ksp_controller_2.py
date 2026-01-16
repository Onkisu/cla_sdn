#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (Full Merge)
- Features: K-Shortest Path Rerouting based on DB Forecast
- Features: Sine Wave Traffic Generation with Spike Detection
- Features: Full IP Resolution and Database Logging
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


# PostgreSQL Configuration
# Saya set ke IP Remote agar sinkron dengan forecast_2.py
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Threshold Congestion (Sesuai forecast_2.py)
BURST_THRESHOLD_BPS = 199000 

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        # --- INIT DATA STRUCTURES ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() # NetworkX Graph
        self.last_bytes = {}
        self.start_time = time.time()
        
        # --- STATE CONGESTION ---
        self.congestion_active = False 
        self.reroute_priority = 30000 

        # [NEW] Variabel untuk mencegah Flapping
        self.last_congestion_time = 0
        self.cooldown_period = 30  # Detik (Wajib stabil dulu sebelum revert)
        self.LOWER_THRESHOLD_BPS = 150000 # Batas aman untuk kembali (Jauh di bawah 250k)
        
        # [NEW] Track current active reroute path
        self.current_reroute_path = None
        self.default_path = None
        
        self.connect_database()
        
        # --- THREADS ---
        # 1. Monitor Traffic Stats (Original)
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        # 2. Monitor Forecast dari DB (New KSP)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        # 3. Discovery Topology (New KSP)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("VoIP Smart Controller (KSP + Full Traffic Monitor) Started")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}. Running without DB storage.")
            self.db_conn = None
    
    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception as e:
            self.logger.error(f"DB Connect Error: {e}")
            return None


    # =================================================================
    # 1. TOPOLOGY DISCOVERY (NETWORKX) - NEW
    # =================================================================
    def _discover_topology(self):
        """Memetakan Switch dan Link ke NetworkX Graph secara berkala"""
        while True:
            hub.sleep(5)
            self.net.clear()
            
            # Get Switches
            switches = get_switch(self, None)
            for s in switches:
                self.net.add_node(s.dp.id)
            
            # Get Links
            links = get_link(self, None)
            for l in links:
                self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    # =================================================================
    # 2. FORECAST MONITORING & KSP LOGIC - FIXED (HYSTERESIS)
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)
            conn = self.get_db_conn()
            if not conn:
                continue
            try:
                cur = conn.cursor()
                # Ambil prediksi terbaru
                cur.execute("""
                    SELECT y_pred 
                    FROM forecast_1h 
                    ORDER BY ts_created DESC 
                    LIMIT 1
                """)
                result = cur.fetchone()
                cur.close()
                conn.close()

                if not result:
                    continue

                pred_bps = result[0]
                current_time = time.time()

                # --- LOGIKA BARU DENGAN HYSTERESIS ---
                
                # CASE 1: Deteksi Congestion (TRIGGER NAIK)
                # Syarat: Traffic tinggi (> 250k) DAN status sekarang sedang Normal
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️ CONGESTION PREDICTED: {pred_bps:.2f} bps -> REROUTING!")
                    
                    # Terapkan Reroute
                    self.apply_ksp_reroute(k=3, trigger_val=pred_bps)
                    
                    # Update status
                    self.congestion_active = True
                    self.last_congestion_time = current_time # Catat waktu kejadian

                # CASE 2: Kembali Normal (TRIGGER TURUN)
                # Syarat:
                # 1. Traffic harus TURUN JAUH (misal < 150k), bukan cuma < 250k
                # 2. Harus sudah melewati masa cooldown (30 detik) dari kejadian terakhir
                elif self.congestion_active:
                    time_since_last_trigger = current_time - self.last_congestion_time
                    
                    if pred_bps < self.LOWER_THRESHOLD_BPS and time_since_last_trigger > self.cooldown_period:
                        self.logger.info(f"✅ TRAFFIC STABLE ({pred_bps:.2f} bps) for {int(time_since_last_trigger)}s -> REVERTING.")
                        
                        # Terapkan Revert
                        self.revert_routing()
                        self.congestion_active = False
                    else:
                        # Logging opsional untuk debug (bisa dihapus kalau nyepam)
                        # self.logger.info(f"⏳ Waiting to revert... Current: {pred_bps:.2f}, Time held: {int(time_since_last_trigger)}s")
                        pass

            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")
                if conn and not conn.closed: conn.close()


    def get_k_shortest_paths(self, src_dpid, dst_dpid, k=3):
        try:
            return list(nx.shortest_simple_paths(self.net, src_dpid, dst_dpid))[0:k]
        except nx.NetworkXNoPath:
            return []
        except Exception:
            return []

    def apply_ksp_reroute(self, k=3, trigger_val=0):
        """Reroute H1 VoIP SAJA (biarkan H3 tetap di path lama)"""
        self.logger.warning(f"🚀 REROUTING H1 VoIP to avoid congestion...")
        
        # 1. HAPUS HANYA FLOW H1 (VoIP), jangan hapus H3!
        self.delete_h1_flows_only()
        
        # 2. Cari path alternatif untuk H1 (HANYA H1!)
        src_sw = 4  # Leaf 1 (tempat H1)
        dst_sw = 5  # Leaf 2 (tempat H2)
        
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=k)
        if len(paths) < 2:
            return
        
        # Default path: [4, 3, 5] (Leaf1→Spine2→Leaf2)
        # Pilih path alternatif: [4, 1, 5] (Leaf1→Spine1→Leaf2)
        new_route = None
        for path in paths:
            if 1 in path:  # Pilih path yang lewat Spine 1 (DPID 1)
                new_route = path
                break
        
        if not new_route:
            new_route = paths[1]  # Fallback ke path kedua
        
        # 3. Install flow H1 di Leaf 1 (DPID 4) untuk ke Spine 1
        dp_leaf1 = self.datapaths.get(4)
        if dp_leaf1 and len(new_route) > 1:
            parser = dp_leaf1.ofproto_parser
            next_hop = new_route[1]  # Spine 1 (DPID 1) atau Spine 3
            
            if self.net.has_edge(4, next_hop):
                out_port = self.net[4][next_hop]['port']
                
                # Match H1→H2 (VoIP)
                match = parser.OFPMatch(
                    eth_type=0x0800,
                    ipv4_src='10.0.0.1',
                    ipv4_dst='10.0.0.2'
                )
                actions = [parser.OFPActionOutput(out_port)]
                
                # Priority tinggi untuk reroute
                self.add_flow(dp_leaf1, 40000, match, actions, idle_timeout=0)
                
                self.logger.info(f"✅ Rerouted H1→H2: DPID 4 → port {out_port} (to DPID {next_hop})")
                self.logger.info(f"📊 New path: {new_route}")
        
        # 4. Simpan state
        self.current_reroute_path = new_route
        self.congestion_active = True
        self.last_congestion_time = time.time()
        
        # 5. Log ke DB
        self.insert_event_log(
            event_type="REROUTE_ACTIVE",
            description=f"H1 rerouted to path {new_route}",
            trigger_value=trigger_val
        )


    def delete_h1_flows_only(self):
        """Hapus HANYA flow H1→H2 (VoIP), biarkan H3 tetap"""
        self.logger.info("🧹 Deleting ONLY H1→H2 VoIP flows...")
        
        for dpid, dp in self.datapaths.items():
            try:
                parser = dp.ofproto_parser
                ofp = dp.ofproto
                
                # Hapus SEMUA flow untuk H1→H2 (tidak peduli protocol/port)
                # Match spesifik: H1 source, H2 destination
                match_h1_to_h2 = parser.OFPMatch(
                    eth_type=0x0800,        # IPv4
                    ipv4_src='10.0.0.1',    # H1
                    ipv4_dst='10.0.0.2'     # H2
                )
                
                # DELETE semua priority untuk H1→H2
                mod = parser.OFPFlowMod(
                    datapath=dp,
                    command=ofp.OFPFC_DELETE,
                    out_port=ofp.OFPP_ANY,
                    out_group=ofp.OFPG_ANY,
                    match=match_h1_to_h2
                )
                dp.send_msg(mod)
                
                # Juga hapus flow dengan hanya source H1 (catch-all)
                match_h1_only = parser.OFPMatch(
                    eth_type=0x0800,
                    ipv4_src='10.0.0.1'
                )
                
                mod2 = parser.OFPFlowMod(
                    datapath=dp,
                    command=ofp.OFPFC_DELETE,
                    out_port=ofp.OFPP_ANY,
                    out_group=ofp.OFPG_ANY,
                    match=match_h1_only
                )
                dp.send_msg(mod2)
                
                self.logger.info(f"  Deleted H1 flows from DPID {dpid}")
                    
            except Exception as e:
                self.logger.error(f"Error deleting H1 flows from DPID {dpid}: {e}")
        
        # Clear cache H1
        h1_keys = [k for k in self.last_bytes.keys() if '10.0.0.1' in k]
        for k in h1_keys:
            del self.last_bytes[k]
        
        self.logger.info("✅ H1 VoIP flows cleared (H3 bursty flows remain intact)")
                    
    def delete_flows_on_path(self, path, src_ip, dst_ip):
        """Hapus semua flow terkait di semua switch pada path tertentu"""
        for dpid in path:
            dp = self.datapaths.get(dpid)
            if not dp:
                continue

            ofp = dp.ofproto
            parser = dp.ofproto_parser

            # Hapus dengan match spesifik (UDP dari src ke dst)
            match_delete = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_ip,
                ipv4_dst=dst_ip,
                ip_proto=17  # UDP
            )

            # Hapus dengan DELETE_STRICT untuk pastikan exact match
            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofp.OFPFC_DELETE_STRICT,
                out_port=ofp.OFPP_ANY,
                out_group=ofp.OFPG_ANY,
                priority=self.reroute_priority,  # Priority harus sama dengan saat install
                match=match_delete
            )
            dp.send_msg(mod)

            # Juga hapus flow dengan priority rendah (default routing)
            mod_default = parser.OFPFlowMod(
                datapath=dp,
                command=ofp.OFPFC_DELETE_STRICT,
                out_port=ofp.OFPP_ANY,
                out_group=ofp.OFPG_ANY,
                priority=10,  # Priority untuk default routing
                match=match_delete
            )
            dp.send_msg(mod_default)
            
            self.logger.info(f"🧹 CLEARED FLOWS for {src_ip}->{dst_ip} on switch {dpid}")

    def revert_routing(self):
        """Kembalikan H1 ke path default (Spine 2), biarkan H3 tetap"""
        self.logger.warning("🔄 REVERTING H1 VoIP to default path (Spine 2)...")
        
        # 1. Hapus flow H1 reroute
        self.delete_h1_flows_only()
        
        # 2. Reset state - H3 MASIH ADA di path lama!
        self.congestion_active = False
        self.current_reroute_path = None
        
        # 3. H1 akan kembali via packet-in ke Spine 2
        self.logger.info("⏳ H1 will return to Spine 2 via packet-in...")
        
        # 4. Log
        self.insert_event_log(
            event_type="REROUTE_REVERT",
            description="H1 VoIP returned to default path (Spine 2)",
            trigger_value=0
        )

    def _trigger_packet_in(self, datapath):
        """Trigger packet-in untuk memaksa controller mengatur ulang routing"""
        # Ini akan memaksa switch mengirim packet ke controller
        # sehingga default routing akan di-setup ulang
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        
        # Kirim empty packet-out untuk trigger
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER)]
        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=ofproto.OFPP_CONTROLLER,
            actions=actions,
            data=b''
        )
        datapath.send_msg(out)

    def insert_event_log(self, event_type, description, trigger_value=0):
        conn = self.get_db_conn()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events
                (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, description, trigger_value))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"DB LOG ERROR: {e}")
            conn.rollback()
            conn.close()

    # =================================================================
    # 3. STANDARD HANDLERS (PacketIn, SwitchFeatures) - MIXED
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

        # 2. PROTOCOL EXTRACTION (INI BAGIAN PENTING)
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4) # Kita pakai nama ip_pkt
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # =================================================================
        # LOGIKA DEFAULT ROUTE (PINNING KE SPINE 2) - FIXED VARIABLE NAME
        # =================================================================
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Port 3 = Menuju Spine 2 (Jalur Tabrakan)
            COLLISION_PORT = 3 

            # Cek: Jika paket dari H1 (di DPID 4) ATAU H3 (di DPID 6)
            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                
                # SKIP jika sedang dalam mode reroute
                if self.congestion_active and src_ip == '10.0.0.1':
                    self.logger.info(f"⚠️ Skipping default routing for {src_ip} during reroute")
                    return
                
                actions = [parser.OFPActionOutput(COLLISION_PORT)]
                
                # Priority 10 (Lebih tinggi dari default 1, lebih rendah dari Reroute 30000)
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                
                # Add Flow agar switch ingat
                self.add_flow(datapath, 10, match, actions, msg.buffer_id, idle_timeout=0)
                
                # Kirim paketnya jalan (Packet Out)
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data
                
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                          in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                
                # STOP! Jangan lanjut ke logika flooding di bawah.
                return 
        # =================================================================

        # --- ARP HANDLER ---
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
    # 4. SINE WAVE LOGIC & STATS HANDLER (FULL ORIGINAL LOGIC)
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

    def get_target_total_bytes(self, elapsed_seconds):
        # Target: 13,000 to 19,800 bytes total per second per DPID
        min_val = 13000
        max_val = 19800
        
        # Midpoint & Amplitude
        mid = (max_val + min_val) / 2  # 16400
        amp = (max_val - min_val) / 2  # 3400
        
        # Period = 1 hour (3600 seconds)
        period = 3600
        phase = (elapsed_seconds % period) / period * 2 * math.pi
        
        sine_value = math.sin(phase)
        
        # Add small randomness (jitter)
        noise = random.uniform(-0.3, 0.3) 
        
        target = mid + (amp * sine_value) + (mid * 0.05 * noise)
        return int(max(min_val, min(max_val, target)))

    def insert_flow_data(self, flow_data):
        conn = self.get_db_conn()
        if not conn:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.flow_stats_
                (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                pkts_tx, pkts_rx, duration_sec, traffic_label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, flow_data)
            conn.commit()
            cur.close()
            conn.close()
        except:
            conn.rollback()
            conn.close()


    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Monitor REAL traffic H1→H2 (VoIP) dan H3→H2 (bursty) - REAL DATA ONLY!"""
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = datapath.id
        timestamp = datetime.now()
        
        for stat in body:
            if stat.priority == 0: 
                continue
                
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            # HANYA monitor H1→H2 dan H3→H2
            if dst_ip != '10.0.0.2':
                continue
                
            if src_ip not in ['10.0.0.1', '10.0.0.3']:
                continue
                
            # Hitung REAL traffic (dari counter switch)
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
            
            if delta_bytes <= 0:
                continue
                
            # === SIMPAN DATA REAL DARI SWITCH ===
            # JANGAN buat data dummy!
            conn = self.get_db_conn()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO traffic.flow_stats_
                        (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                        ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                        pkts_tx, pkts_rx, duration_sec, traffic_label)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        timestamp, dpid, src_ip, dst_ip,
                        match.get('eth_src', ''), match.get('eth_dst', ''),
                        match.get('ip_proto', 0),  # Ambil real protocol
                        match.get('udp_src', 0), match.get('udp_dst', 0),  # Ambil real ports
                        delta_bytes, delta_bytes,  # tx = rx (asumsi no loss di switch)
                        delta_packets, delta_packets,  # PAKAI delta_packets REAL!
                        1.0,  # duration 1 detik (karena polling setiap 1s)
                        'voip' if src_ip == '10.0.0.1' else 'bursty'
                    ))
                    conn.commit()
                    cur.close()
                    conn.close()
                    
                    # Debug log
                    if delta_bytes > 1000:
                        self.logger.info(f"📊 REAL DATA: {src_ip}→H2: {delta_bytes}B, {delta_packets} pkts on DPID {dpid}")
                        
                except Exception as e:
                    if conn and not conn.closed:
                        conn.rollback()
                        conn.close()