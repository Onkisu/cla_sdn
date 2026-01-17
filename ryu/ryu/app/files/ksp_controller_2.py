#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FIXED VERSION)
- Fix: Threshold Hysteresis (Mencegah Flapping)
- Fix: Cache Clearing (Menghapus statistik lama saat reroute)
- Fix: Database Safety & Revert Logic
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
from ryu.topology import api
from ryu.topology.api import get_switch, get_link
import networkx as nx
import psycopg2
from datetime import datetime
import time

# PostgreSQL Configuration
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Threshold Congestion (Batas Atas Trigger Macet)
BURST_THRESHOLD_BPS = 120000 

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        # --- INIT DATA STRUCTURES ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() 
        self.last_bytes = {}
        self.start_time = time.time()
        
        # --- STATE CONGESTION ---
        self.congestion_active = False 
        self.reroute_priority = 40000 
        self.default_priority = 10    

        self.last_congestion_time = 0
        self.cooldown_period = 30  
        
        # ### FIX 1: LOWER THRESHOLD HARUS LEBIH KECIL DARI BURST ###
        # Agar tidak flapping (Oscillation). 80k < 120k.
        self.LOWER_THRESHOLD_BPS = 80000 
        
        self.current_reroute_path = None
        self.last_reroute_ts = 0 # Timestamp terakhir reroute
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("✅ VoIP Controller (FIXED FINAL) Started")
        self.logger.info(f"⚙️ Config: Burst={BURST_THRESHOLD_BPS}, Revert={self.LOWER_THRESHOLD_BPS}")

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
            return None

    # =================================================================
    # 1. TOPOLOGY DISCOVERY
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            try:
                self.net.clear()
                switches = get_switch(self, None)
                for s in switches: self.net.add_node(s.dp.id)
                links = get_link(self, None)
                for l in links:
                    self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)
            except Exception as e:
                self.logger.error(f"Topology Error: {e}")

    # =================================================================
    # 2. FORECAST MONITORING & REROUTE LOGIC
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)
            conn = self.get_db_conn()
            if not conn: continue
            try:
                cur = conn.cursor()
                # Ambil prediksi terakhir
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                result = cur.fetchone()
                cur.close()
                conn.close()

                if not result: continue
                pred_bps = result[0]
                current_time = time.time()

                # --- LOGIC TRIGGER (FIXED) ---
                
                # 1. Trigger REROUTE (Jika Trafik Tinggi & Belum Reroute)
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️ CONGESTION PREDICTED: {pred_bps:.0f} bps (> {BURST_THRESHOLD_BPS}) -> REROUTING!")
                    self.apply_ksp_reroute(k=3, trigger_val=pred_bps)
                    
                # 2. Trigger REVERT (Jika Trafik Rendah & Sedang Reroute & Cooldown Selesai)
                elif self.congestion_active:
                    delta_t = current_time - self.last_congestion_time
                    if pred_bps < self.LOWER_THRESHOLD_BPS and delta_t > self.cooldown_period:
                        self.logger.info(f"✅ TRAFFIC STABLE ({pred_bps:.0f} bps < {self.LOWER_THRESHOLD_BPS}) -> REVERTING.")
                        self.revert_routing()
                    
            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")
                if conn: conn.close()

    def send_barrier_request(self, datapath):
        """Mencegah Packet-In memproses paket sebelum flow terpasang"""
        parser = datapath.ofproto_parser
        req = parser.OFPBarrierRequest(datapath)
        datapath.send_msg(req)

    def apply_ksp_reroute(self, k=3, trigger_val=0):
        """
        Static Reroute: Memindahkan H1 ke Spine 1 (Port 1).
        Disebut KSP untuk kompatibilitas, namun implementasi static untuk stabilitas.
        """
        self.logger.warning(f"🚀 REROUTING H1 VoIP to Spine 1...")
        
        # 1. Bersihkan Flow Lama & Cache
        self.delete_h1_flows_only()
        
        # 2. Install Flow Baru di Leaf 1 (DPID 4) -> Ke Spine 1 (Port 1)
        dp_leaf1 = self.datapaths.get(4)
        if dp_leaf1:
            parser = dp_leaf1.ofproto_parser
            out_port = 1 # Port ke Spine 1
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(out_port)]
            self.add_flow(dp_leaf1, self.reroute_priority, match, actions, idle_timeout=0)
            self.logger.info(f"   + Rule Installed: DPID 4 -> Port {out_port} (Spine 1)")

        # 3. Install Flow di Spine 1 (DPID 1) -> Ke Leaf Tujuan (DPID 5)
        dp_spine = self.datapaths.get(1)
        if dp_spine:
            parser = dp_spine.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(1)]  # Port ke Leaf tujuan
            self.add_flow(dp_spine, self.reroute_priority, match, actions, idle_timeout=0)

        # 4. Install Flow di Leaf Tujuan (DPID 5) -> Ke Host
        dp_leaf_dst = self.datapaths.get(5)
        if dp_leaf_dst:
            parser = dp_leaf_dst.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(1)]  # Port ke H2
            self.add_flow(dp_leaf_dst, self.reroute_priority, match, actions, idle_timeout=0)

        # 5. Update State
        self.current_reroute_path = [4, 1, 5]
        self.congestion_active = True
        self.last_congestion_time = time.time()
        self.last_reroute_ts = time.time()
        
        self.insert_event_log("REROUTE_ACTIVE", "H1 rerouted to Spine 1", trigger_val)

    def revert_routing(self):
        """Kembalikan H1 ke Spine 2 (Port 3)"""
        self.logger.warning("🔄 REVERTING H1 VoIP to default path (Spine 2)...")
        
        # 1. HAPUS FLOW REROUTE
        self.delete_h1_flows_only()
        
        # 2. Install Explicit Default Flow (Leaf 1 -> Spine 2 / Port 3)
        dp_leaf1 = self.datapaths.get(4)
        if dp_leaf1:
            parser = dp_leaf1.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(3)] # Port 3 = Default / Spine 2
            self.add_flow(dp_leaf1, self.default_priority, match, actions, idle_timeout=0)
            self.logger.info("   + Revert Rule: DPID 4 -> Port 3 (Spine 2)")
        
        # 3. Reset State
        self.congestion_active = False
        self.current_reroute_path = None
        self.last_reroute_ts = time.time()
        
        self.insert_event_log("REROUTE_REVERT", "H1 VoIP restored to Spine 2", 0)

    def delete_h1_flows_only(self):
        """Hapus flow H1 + Clear Cache"""
        self.logger.info("🧹 Cleaning H1->H2 flows and stats...")
        for dpid, dp in self.datapaths.items():
            parser = dp.ofproto_parser
            ofp = dp.ofproto
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            mod = parser.OFPFlowMod(
                datapath=dp, command=ofp.OFPFC_DELETE,
                out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                match=match
            )
            dp.send_msg(mod)
            self.send_barrier_request(dp)
        
        # ### FIX 2: BENAR-BENAR MENGHAPUS CACHE ###
        # Menggunakan 'del' agar perhitungan delta bytes reset
        keys_to_delete = [k for k in self.last_bytes if '10.0.0.1' in k]
        for k in keys_to_delete:
            del self.last_bytes[k] 
        self.logger.info(f"   + Cleared {len(keys_to_delete)} stat entries.")

    def insert_event_log(self, event_type, description, trigger_value=0):
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, description, trigger_value))
            conn.commit()
            conn.close()
        except: pass

    # =================================================================
    # 3. HANDLERS (PacketIn)
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                # Auto-init rule untuk Leaf 1 saat konek
                if datapath.id == 4: 
                    self.logger.info("DPID 4 Connected: Init Default Rule.")
                    self.revert_routing() 
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
        
        dst = eth.dst
        src = eth.src
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # =================================================================
        # LOGIKA ROUTING: PINNING vs REROUTE
        # =================================================================
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Logic H1 (DPID 4) & H3 (DPID 6)
            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                
                # JIKA CONGESTION ACTIVE (Spine 1)
                if self.congestion_active and src_ip == '10.0.0.1':
                    actions = [parser.OFPActionOutput(1)] # Force ke Spine 1
                    match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                    # Install High Priority Flow
                    self.add_flow(datapath, self.reroute_priority, match, actions, msg.buffer_id, idle_timeout=0)
                    
                    # Packet Out Langsung
                    data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                              in_port=in_port, actions=actions, data=data)
                    datapath.send_msg(out)
                    return

                # NORMAL CONDITION (Spine 2 / Port 3)
                COLLISION_PORT = 3 
                actions = [parser.OFPActionOutput(COLLISION_PORT)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                self.add_flow(datapath, self.default_priority, match, actions, msg.buffer_id, idle_timeout=0)
                
                data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                          in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return 

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

        # --- L2 SWITCHING ---
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
    # 4. MONITORING
    # =================================================================
    def _monitor_traffic(self):
        while True:
            # Gunakan list() untuk menghindari error 'dict changed size during iteration'
            for dp in list(self.datapaths.values()):
                if dp.id:
                    try:
                        req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                        dp.send_msg(req)
                    except: pass
            hub.sleep(1)

    def _resolve_ip(self, mac):
        if not mac: return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: return ip
        return None

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()

        # === PATCH: Ignore Stale Stats after Reroute ===
        # Beri jeda 3 detik setelah reroute sebelum menerima stats lagi
        # agar counter yang belum ter-reset di switch tidak terbaca sebagai lonjakan.
        if time.time() - self.last_reroute_ts < 3.0:
            return 
        
        for stat in body:
            if stat.priority == 0: continue
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            if dst_ip != '10.0.0.2': continue
            if src_ip not in ['10.0.0.1', '10.0.0.3']: continue
                
            flow_key = f"{dpid}-{src_ip}-{dst_ip}"
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            # Hitung Delta
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            
            # Jika counter reset (karena flow baru diinstall), anggap delta = current
            if byte_count < prev_b:
                delta_b = byte_count
                delta_p = packet_count
            else:
                delta_b = byte_count - prev_b
                delta_p = packet_count - prev_p
            
            # Update Cache
            self.last_bytes[flow_key] = (byte_count, packet_count)
            
            if delta_b <= 0: continue
            
            ip_proto = match.get('ip_proto') or 17
            tp_src = match.get('tcp_src') or match.get('udp_src') or 0
            tp_dst = match.get('tcp_dst') or match.get('udp_dst') or 0
            
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
                    """, (timestamp, dpid, src_ip, dst_ip, 
                          match.get('eth_src',''), match.get('eth_dst',''),
                          ip_proto, tp_src, tp_dst, delta_b, delta_b, 
                          delta_p, delta_p, 1.0, 
                          'voip' if src_ip == '10.0.0.1' else 'bursty'))
                    conn.commit()
                    conn.close()
                except Exception as e: 
                    pass