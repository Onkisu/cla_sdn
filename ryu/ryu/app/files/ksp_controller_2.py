#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (Fixed on User Base)
- Fix: Explicit Revert Logic (Force install default path)
- Fix: Traffic Doubling (Barrier Request + Anti-Flood in Packet-In)
- Base: User Provided Code (Ping Compatible)
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
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Threshold Congestion
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
        self.reroute_priority = 40000 # Sesuai kode user
        self.default_priority = 10    # Sesuai kode user

        self.last_congestion_time = 0
        self.cooldown_period = 30  
        self.LOWER_THRESHOLD_BPS = 150000 
        
        self.current_reroute_path = None
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("✅ VoIP Controller (FIXED VERSION) Started")

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
            self.net.clear()
            switches = get_switch(self, None)
            for s in switches: self.net.add_node(s.dp.id)
            links = get_link(self, None)
            for l in links:
                self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    # =================================================================
    # 2. FORECAST MONITORING & KSP LOGIC (FIXED)
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
                current_time = time.time()

                # TRIGGER REROUTE
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️ CONGESTION PREDICTED: {pred_bps:.0f} bps -> REROUTING!")
                    self.apply_ksp_reroute(k=3, trigger_val=pred_bps)
                    
                # TRIGGER REVERT
                elif self.congestion_active:
                    delta_t = current_time - self.last_congestion_time
                    if pred_bps < self.LOWER_THRESHOLD_BPS and delta_t > self.cooldown_period:
                        self.logger.info(f"✅ TRAFFIC STABLE ({pred_bps:.0f} bps) -> REVERTING.")
                        self.revert_routing()
                    
            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")
                if conn: conn.close()

    def send_barrier_request(self, datapath):
        """Helper untuk mengirim Barrier Request (Penting untuk cegah Doubling)"""
        parser = datapath.ofproto_parser
        req = parser.OFPBarrierRequest(datapath)
        datapath.send_msg(req)

    def apply_ksp_reroute(self, k=3, trigger_val=0):
        """Reroute H1 via Spine 1 (Port 1)"""
        self.logger.warning(f"🚀 REROUTING H1 VoIP to avoid congestion...")
        
        # 1. HAPUS FLOW LAMA + BARRIER (FIX DOUBLING)
        self.delete_h1_flows_only()
        
        # 2. Install Flow Baru di Leaf 1 (DPID 4)
        dp_leaf1 = self.datapaths.get(4)
        if dp_leaf1:
            parser = dp_leaf1.ofproto_parser
            
            # Target Path: Port 1 (Spine 1)
            out_port = 1 
            
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(out_port)]
            
            # Priority Tinggi (40000)
            self.add_flow(dp_leaf1, self.reroute_priority, match, actions, idle_timeout=0)
            
            self.logger.info(f"✅ Rerouted H1->H2: DPID 4 -> Port {out_port} (Spine 1)")
        

        dp_spine = self.datapaths.get(1)
        if dp_spine:
            parser = dp_spine.ofproto_parser
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2'
            )
            actions = [parser.OFPActionOutput(1)]  # port ke leaf tujuan (DPID 5)
            self.add_flow(dp_spine, self.reroute_priority, match, actions, idle_timeout=0)

        dp_leaf_dst = self.datapaths.get(5)
        if dp_leaf_dst:
            parser = dp_leaf_dst.ofproto_parser
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2'
            )
            actions = [parser.OFPActionOutput(1)]  # port ke H2
            self.add_flow(dp_leaf_dst, self.reroute_priority, match, actions, idle_timeout=0)



        # 3. Update State
        self.current_reroute_path = [4, 1, 5]
        self.congestion_active = True
        self.last_congestion_time = time.time()
        self.last_reroute_ts = time.time()
        
        # 4. Log
        self.insert_event_log("REROUTE_ACTIVE", f"H1 rerouted to Spine 1", trigger_val)

    def revert_routing(self):
        """Kembalikan H1 ke Spine 2 (Port 3) - EXPLICIT LOGIC"""
        self.logger.warning("🔄 REVERTING H1 VoIP to default path (Spine 2)...")
        
        # 1. HAPUS FLOW REROUTE + BARRIER
        self.delete_h1_flows_only()
        
        # 2. PASANG EXPLICIT DEFAULT FLOW (FIX REVERT GAGAL)
        # Jangan tunggu Packet-In, langsung pasang rule default!
        dp_leaf1 = self.datapaths.get(4)
        if dp_leaf1:
            parser = dp_leaf1.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            
            # Port 3 = Spine 2 (Default Path)
            actions = [parser.OFPActionOutput(3)] 
            
            # Priority 10 (Sesuai logic default kamu)
            self.add_flow(dp_leaf1, self.default_priority, match, actions, idle_timeout=0)
            self.logger.info("✅ Explicitly restored H1->H2 to Port 3 (Spine 2)")
        
        # REVERT di Spine (DPID 1) → arah default
        dp_spine = self.datapaths.get(1)
        if dp_spine:
            parser = dp_spine.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(3)]  # ke spine 2 / default
            self.add_flow(dp_spine, self.default_priority, match, actions, idle_timeout=0)


        # REVERT di Leaf tujuan (DPID 5)
        dp_leaf_dst = self.datapaths.get(5)
        if dp_leaf_dst:
            parser = dp_leaf_dst.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(1)]
            self.add_flow(dp_leaf_dst, self.default_priority, match, actions, idle_timeout=0)

        # 3. Reset State
        self.congestion_active = False
        self.current_reroute_path = None
        self.last_reroute_ts = time.time()

        
        self.insert_event_log("REROUTE_REVERT", "H1 VoIP restored to Spine 2", 0)

    def delete_h1_flows_only(self):
        """Hapus flow H1 + Barrier Request"""
        self.logger.info("🧹 Cleaning H1->H2 flows...")
        for dpid, dp in self.datapaths.items():
            parser = dp.ofproto_parser
            ofp = dp.ofproto
            
            # Match H1->H2
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            
            mod = parser.OFPFlowMod(
                datapath=dp, command=ofp.OFPFC_DELETE,
                out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                match=match
            )
            dp.send_msg(mod)
            
            # FIX TRAFFIC DOUBLING: Kirim Barrier
            self.send_barrier_request(dp)
        
        # Clear cache
               
        for k in list(self.last_bytes):
            if '10.0.0.1' in k:
                self.last_bytes[k] = self.last_bytes[k]  # NO-OP (intentional)


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
    # 3. STANDARD HANDLERS (PacketIn) - MODIFIED FOR SAFETY
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                if datapath.id == 4: self.revert_routing() # Init default rule
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
        # LOGIKA ROUTING: PINNING vs REROUTE (FIXED)
        # =================================================================
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Logic H1 (DPID 4) & H3 (DPID 6)
            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                
                # --- FIX: Jika sedang Reroute, JANGAN RETURN KOSONG ---
                # Kalau return kosong, dia bakal jatuh ke Flooding -> Doubling Traffic!
                if self.congestion_active and src_ip == '10.0.0.1':
                    actions = [parser.OFPActionOutput(1)] # Force ke Spine 1
                    match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                    self.add_flow(datapath, self.reroute_priority, match, actions, msg.buffer_id, idle_timeout=0)
                    
                    # Kirim packet out agar tidak drop
                    data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                              in_port=in_port, actions=actions, data=data)
                    datapath.send_msg(out)
                    return

                # Normal Condition (Spine 2 / Port 3)
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

        # --- STANDARD SWITCHING LOGIC (USER CODE) ---
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
    # 4. MONITORING (USER CODE)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
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

                # === PATCH 1: ignore stale stats right after reroute ===
        now = time.time()
        if hasattr(self, "last_reroute_ts"):
            if now - self.last_reroute_ts < 2.0:
                return   # DROP SEMUA stats (stale window)

        
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
            
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            delta_b = max(0, byte_count - prev_b)
            delta_p = max(0, packet_count - prev_p)
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
                except: pass