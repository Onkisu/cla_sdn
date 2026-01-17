#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting (FINAL MERGE)
- Reroute/Revert: FIXED (Priority 30000 vs 15000 vs 1)
- Data Logging: FIXED (Restore IP Learning from old code)
- Traffic: PURE (No Sine Wave/Synthetic)
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
        self.ip_to_mac = {}  # KUNCI LOGGING DB
        self.datapaths = {}
        self.net = nx.DiGraph() 
        self.last_bytes = {}
        self.start_time = time.time()
        
        # --- STATE CONGESTION ---
        self.congestion_active = False 
        
        # PRIORITAS (CRITICAL FIX FOR REROUTE)
        self.PRIO_REROUTE = 30000  
        self.PRIO_NORMAL  = 15000  
        self.PRIO_L2      = 1      

        self.last_congestion_time = 0
        self.cooldown_period = 30  
        self.LOWER_THRESHOLD_BPS = 90000 
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("VoIP Smart Controller (FINAL MERGE) Started")

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
        except: return None

    # =================================================================
    # 1. TOPOLOGY & PATH
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            self.net.clear()
            switches = get_switch(self, None)
            for s in switches:
                self.net.add_node(s.dp.id)
            links = get_link(self, None)
            for l in links:
                self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    def get_k_shortest_paths(self, src, dst, k=3):
        try:
            return list(nx.shortest_simple_paths(self.net, src, dst))[0:k]
        except: return []

    # =================================================================
    # 2. LOGIC REROUTE & REVERT (STABILIZED)
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

                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️  CONGESTION: {pred_bps:.0f} bps -> REROUTE!")
                    self.apply_ksp_reroute(k=3, trigger_val=pred_bps)
                    self.congestion_active = True
                    self.last_congestion_time = current_time

                elif self.congestion_active:
                    time_since = current_time - self.last_congestion_time
                    if pred_bps < self.LOWER_THRESHOLD_BPS and time_since > self.cooldown_period:
                        self.logger.info(f"✅ STABLE: {pred_bps:.0f} bps -> REVERT.")
                        self.revert_routing()
                        self.congestion_active = False
            except Exception: pass

    def apply_ksp_reroute(self, k=3, trigger_val=0):
        src_sw, dst_sw = 4, 5
        src_ip, dst_ip = '10.0.0.1', '10.0.0.2'
        
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=k)
        if len(paths) < k: return

        new_route = paths[k-1] 
        self.logger.info(f" >>> REROUTE ACTIVE: {new_route}")

        # 1. CLEANUP NORMAL (15000) di Source
        dp_src = self.datapaths.get(src_sw)
        if dp_src: self.del_flow(dp_src, self.PRIO_NORMAL, src_ip, dst_ip)

        # 2. INSTALL REROUTE (30000)
        for i in range(len(new_route) - 1):
            curr, next_dpid = new_route[i], new_route[i+1]
            if self.net.has_edge(curr, next_dpid):
                out_port = self.net[curr][next_dpid]['port']
                dp = self.datapaths.get(curr)
                if dp:
                    self.add_flow_explicit(dp, self.PRIO_REROUTE, src_ip, dst_ip, out_port)

        # 3. SECURE DESTINATION (Anti-Loop di DPID 5)
        # Pastikan DPID 5 membuang paket ke Host H2, bukan balik ke Spine
        dp_dst = self.datapaths.get(dst_sw)
        if dp_dst:
            # Resolusi port H2 dari ARP cache atau default port 1
            h2_mac = self.ip_to_mac.get(dst_ip)
            out_port_host = self.mac_to_port.get(dst_sw, {}).get(h2_mac, 1) 
            self.add_flow_explicit(dp_dst, self.PRIO_REROUTE, src_ip, dst_ip, out_port_host)

        self.insert_event_log("REROUTE_ACTIVE", json.dumps(new_route), trigger_val)

    def revert_routing(self):
        self.logger.info("🔄 REVERT TRIGGERED: Restoring Default Path...")
        src_sw, dst_sw = 4, 5
        src_ip, dst_ip = '10.0.0.1', '10.0.0.2'
        
        # 1. HAPUS REROUTE (30000) di SEMUA switch yang terlibat
        reroute_nodes = [4, 1, 5] 
        for dpid in reroute_nodes:
            dp = self.datapaths.get(dpid)
            if dp: self.del_flow(dp, self.PRIO_REROUTE, src_ip, dst_ip)

        # 2. FORCE NORMAL (15000) di Source agar masuk jalur lama
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=1)
        if paths:
            default_route = paths[0] 
            if len(default_route) > 1:
                next_dpid = default_route[1]
                if self.net.has_edge(src_sw, next_dpid):
                    out_port = self.net[src_sw][next_dpid]['port']
                    dp_src = self.datapaths.get(src_sw)
                    if dp_src:
                        self.add_flow_explicit(dp_src, self.PRIO_NORMAL, src_ip, dst_ip, out_port)
                        self.logger.info(f"   >>> Default Path ENFORCED: DPID {src_sw} -> Port {out_port}")

        self.insert_event_log("REROUTE_REVERT", "Restored Default Path", 0)

    # --- Helper Functions ---
    def add_flow_explicit(self, datapath, priority, src_ip, dst_ip, out_port):
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ip_proto=17, ipv4_src=src_ip, ipv4_dst=dst_ip)
        actions = [parser.OFPActionOutput(out_port)]
        self.add_flow(datapath, priority, match, actions, idle_timeout=0)

    def del_flow(self, datapath, priority, src_ip, dst_ip):
        ofp = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ip_proto=17, ipv4_src=src_ip, ipv4_dst=dst_ip)
        mod = parser.OFPFlowMod(
            datapath=datapath, command=ofp.OFPFC_DELETE_STRICT,
            out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
            priority=priority, match=match
        )
        datapath.send_msg(mod)

    # =================================================================
    # 3. STANDARD HANDLERS (PacketIn)
    # =================================================================
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
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, 
                                    priority=priority, match=match, instructions=inst, idle_timeout=idle_timeout)
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
        
        dst = eth.dst
        src = eth.src
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # --- FIX UTAMA AGAR DB MASUK: SNOOPING IP DARI PACKET IN ---
        # Ini yang hilang di versi sebelumnya!
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            self.ip_to_mac[ip_pkt.src] = src
        # -----------------------------------------------------------

        # ARP Handler
        arp_pkt = pkt.get_protocol(arp.arp)
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
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, 
                                          in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                datapath.send_msg(out)
                return

        # L2 Switching (Priority 1)
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = []
        is_leaf = dpid >= 4
        if out_port == ofproto.OFPP_FLOOD:
            if is_leaf and in_port <= 3: 
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD)) 
            else:
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
        else:
            actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, self.PRIO_L2, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, 
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 4. MONITORING (PURE D-ITG + FIX IP RESOLVE)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
            hub.sleep(1)

    # Helper untuk mengembalikan IP dari MAC (Wajib ada!)
    def _resolve_ip(self, mac):
        if not mac: return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: return ip
        return None

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = format(ev.msg.datapath.id, '016x')
        timestamp = datetime.now()
        conn = self.get_db_conn()
        
        for stat in body:
            if stat.priority == 0: continue 

            match = stat.match
            
            # --- RESOLVE IP DARI MAC JIKA MATCH IP KOSONG ---
            src_mac = match.get('eth_src')
            dst_mac = match.get('eth_dst')
            src_ip = match.get('ipv4_src') or self._resolve_ip(src_mac)
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(dst_mac)
            # ------------------------------------------------
            
            # Sekarang aman untuk cek ini
            if not src_ip or not dst_ip: continue
            
            in_port = match.get('in_port', 0) 
            flow_key = f"{dpid}-{src_ip}-{dst_ip}-{in_port}-{stat.priority}"
            
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            if flow_key in self.last_bytes:
                last_b, last_p = self.last_bytes[flow_key]
                delta_bytes = max(0, byte_count - last_b)
                delta_pkts = max(0, packet_count - last_p)
            else:
                delta_bytes = byte_count
                delta_pkts = packet_count
                
            self.last_bytes[flow_key] = (byte_count, packet_count)

            if delta_bytes > 0 and conn:
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
                        src_mac, dst_mac,
                        match.get('ip_proto', 0), match.get('udp_src',0), match.get('udp_dst',0),
                        delta_bytes, delta_bytes, 
                        delta_pkts, delta_pkts,
                        1.0,
                        'voip' if src_ip == '10.0.0.1' else 'bursty'
                    ))
                    conn.commit()
                    cur.close()
                except Exception:
                    conn.rollback()
        
        if conn: conn.close()

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
            conn.commit()
            cur.close()
            conn.close()
        except: pass