#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting (Fixed & Industrial)
- Base Logic: User's Proven Packet-In (Ping Works)
- Features: Automatic Make-Before-Break Rerouting
- DB: Async PostgreSQL Logging
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
HYSTERESIS_LOWER_BPS = 80000   # Traffic harus turun ke angka ini baru kembali
COOLDOWN_PERIOD_SEC = 15       # Waktu tunggu minimal antar switch

# KUNCI REROUTING:
PRIORITY_NORMAL  = 10          # Priority Default (Rendah)
PRIORITY_REROUTE = 500         # Priority Reroute (Tinggi) - Menimpa Default
COOKIE_REROUTE   = 0xDEADBEEF  # Penanda khusus flow reroute

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        # --- Standard Init ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() 
        self.last_bytes = {}
        
        # --- State Machine ---
        self.congestion_active = False 
        self.last_state_change = 0
        
        self.connect_database()
        
        # --- Threads ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast_smart)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("🟢 VoIP Controller Ready (User Logic + Industrial Reroute)")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("✅ Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}")
            self.db_conn = None
    
    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except: return None

    # =================================================================
    # 1. TOPOLOGY (NETWORKX)
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(3)
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
    # 2. SMART FORECAST WATCHDOG
    # =================================================================
    def _monitor_forecast_smart(self):
        self.logger.info("👁️  AI Watchdog Active...")
        while True:
            hub.sleep(1.0)
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                res = cur.fetchone()
                cur.close(); conn.close()

                if not res: continue
                pred_bps = float(res[0])
                now = time.time()
                elapsed = now - self.last_state_change

                # --- LOGIC TRIGGER ---
                # Jika NORMAL -> cek BURST
                if not self.congestion_active:
                    if pred_bps > BURST_THRESHOLD_BPS:
                        self.logger.warning(f"🚀 AI PREDICTION: Burst ({pred_bps:,.0f} bps). REROUTING H1!")
                        self.perform_industrial_reroute(pred_bps)
                        self.congestion_active = True
                        self.last_state_change = now

                # Jika REROUTE -> cek NORMAL (dengan Hysteresis)
                else:
                    if pred_bps < HYSTERESIS_LOWER_BPS and elapsed > COOLDOWN_PERIOD_SEC:
                        self.logger.info(f"✅ AI PREDICTION: Stable ({pred_bps:,.0f} bps). RESTORING PATH.")
                        self.perform_industrial_revert(pred_bps)
                        self.congestion_active = False
                        self.last_state_change = now

            except Exception as e:
                self.logger.error(f"Forecast Error: {e}")
                if conn: conn.close()

    # =================================================================
    # 3. REROUTING CORE (KEREN & SEAMLESS)
    # =================================================================
    def perform_industrial_reroute(self, val):
        """
        Pindah jalur H1->H2 ke Spine 2 (atau jalur alternatif lain).
        Menggunakan Cookie & Priority 500.
        """
        src, dst = 4, 5  # Asumsi H1 di Switch 4, H2 di Switch 5
        
        try:
            # 1. Cari jalur
            paths = list(nx.shortest_simple_paths(self.net, src, dst))
            # Cari jalur yang lewat Spine 2 (DPID 2)
            # Atau jalur yang TIDAK lewat Spine 1 (DPID 1)
            # Dan TIDAK lewat Spine 3 (DPID 3 - karena log anda menunjukkan ini macet)
            alt_path = None
            for p in paths:
                if 2 in p: # Prioritaskan Spine 2
                    alt_path = p
                    break
            
            if not alt_path and len(paths) > 1: alt_path = paths[1] # Fallback path ke-2
            
            if not alt_path: return

            self.logger.info(f"🛣️  Installing Path: {alt_path}")

            # 2. Install Flow (Make-Before-Break: Reverse Order)
            for i in range(len(alt_path) - 1, 0, -1):
                u, v = alt_path[i-1], alt_path[i]
                if self.net.has_edge(u, v):
                    out_port = self.net[u][v]['port']
                    dp = self.datapaths.get(u)
                    if dp:
                        # Install Flow Priority Tinggi (500)
                        self._install_reroute_flow(dp, out_port)
                        # Barrier Request (Tunggu switch selesai)
                        dp.send_msg(dp.ofproto_parser.OFPBarrierRequest(dp))

            self.insert_event_log("REROUTE_ACTIVE", f"Path: {alt_path}", val)

        except Exception as e:
            self.logger.error(f"Reroute Fail: {e}")

    def perform_industrial_revert(self, val):
        """
        Hapus flow Priority 500. Traffic otomatis jatuh ke Priority 10 (Default).
        """
        self.logger.info("🔙 Reverting to Default Path...")
        for dp in self.datapaths.values():
            self._delete_reroute_flow(dp) # Hapus berdasarkan Cookie
            dp.send_msg(dp.ofproto_parser.OFPBarrierRequest(dp))
        
        self.insert_event_log("REROUTE_REVERT", "Back to Default", val)

    # --- Low Level Flow ---
    def _install_reroute_flow(self, dp, port):
        parser = dp.ofproto_parser
        # Match HANYA H1->H2 (VoIP)
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(port)]
        inst = [parser.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        mod = parser.OFPFlowMod(dp, priority=PRIORITY_REROUTE, match=match, 
                                instructions=inst, cookie=COOKIE_REROUTE)
        dp.send_msg(mod)

    def _delete_reroute_flow(self, dp):
        parser = dp.ofproto_parser
        # Hapus Flow dengan Cookie REROUTE
        mod = parser.OFPFlowMod(dp, command=dp.ofproto.OFPFC_DELETE, 
                                out_port=dp.ofproto.OFPP_ANY, out_group=dp.ofproto.OFPG_ANY,
                                cookie=COOKIE_REROUTE, cookie_mask=0xFFFFFFFFFFFFFFFF)
        dp.send_msg(mod)

    def insert_event_log(self, evt, desc, val):
        conn = self.get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value) VALUES (NOW(), %s, %s, %s)", (evt, desc, val))
                conn.commit(); conn.close()
            except: pass

    # =================================================================
    # 4. PACKET IN HANDLER (EXACT COPY FROM YOUR CODE - PING WORKS)
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

        # 2. PROTOCOL EXTRACTION
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # =================================================================
        # LOGIKA DEFAULT ROUTE (PINNING KE SPINE 1 - FIXED FOR DEFAULT)
        # =================================================================
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Kita arahkan default ke Spine 1 (Node 1) jika bisa ditemukan portnya
            # Ini akan ditimpa oleh Reroute Priority 500 saat burst
            if (dpid == 4 or dpid == 6) and src_ip in ['10.0.0.1', '10.0.0.3'] and dst_ip == '10.0.0.2':
                # Jangan set flow jika sedang reroute (biar yg priority 500 yg handle)
                # Tapi kalaupun di set, priority 10 akan kalah. Aman.
                
                # Cari port ke Spine 1 (dpid 1)
                out_port_default = None
                if self.net.has_edge(dpid, 1):
                    out_port_default = self.net[dpid][1]['port']
                
                # Jika ketemu port ke spine 1, gunakan. Jika tidak, fallback ke collision port user (3)
                final_port = out_port_default if out_port_default else 3
                
                actions = [parser.OFPActionOutput(final_port)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                
                # Priority 10 (Default)
                self.add_flow(datapath, PRIORITY_NORMAL, match, actions, msg.buffer_id, idle_timeout=0)
                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                        in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return 

        # --- ARP HANDLER (USER CODE) ---
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

        actions = [parser.OFPActionOutput(out_port)] # User logic simple flood

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 5. MONITORING
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                dp.send_msg(dp.ofproto_parser.OFPFlowStatsRequest(dp))
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body; dpid = ev.msg.datapath.id
        timestamp = datetime.now()
        conn = self.get_db_conn()
        if not conn: return

        try:
            cur = conn.cursor()
            for stat in body:
                if stat.priority == 0: continue
                match = stat.match
                src = match.get('ipv4_src'); dst = match.get('ipv4_dst')
                
                if src and dst and dst == '10.0.0.2':
                    key = f"{dpid}-{src}-{dst}"
                    prev = self.last_bytes.get(key, 0)
                    delta = max(0, stat.byte_count - prev)
                    self.last_bytes[key] = stat.byte_count
                    
                    if delta == 0: continue # Skip zero traffic

                    label = 'voip' if src == '10.0.0.1' else 'bursty'
                    cur.execute("""
                        INSERT INTO traffic.flow_stats_ 
                        (timestamp, dpid, src_ip, dst_ip, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec, traffic_label)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1.0, %s)
                    """, (timestamp, dpid, src, dst, delta, delta, stat.packet_count, stat.packet_count, label))
            conn.commit(); cur.close(); conn.close()
        except: pass