#!/usr/bin/env python3
"""
Ryu SDN Controller: Enterprise Grade (VoIP Reroute + Stable Ping)
- Logic: Make-Before-Break (Seamless Reroute)
- Network: Proxy ARP (Instant Ping / No Loop)
- Database: Async Logging
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
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
REROUTE_COOKIE = 0xDEADBEEF  # Penanda Khusus Flow Reroute

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_to_mac = {}    # ARP Cache
        self.datapaths = {}
        self.net = nx.DiGraph() 
        self.last_bytes = {}
        
        # State Management
        self.congestion_active = False 
        self.last_congestion_time = 0
        self.cooldown_period = 30  
        self.LOWER_THRESHOLD_BPS = 150000
        self.current_reroute_path = None
        
        # Threads
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("🟢 CONTROLLER READY: Proxy ARP & AI Reroute Active")

    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception:
            return None

    # =================================================================
    # 1. TOPOLOGY & NETWORK GRAPH
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(3)
            copy_net = nx.DiGraph()
            switches = get_switch(self, None)
            links = get_link(self, None)
            
            for s in switches: copy_net.add_node(s.dp.id)
            for l in links:
                copy_net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                copy_net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)
            
            self.net = copy_net 

    # =================================================================
    # 2. INTELLIGENT REROUTING LOGIC
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(1)
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

                is_burst = pred_bps > BURST_THRESHOLD_BPS
                is_safe = pred_bps < self.LOWER_THRESHOLD_BPS
                can_revert = (now - self.last_congestion_time) > self.cooldown_period

                if is_burst and not self.congestion_active:
                    self.logger.warning(f"🚀 DETEKSI BURST ({pred_bps:.0f} bps). Switch ke Jalur Alternatif...")
                    self.activate_reroute(pred_bps)
                    self.congestion_active = True
                    self.last_congestion_time = now

                elif is_safe and self.congestion_active and can_revert:
                    self.logger.info(f"✅ TRAFFIC NORMAL ({pred_bps:.0f} bps). Restore Jalur Utama.")
                    self.deactivate_reroute()
                    self.congestion_active = False

            except Exception as e:
                self.logger.error(f"Forecast Error: {e}")

    def activate_reroute(self, trigger_val):
        try:
            # Cari jalur dari Leaf 1 (4) ke Leaf 2 (5) TANPA melewati Spine 1 (1)
            paths = list(nx.shortest_simple_paths(self.net, 4, 5))
            backup_path = next((p for p in paths if 1 not in p), None)
            
            if not backup_path: return

            self.logger.info(f"🛣️  Path Baru: {backup_path}")

            # Install Flow Reverse Order (Egress -> Ingress)
            for i in range(len(backup_path) - 1, 0, -1):
                src_dpid = backup_path[i-1]
                dst_dpid = backup_path[i]
                
                out_port = self.net[src_dpid][dst_dpid]['port']
                dp = self.datapaths.get(src_dpid)
                
                if dp:
                    self.add_reroute_flow(dp, out_port)
                    self.send_barrier_request(dp)

            self.current_reroute_path = backup_path
            self.log_event("REROUTE_ACTIVE", f"Path: {backup_path}", trigger_val)

        except Exception as e:
            self.logger.error(f"Gagal Reroute: {e}")

    def deactivate_reroute(self):
        # Hapus flow reroute (cookie deadbeef)
        if self.current_reroute_path:
            for dpid in self.current_reroute_path:
                dp = self.datapaths.get(dpid)
                if dp:
                    self.del_reroute_flow(dp)
        
        self.current_reroute_path = None
        self.log_event("REROUTE_REVERT", "Kembali ke jalur default", 0)

    # =================================================================
    # 3. FLOW OPERATIONS
    # =================================================================
    def add_reroute_flow(self, datapath, out_port):
        parser = datapath.ofproto_parser
        # Match H1 -> H2
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src="10.0.0.1", ipv4_dst="10.0.0.2")
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # Priority 30000 + Cookie
        mod = parser.OFPFlowMod(datapath=datapath, priority=30000, match=match,
                                instructions=inst, cookie=REROUTE_COOKIE)
        datapath.send_msg(mod)

    def del_reroute_flow(self, datapath):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        # Delete by Cookie
        mod = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_DELETE,
                                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY,
                                cookie=REROUTE_COOKIE, cookie_mask=0xFFFFFFFFFFFFFFFF)
        datapath.send_msg(mod)

    def send_barrier_request(self, datapath):
        datapath.send_msg(datapath.ofproto_parser.OFPBarrierRequest(datapath))

    def log_event(self, evt, desc, val):
        conn = self.get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value) VALUES (NOW(), %s, %s, %s)", (evt, desc, val))
                conn.commit(); conn.close()
            except: pass

    # =================================================================
    # 4. PACKET HANDLING (ARP PROXY & ICMP)
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
        # Send everything to controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
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
        dst = eth.dst; src = eth.src

        # --- [FIX] PROXY ARP (MENCEGAH LOOP & MEMPERBAIKI PING) ---
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            self._handle_arp(datapath, in_port, pkt)
            return
        # ----------------------------------------------------------

        # IP Handling (ICMP/UDP/TCP)
        if eth.ethertype == ether_types.ETH_TYPE_IP:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)
            # Default Route Logic (Pinning ke Spine 1 untuk simulasi tabrakan)
            # H1 -> H2 lewat Spine 1 secara default
            if ip_pkt and dpid in [4, 6] and ip_pkt.dst == '10.0.0.2':
                # Force ke Port 1 (Asumsi Uplink ke Spine 1)
                actions = [parser.OFPActionOutput(1)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=ip_pkt.src, ipv4_dst='10.0.0.2')
                self.add_flow(datapath, 10, match, actions, msg.buffer_id)
                
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                        in_port=in_port, actions=actions, data=msg.data)
                datapath.send_msg(out)
                return

        # L2 Learning Standard
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id)

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=msg.data)
        datapath.send_msg(out)

    def _handle_arp(self, datapath, port, pkt):
        """Menangani ARP: Reply jika tahu, Flood jika tidak (tapi aman)"""
        pkt_arp = pkt.get_protocol(arp.arp)
        src_ip = pkt_arp.src_ip
        src_mac = pkt_arp.src_mac
        dst_ip = pkt_arp.dst_ip

        self.ip_to_mac[src_ip] = src_mac # Belajar IP->MAC

        if pkt_arp.opcode == arp.ARP_REQUEST:
            if dst_ip in self.ip_to_mac:
                # Proxy Reply (Controller menjawab mewakili Host)
                dst_mac = self.ip_to_mac[dst_ip]
                e = ethernet.ethernet(dst=src_mac, src=dst_mac, ethertype=ether_types.ETH_TYPE_ARP)
                a = arp.arp(opcode=arp.ARP_REPLY, src_mac=dst_mac, src_ip=dst_ip,
                            dst_mac=src_mac, dst_ip=src_ip)
                p = packet.Packet(); p.add_protocol(e); p.add_protocol(a); p.serialize()
                
                actions = [datapath.ofproto_parser.OFPActionOutput(port)]
                out = datapath.ofproto_parser.OFPPacketOut(
                    datapath=datapath, buffer_id=datapath.ofproto.OFP_NO_BUFFER,
                    in_port=datapath.ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                datapath.send_msg(out)
            else:
                # Flood jika belum tahu (tapi hanya sekali)
                actions = [datapath.ofproto_parser.OFPActionOutput(datapath.ofproto.OFPP_FLOOD)]
                out = datapath.ofproto_parser.OFPPacketOut(
                    datapath=datapath, buffer_id=datapath.ofproto.OFP_NO_BUFFER,
                    in_port=port, actions=actions, data=pkt.data)
                datapath.send_msg(out)

    # =================================================================
    # 5. MONITORING TRAFFIC
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
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
                    prev_bytes = self.last_bytes.get(key, 0)
                    delta = max(0, stat.byte_count - prev_bytes)
                    self.last_bytes[key] = stat.byte_count

                    cur.execute("""
                        INSERT INTO traffic.flow_stats_ 
                        (timestamp, dpid, src_ip, dst_ip, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1.0)
                    """, (timestamp, dpid, src, dst, delta, delta, stat.packet_count, stat.packet_count))
            conn.commit(); cur.close(); conn.close()
        except: pass