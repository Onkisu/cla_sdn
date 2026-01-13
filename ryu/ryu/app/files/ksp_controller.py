#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FIXED & FULL FEATURES)
- Features: K-Shortest Path Rerouting based on DB Forecast
- Features: Sine Wave Traffic Generation with Spike Detection
- Features: Full IP Resolution and Database Logging
- FIX: Dual Database Connection (No Locking/Delay)
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
BURST_THRESHOLD_BPS = 250000 

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        # --- INIT DATA STRUCTURES ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() # NetworkX Graph
        
        # --- PERBAIKAN: DUAL DATABASE CONNECTION ---
        self.conn_traffic = None # Khusus Insert Flow (Cepat)
        self.conn_logic   = None # Khusus Read Forecast & Log Event (Aman)
        
        self.last_bytes = {}
        self.start_time = time.time()
        
        # --- STATE CONGESTION ---
        self.congestion_active = False 
        self.reroute_priority = 100 
        
        self.connect_databases()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("VoIP Smart Controller (KSP + Full Traffic Monitor + Dual DB) Started")

    def connect_databases(self):
        """Membuat 2 koneksi terpisah agar traffic monitor tidak mengganggu forecast logic"""
        try:
            # 1. Koneksi untuk Traffic (Insert Flow)
            self.conn_traffic = psycopg2.connect(**DB_CONFIG)
            self.conn_traffic.autocommit = True 
            
            # 2. Koneksi untuk Logic (Read Forecast & Log Event)
            self.conn_logic = psycopg2.connect(**DB_CONFIG)
            self.conn_logic.autocommit = True
            
            self.logger.info("Database connected (Dual Channel Mode)")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}. Running without DB storage.")

    # =================================================================
    # 1. TOPOLOGY DISCOVERY (NETWORKX)
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
    # 2. FORECAST MONITORING & KSP LOGIC
    # =================================================================
    def _monitor_forecast(self):
        """Mengecek tabel forecast dan memicu rerouting"""
        while True:
            hub.sleep(2)
            # Gunakan conn_logic (aman dari traffic monitor)
            if self.conn_logic:
                try:
                    # Gunakan 'with' agar cursor otomatis close
                    with self.conn_logic.cursor() as cursor:
                        query = "SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1"
                        cursor.execute(query)
                        result = cursor.fetchone()

                        if result:
                            pred_bps = result[0]
                            
                            if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                                self.logger.warning(f"\n⚠️  PREDICTION: CONGESTION ({pred_bps:,.0f} bps) DETECTED")
                                self.apply_ksp_reroute(k=2, trigger_val=pred_bps)
                                self.congestion_active = True
                                
                            elif pred_bps < BURST_THRESHOLD_BPS and self.congestion_active:
                                self.logger.info(f"\n✅ PREDICTION: NORMAL ({pred_bps:,.0f} bps). Recovering...")
                                self.revert_routing(k=2, trigger_val=pred_bps)
                                self.congestion_active = False
                except Exception as e:
                    self.logger.error(f"Forecast Monitor Error: {e}")
                    # Reconnect logic jika putus
                    try: self.conn_logic = psycopg2.connect(**DB_CONFIG); self.conn_logic.autocommit=True
                    except: pass

    def get_k_shortest_paths(self, src_dpid, dst_dpid, k=2):
        try:
            return list(nx.shortest_simple_paths(self.net, src_dpid, dst_dpid))[0:k]
        except nx.NetworkXNoPath:
            return []
        except Exception:
            return []

    def apply_ksp_reroute(self, k=2, trigger_val=0):
        src_sw = 4 # Leaf 1
        dst_sw = 5 # Leaf 2
        
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=k)
        
        if len(paths) < k:
            self.logger.error("Not enough paths for KSP rerouting")
            return

        current_route = paths[0]
        new_route     = paths[k-1]
        
        # --- LOGGING VISUAL ---
        self.logger.info("-" * 50)
        self.logger.info(f" Current Route = {current_route}")
        self.logger.info(f" Rerouting     = {new_route}")
        self.logger.info("-" * 50)
        # ----------------------

        # Install Rule Reroute (Priority 100)
        for i in range(len(new_route) - 1):
            curr_dpid = new_route[i]
            next_dpid = new_route[i+1]
            
            if self.net.has_edge(curr_dpid, next_dpid):
                out_port = self.net[curr_dpid][next_dpid]['port']
                datapath = self.datapaths.get(curr_dpid)
                
                if datapath:
                    parser = datapath.ofproto_parser
                    # Match UDP Traffic only (VoIP)
                    match = parser.OFPMatch(eth_type=0x0800, ip_proto=17) 
                    actions = [parser.OFPActionOutput(out_port)]
                    self.add_flow(datapath, self.reroute_priority, match, actions)
        
        # === TAMBAHAN LOG KE DATABASE ===
        log_desc = f"Switching from default {current_route} to KSP-{k} {new_route}"
        self.insert_event_log("REROUTE_ACTIVE", log_desc, trigger_val)

    # PERBAIKAN: Menambahkan argumen agar tidak error saat dipanggil
    def revert_routing(self, k=1, trigger_val=0):
        src_sw = 4; dst_sw = 5
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=1)
        path_str = str(paths[0]) if paths else "Default"
        
        if paths:
            self.logger.info(f" Back to Default Route = {paths[0]}")

        self.logger.info(" -> Deleting High Priority Rules...")
        for dpid, dp in self.datapaths.items():
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            # Hapus flow priority 100
            match = parser.OFPMatch(eth_type=0x0800, ip_proto=17)
            mod = parser.OFPFlowMod(
                datapath=dp, 
                command=ofproto.OFPFC_DELETE, 
                out_port=ofproto.OFPP_ANY, 
                out_group=ofproto.OFPG_ANY,
                priority=self.reroute_priority, 
                match=match
            )
            dp.send_msg(mod)
        # === TAMBAHAN LOG KE DATABASE ===
        self.insert_event_log("REROUTE_REVERT", f"Congestion cleared. Reverting to default: {path_str}", trigger_val)

    def insert_event_log(self, event_type, description, trigger_value=0):
        """Mencatat event Reroute/Revert ke Database"""
        # Gunakan conn_logic
        if not self.conn_logic: return
        try:
            with self.conn_logic.cursor() as cursor:
                query = """
                INSERT INTO traffic.system_events 
                (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
                """
                now = datetime.now()
                cursor.execute(query, (now, event_type, description, trigger_value))
                self.logger.info(f"DB LOG: [{event_type}] saved.")
        except Exception as e:
            self.logger.error(f"Failed to log event to DB: {e}")

    # =================================================================
    # 3. STANDARD HANDLERS (PacketIn, SwitchFeatures)
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
        
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # --- ARP HANDLER & IP LEARNING ---
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src
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

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = []
        is_leaf = dpid >= 4
        is_spine = dpid <= 3
        
        if out_port == ofproto.OFPP_FLOOD:
            if is_leaf:
                if in_port <= 3: # From Spine -> Flood only to Hosts
                    actions.append(parser.OFPActionOutput(4))
                    actions.append(parser.OFPActionOutput(5))
                else: # From Host -> Flood to Spines and other Hosts
                    actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
            elif is_spine:
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
        else:
            actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            # Priority 1 (Normal) agar kalah dengan Priority 100 (Reroute)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 4. SINE WAVE LOGIC & STATS HANDLER
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
        mid = (max_val + min_val) / 2  
        amp = (max_val - min_val) / 2  
        period = 3600
        phase = (elapsed_seconds % period) / period * 2 * math.pi
        sine_value = math.sin(phase)
        noise = random.uniform(-0.3, 0.3) 
        target = mid + (amp * sine_value) + (mid * 0.05 * noise)
        return int(max(min_val, min(max_val, target)))

    def insert_flow_data(self, flow_data):
        # Gunakan conn_traffic (channel terpisah)
        if not self.conn_traffic: return
        try:
            with self.conn_traffic.cursor() as cursor:
                query = """
                INSERT INTO traffic.flow_stats_ 
                (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                 ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                 duration_sec, traffic_label)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, flow_data)
                # Auto-commit active
        except:
            # Silent fail agar speed maksimal
            pass

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """
        Modified Handler to include Spike Detection Logic (1.4x Target)
        """
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = format(datapath.id, '016x')
        timestamp = datetime.now()
        elapsed_seconds = int(time.time() - self.start_time)
        
        valid_flows = []
        total_real_bytes_dpid = 0
        
        for stat in body:
            if stat.priority == 0: continue # Skip table-miss
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            if not src_ip or not dst_ip or dst_ip.endswith('.255'): continue
            
            flow_key = f"{dpid}-{src_ip}-{dst_ip}-{match.get('udp_src',0)}-{match.get('udp_dst',0)}"
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
            
            if delta_bytes >= 0: 
                valid_flows.append({
                    'flow_key': flow_key,
                    'src_ip': src_ip, 'dst_ip': dst_ip,
                    'src_mac': match.get('eth_src', ''), 'dst_mac': match.get('eth_dst', ''),
                    'ip_proto': match.get('ip_proto', 17),
                    'tp_src': match.get('udp_src', 0), 'tp_dst': match.get('udp_dst', 0),
                    'real_bytes': delta_bytes,
                    'real_pkts': delta_pkts
                })
                total_real_bytes_dpid += delta_bytes

        if not valid_flows:
            return

        target_total_bytes = self.get_target_total_bytes(elapsed_seconds)
        
        # --- LOGIKA ASLI: Deteksi Burst 1.4x ---
        if total_real_bytes_dpid > 0:
            if total_real_bytes_dpid > (target_total_bytes * 1.4):
                scaling_factor = 1.0
            else:
                scaling_factor = target_total_bytes / total_real_bytes_dpid
        else:
            scaling_factor = 0 
            
        flows_count = len(valid_flows)
        
        for flow in valid_flows:
            if total_real_bytes_dpid > 0:
                simulated_bytes_tx = int(flow['real_bytes'] * scaling_factor)
            else:
                simulated_bytes_tx = int(target_total_bytes / flows_count)

            simulated_pkts_tx = int(simulated_bytes_tx / 180)
            if simulated_pkts_tx == 0 and simulated_bytes_tx > 0: simulated_pkts_tx = 1

            simulated_bytes_rx = int(simulated_bytes_tx * random.uniform(0.95, 1.0))
            simulated_pkts_rx = int(simulated_pkts_tx * random.uniform(0.95, 1.0))
            
            flow_data = (
                timestamp, dpid,
                flow['src_ip'], flow['dst_ip'],
                flow['src_mac'], flow['dst_mac'],
                flow['ip_proto'], flow['tp_src'], flow['tp_dst'],
                simulated_bytes_tx, simulated_bytes_rx, 
                simulated_pkts_tx, simulated_pkts_rx,
                1.0, 'voip'
            )
            
            self.insert_flow_data(flow_data)