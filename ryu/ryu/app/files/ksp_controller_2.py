#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FIXED)
- Fixed: Ghost Traffic / Traffic Copying during Reroute
- Features: K-Shortest Path Rerouting based on DB Forecast
- Features: Sine Wave Traffic Generation with Spike Detection
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
BURST_THRESHOLD_BPS = 199000 

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
        self.reroute_priority = 30000 

        # Variabel untuk mencegah Flapping
        self.last_congestion_time = 0
        self.cooldown_period = 30  
        self.LOWER_THRESHOLD_BPS = 150000 
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("VoIP Smart Controller (KSP FIXED) Started")

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
    # 1. TOPOLOGY DISCOVERY (NETWORKX)
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

    # =================================================================
    # 2. FORECAST MONITORING & KSP LOGIC (FIXED)
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)
            conn = self.get_db_conn()
            if not conn:
                continue
            try:
                cur = conn.cursor()
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

                # CASE 1: Deteksi Congestion (TRIGGER NAIK)
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️ CONGESTION PREDICTED: {pred_bps:.2f} bps -> REROUTING!")
                    self.apply_ksp_reroute(k=3, trigger_val=pred_bps)
                    self.congestion_active = True
                    self.last_congestion_time = current_time

                # CASE 2: Kembali Normal (TRIGGER TURUN)
                elif self.congestion_active:
                    time_since_last_trigger = current_time - self.last_congestion_time
                    if pred_bps < self.LOWER_THRESHOLD_BPS and time_since_last_trigger > self.cooldown_period:
                        self.logger.info(f"✅ TRAFFIC STABLE ({pred_bps:.2f} bps) -> REVERTING.")
                        self.revert_routing()
                        self.congestion_active = False

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
        src_sw = 4 # Leaf 1
        dst_sw = 5 # Leaf 2
        src_ip = '10.0.0.1'
        dst_ip = '10.0.0.2'
        
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=k)
        
        if len(paths) < k:
            self.logger.error("Not enough paths for KSP rerouting")
            return

        current_route = paths[0]
        new_route     = paths[k-1]
        
        self.logger.info(f" >>> REROUTE ACTIVE: {current_route} ==> {new_route}")

        # === FIX: AGGRESSIVE CLEANUP DI JALUR LAMA ===
        # Kita harus menghapus flow L2 (MAC) dan L3 (IP) di jalur lama
        # agar monitor tidak membaca flow "hantu" (byte=0) dan mengisinya dengan sine wave palsu.
        
        target_dst_mac = self.ip_to_mac.get(dst_ip) # Resolve MAC H2

        for dpid in current_route:
            dp = self.datapaths.get(dpid)
            if not dp: continue

            ofp = dp.ofproto
            parser = dp.ofproto_parser

            # 1. Hapus Flow Spesifik L3 (IP) - Priority 10 / Previous Reroutes
            match_l3 = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_ip,
                ipv4_dst=dst_ip
            )
            mod_l3 = parser.OFPFlowMod(
                datapath=dp, command=ofp.OFPFC_DELETE,
                out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                match=match_l3
            )
            dp.send_msg(mod_l3)

            # 2. Hapus Flow Standard L2 (MAC) - Priority 1
            # INI PENTING! Standard switching pakai MAC. Jika tidak dihapus, flow entry tetap ada.
            # Monitor statistik akan melihat flow ini ada, dan men-generate "Ghost Traffic".
            if target_dst_mac:
                match_l2 = parser.OFPMatch(eth_dst=target_dst_mac)
                mod_l2 = parser.OFPFlowMod(
                    datapath=dp, command=ofp.OFPFC_DELETE,
                    out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                    match=match_l2
                )
                dp.send_msg(mod_l2)
                
            self.logger.info(f"   [Cleaned] Switch {dpid}: Removed L2/L3 flows for {dst_ip}")

        # === INSTALL JALUR BARU (PRIORITY TINGGI) ===
        for i in range(len(new_route) - 1):
            curr_dpid = new_route[i]
            next_dpid = new_route[i+1]
            
            if self.net.has_edge(curr_dpid, next_dpid):
                out_port = self.net[curr_dpid][next_dpid]['port']
                datapath = self.datapaths.get(curr_dpid)
                
                if datapath:
                    parser = datapath.ofproto_parser
                    # Match khusus UDP/VoIP agar spesifik
                    match = parser.OFPMatch(eth_type=0x0800, ip_proto=17, ipv4_src=src_ip) 
                    actions = [parser.OFPActionOutput(out_port)]
                    self.add_flow(datapath, self.reroute_priority, match, actions)
        
        # Log Event
        log_payload = {"current": current_route, "reroute": new_route, "k": k, "trigger": trigger_val}
        self.insert_event_log("REROUTE_ACTIVE", json.dumps(log_payload), trigger_val)

    
    def revert_routing(self):
        self.logger.info("🔄 REVERT TRIGGERED: Deleting High Priority Reroute Flows...")
        
        # Hapus Rule Reroute di Switch Source (Leaf 1 / DPID 4)
        datapath = self.datapaths.get(4) 
        if datapath:
            ofp = datapath.ofproto
            parser = datapath.ofproto_parser
            
            # Hapus Strict Flow Priority 30000
            match = parser.OFPMatch(eth_type=0x0800, ip_proto=17, ipv4_src='10.0.0.1')
            
            mod = parser.OFPFlowMod(
                datapath=datapath,
                command=ofp.OFPFC_DELETE_STRICT,
                out_port=ofp.OFPP_ANY,
                out_group=ofp.OFPG_ANY,
                priority=30000,
                match=match
            )
            datapath.send_msg(mod)
            self.logger.info("   >>> Delete Sent to DPID 4. Traffic should fall back to Default Path.")

        # Bersihkan juga di switch lain jika perlu, tapi biasanya source cukup.
        # Flow lama (Default Path) akan otomatis terbentuk lagi lewat PacketIn mechanism 
        # karena kita sudah menghapus L2 flow di 'apply_ksp_reroute', jadi switch akan tanya controller lagi.

        self.insert_event_log("REROUTE_REVERT", "Explicit Strict Delete", 0)

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
        except Exception:
            pass

    # =================================================================
    # 3. STANDARD HANDLERS
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

        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # --- COLLISION LOGIC (DEFAULT ROUTE FOR H1) ---
        # Hanya aktif jika tidak ada rule Priority 30000
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            COLLISION_PORT = 3 

            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                actions = [parser.OFPActionOutput(COLLISION_PORT)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                
                # Priority 10 (Lebih kecil dari Reroute 30000)
                self.add_flow(datapath, 10, match, actions, msg.buffer_id, idle_timeout=0)
                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
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

        # --- STANDARD L2 SWITCHING ---
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
            # Priority 1 (Standard)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
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
        conn = self.get_db_conn()
        if not conn: return
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
        except: pass

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = format(datapath.id, '016x')
        timestamp = datetime.now()
        elapsed_seconds = int(time.time() - self.start_time)
        
        valid_flows = []
        total_real_bytes_dpid = 0
        
        for stat in body:
            if stat.priority == 0: continue 
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            if not src_ip or not dst_ip or dst_ip.endswith('.255'): continue
            
            in_port = match.get('in_port', 'any')
            flow_key = f"{dpid}-{src_ip}-{dst_ip}-{match.get('udp_src',0)}-{match.get('udp_dst',0)}-{in_port}"
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
            
            # Hanya proses jika ada flow entry
            # Note: Jika flow sudah didelete oleh logic KSP, flow ini tidak akan muncul disini,
            # sehingga tidak akan ada "Ghost Traffic" sine wave.
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

        if not valid_flows: return

        target_total_bytes = self.get_target_total_bytes(elapsed_seconds)
        
        if total_real_bytes_dpid > 0:
            if total_real_bytes_dpid > (target_total_bytes * 1.4):
                scaling_factor = 1.0 # Spike
            else:
                scaling_factor = target_total_bytes / total_real_bytes_dpid
        else:
            scaling_factor = 0 
            
        flows_count = len(valid_flows)
        
        for flow in valid_flows:
            if total_real_bytes_dpid > 0:
                simulated_bytes_tx = int(flow['real_bytes'] * scaling_factor)
            else:
                # Jika 0 byte, bagi rata target sine wave.
                # MASALAH SEBELUMNYA: Flow L2 lama masih ada (byte=0), jadi masuk sini.
                # SOLUSI: Flow L2 lama sudah dihapus di apply_ksp_reroute, jadi loop ini aman.
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