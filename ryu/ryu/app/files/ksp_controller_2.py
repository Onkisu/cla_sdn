#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (Hybrid Merge)
==========================================================================
Logic Updated:
1. DEFAULT / REVERT PATH -> WAJIB SPINE 3 (Tempat H3).
2. REROUTE PATH        -> Pindah ke SPINE 1 atau 2.
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
HYSTERESIS_LOWER_BPS = 80000
COOLDOWN_PERIOD_SEC = 20

COOKIE_REROUTE = 0xDEADBEEF
PRIORITY_REROUTE = 30000 

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)

        self.reroute_stage = 'IDLE' 
        self.stopped_spines = []
        self.trigger_val_cache = 0
        self.temp_silence_cache = {}
        
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() 
        self.last_bytes = {}
       
        self.congestion_active = False 
        self.last_state_change_time = 0
        self.current_reroute_path = None
        
        self.connect_database()
        
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast_smart)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("🟢 VoIP Smart Controller (Target: Default Spine 3) Started")

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
        except Exception:
            return None

    # =================================================================
    # 1. TOPOLOGY DISCOVERY
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
    # 2. FORECAST MONITORING (UPDATED LOGIC)
    # =================================================================
    def _monitor_forecast_smart(self):
        self.logger.info("👁️  AI Watchdog Active: Enforcing Return to Spine 3")
        
        target_src = '10.0.0.1'
        target_dst = '10.0.0.2'

        while True:
            hub.sleep(1.0)
            
            # --- STAGE 1: IDLE ---
            if self.reroute_stage == 'IDLE':
                conn = self.get_db_conn()
                if not conn: continue
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                    result = cur.fetchone()
                    cur.close(); conn.close()

                    if result:
                        pred_bps = float(result[0])
                        if pred_bps > BURST_THRESHOLD_BPS:
                            # Cari Spine yang sedang aktif (Harusnya Spine 3 jika sesuai rencana)
                            current_spines = self._get_active_spines_for_flow(target_src, target_dst)
                            
                            if not current_spines: continue

                            self.logger.warning(f"🚀 BURST DETECTED ({pred_bps:.0f}). KILLING Active Spines: {current_spines}")
                            
                            for sp_dpid in current_spines:
                                self._emergency_stop_flow(sp_dpid, target_src, target_dst)
                            
                            self.stopped_spines = current_spines
                            self.reroute_stage = 'VERIFYING'
                            self.silence_check_counter = 0
                            self.trigger_val_cache = pred_bps
                except Exception as e:
                    self.logger.error(f"DB Error: {e}")

            # --- STAGE 2: VERIFYING SILENCE ---
            elif self.reroute_stage == 'VERIFYING':
                self.silence_check_counter += 1
                if self._are_spines_silent(self.stopped_spines, target_src, target_dst):
                    self.logger.info(f"✅ SILENCE CONFIRMED. Installing Alternate Path...")
                    
                    # FIX: Kirim LIST (stopped_spines) agar dihindari semua
                    # Ini memastikan jika burst di Spine 3, dia cari jalan LAIN (1 atau 2)
                    self.perform_dynamic_reroute(target_src, target_dst, self.stopped_spines, self.trigger_val_cache)
                    
                    self.reroute_stage = 'REROUTED'
                    self.last_state_change_time = time.time()
                else:
                    self.logger.info(f"⏳ Waiting for traffic to drain...")
                    if self.silence_check_counter % 3 == 0:
                         for sp in self.stopped_spines: self._emergency_stop_flow(sp, target_src, target_dst)

            # --- STAGE 3: REROUTED ---
            elif self.reroute_stage == 'REROUTED':
                conn = self.get_db_conn()
                if not conn: continue
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                    result = cur.fetchone()
                    cur.close(); conn.close()

                    if result:
                        pred_bps = float(result[0])
                        now = time.time()
                        
                        if pred_bps < HYSTERESIS_LOWER_BPS and (now - self.last_state_change_time > COOLDOWN_PERIOD_SEC):
                            self.logger.info("🔙 TRAFFIC NORMAL. Initiating REVERT Sequence...")
                            
                            current_spines = self._get_active_spines_for_flow(target_src, target_dst)
                            if not current_spines:
                                self.logger.warning("Warning: Blind stop for revert.")
                            
                            self.logger.warning(f"⛔ STOPPING BACKUP PATH: {current_spines}")
                            
                            for sp_dpid in current_spines:
                                self._emergency_stop_flow(sp_dpid, target_src, target_dst)
                                
                            self.stopped_spines = current_spines
                            self.reroute_stage = 'REVERT_VERIFY'
                            self.silence_check_counter = 0

                except Exception as e:
                     self.logger.error(f"Revert Logic Error: {e}")

            # --- STAGE 4: REVERT VERIFY ---
            elif self.reroute_stage == 'REVERT_VERIFY':
                self.silence_check_counter += 1
                
                if self._are_spines_silent(self.stopped_spines, target_src, target_dst):
                    self.logger.info(f"✅ BACKUP PATH SILENT. Restoring Target Spine 3...")
                    
                    # PANGGIL REVERT KHUSUS (KE SPINE 3)
                    self.perform_dynamic_revert(target_src, target_dst)
                    
                    self.reroute_stage = 'IDLE'
                    self.last_state_change_time = time.time()
                else:
                    self.logger.info(f"⏳ Waiting for traffic to drain (Revert Phase)...")
                    if self.silence_check_counter % 3 == 0:
                         for sp in self.stopped_spines: self._emergency_stop_flow(sp, target_src, target_dst)

    # =================================================================
    # HELPERS
    # =================================================================

    def _emergency_stop_flow(self, dpid_target, src_ip, dst_ip):
        if dpid_target in self.datapaths:
            dp = self.datapaths[dpid_target]
            ofproto = dp.ofproto
            parser = dp.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
            mod = parser.OFPFlowMod(
                datapath=dp, command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY, match=match
            )
            dp.send_msg(mod)
            self._send_barrier(dp)

    def _check_is_traffic_silent(self, dpid, src_ip, dst_ip):
        prefix_key = f"{dpid}-{src_ip}-{dst_ip}-"
        matching_keys = [k for k in self.last_bytes.keys() if k.startswith(prefix_key)]
        if not matching_keys: return True
        
        all_silent = True
        if not hasattr(self, 'temp_silence_cache'): self.temp_silence_cache = {}

        for key in matching_keys:
            curr_bytes, _ = self.last_bytes[key]
            cache_key = f"silent_check_{key}"
            prev_bytes = self.temp_silence_cache.get(cache_key, -1)
            self.temp_silence_cache[cache_key] = curr_bytes
            if curr_bytes != prev_bytes: all_silent = False
        return all_silent

    # --- FIX 1: REROUTE MENERIMA LIST AVOID ---
    def perform_dynamic_reroute(self, src_ip, dst_ip, avoid_list, trigger_val):
        """Cari jalur baru menghindari SEMUA node di avoid_list"""
        src_sw = 4 
        dst_sw = 5 
        
        # Pastikan avoid_list adalah list, jaga-jaga kalau single int
        if not isinstance(avoid_list, list): avoid_list = [avoid_list]

        try:
            paths = list(nx.shortest_simple_paths(self.net, src_sw, dst_sw))
            
            # LOGIC: Pilih path yang tidak mengandung SATUPUN anggota avoid_list
            alt_path = next((p for p in paths if not any(avoid_node in p for avoid_node in avoid_list)), None)
            
            if alt_path:
                self.logger.info(f"🛣️  REROUTING {src_ip}->{dst_ip} via {alt_path}")
                self._install_path(alt_path) # Refactored install logic
                
                self.current_reroute_path = alt_path
                self.congestion_active = True
                self.insert_event_log("REROUTE_DYN", f"Path: {alt_path}", trigger_val)
            else:
                self.logger.warning(f"No alternative path found avoiding {avoid_list}!")

        except Exception as e:
            self.logger.error(f"Reroute Fail: {e}")

    # --- FIX 2: REVERT MEMAKSA KE SPINE 3 ---
    def perform_dynamic_revert(self, src_ip, dst_ip):
        """
        Kembalikan ke SPINE 3 secara paksa (sesuai request user).
        Jika Spine 3 mati/putus, baru pakai shortest path biasa.
        """
        src_sw = 4
        dst_sw = 5
        preferred_spine = 3  # <--- REQUEST ANDA
        
        try:
            target_path = None
            
            # 1. Cek apakah Spine 3 ada di topologi dan terhubung
            # Path Manual: [4, 3, 5]
            if (self.net.has_edge(src_sw, preferred_spine) and 
                self.net.has_edge(preferred_spine, dst_sw)):
                target_path = [src_sw, preferred_spine, dst_sw]
                self.logger.info(f"🔙 FORCING RETURN TO SPINE {preferred_spine}: {target_path}")
            else:
                # Fallback jika Spine 3 putus kabelnya
                target_path = nx.shortest_path(self.net, src_sw, dst_sw)
                self.logger.warning(f"🔙 Spine {preferred_spine} unavailable. Using Shortest: {target_path}")
            
            if target_path:
                self._install_path(target_path)
                self.current_reroute_path = target_path
                self.congestion_active = False
                self.insert_event_log("REVERT_DONE", f"Restored: {target_path}", 0)

        except Exception as e:
            self.logger.error(f"Revert Install Fail: {e}")

    def _install_path(self, path):
        """Helper untuk install flow di sepanjang path"""
        for i in range(len(path) - 1, 0, -1):
            curr = path[i-1]
            nxt = path[i]
            if self.net.has_edge(curr, nxt):
                out_port = self.net[curr][nxt]['port']
                dp = self.datapaths.get(curr)
                if dp:
                    self._install_reroute_flow(dp, out_port)
                    self._send_barrier(dp)

    def _are_spines_silent(self, dpids_list, src, dst):
        if not dpids_list: return True 
        all_silent = True
        for dpid in dpids_list:
            if not self._check_is_traffic_silent(dpid, src, dst):
                all_silent = False
                break
        return all_silent

    def _install_reroute_flow(self, datapath, out_port):
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(
            datapath=datapath, priority=PRIORITY_REROUTE, match=match,
            instructions=inst, cookie=COOKIE_REROUTE, idle_timeout=0, hard_timeout=0
        )
        datapath.send_msg(mod)

    def _send_barrier(self, datapath):
        datapath.send_msg(datapath.ofproto_parser.OFPBarrierRequest(datapath))

    def insert_event_log(self, event_type, description, trigger_value=0):
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, description, trigger_value))
            conn.commit(); cur.close(); conn.close()
        except Exception: pass

    # =================================================================
    # STANDARD HANDLERS (Sama seperti sebelumnya)
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

        dst = eth.dst; src = eth.src
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4) 
        if ip_pkt: self.ip_to_mac[ip_pkt.src] = src

        # DEFAULT ROUTING: DISINI ANDA BISA SET AGAR DEFAULT LEWAT SPINE 3 JUGA
        if ip_pkt:
            src_ip = ip_pkt.src; dst_ip = ip_pkt.dst
            
            # KITA UBAH DISINI: Jika tidak ada congestion, arahkan ke SPINE 3 (Port 2 biasanya)
            # Asumsi Port Mapping di Leaf 4: Port 1->Spine1, Port 2->Spine2, Port 3->Spine3
            # TAPI KARENA REROUTE FLOW SUDAH DI-HANDLE OLEH "perform_dynamic_revert",
            # Default packet-in ini hanya untuk paket pertama saja.
            
            if self.congestion_active and src_ip == '10.0.0.1': return
                
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.ip_to_mac:
                target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                e = ethernet.ethernet(dst=src, src=target_mac, ethertype=ether_types.ETH_TYPE_ARP)
                a = arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=arp_pkt.dst_ip, dst_mac=src, dst_ip=arp_pkt.src_ip)
                p = packet.Packet()
                p.add_protocol(e); p.add_protocol(a); p.serialize()
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                datapath.send_msg(out); return

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

        data = None if msg.buffer_id == ofproto.OFP_NO_BUFFER else msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values(): self._request_stats(dp)
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

    def _get_active_spines_for_flow(self, src_ip, dst_ip):
        active_spines = []
        for flow_key, (bytes_count, pkts) in self.last_bytes.items():
            try:
                parts = flow_key.split('-')
                if len(parts) >= 3: dpid = int(parts[0]); f_src = parts[1]; f_dst = parts[2]
                else: continue
            except ValueError: continue

            if f_src != src_ip or f_dst != dst_ip or bytes_count <= 0 or dpid > 3: continue
            active_spines.append(dpid)
        return list(set(active_spines))

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body; datapath = ev.msg.datapath; dpid = datapath.id
        timestamp = datetime.now()
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            for stat in body:
                if stat.duration_sec < 2 or stat.priority == 0: continue
                match = stat.match
                src_ip = match.get('ipv4_src'); dst_ip = match.get('ipv4_dst')
                if not src_ip: src_ip = self._resolve_ip(match.get('eth_src'))
                if not dst_ip: dst_ip = self._resolve_ip(match.get('eth_dst'))
                if dst_ip != '10.0.0.2' or src_ip not in ['10.0.0.1', '10.0.0.3']: continue

                flow_key = f"{dpid}-{src_ip}-{dst_ip}-{stat.priority}" # Key Update dengan Priority
                byte_count = stat.byte_count; packet_count = stat.packet_count

                if flow_key in self.last_bytes:
                    last_bytes, last_packets = self.last_bytes[flow_key]
                    delta_bytes = max(0, byte_count - last_bytes)
                    delta_packets = max(0, packet_count - last_packets)
                else:
                    delta_bytes = byte_count; delta_packets = packet_count

                self.last_bytes[flow_key] = (byte_count, packet_count)
                if delta_bytes <= 0: continue

                cur.execute("""
                    INSERT INTO traffic.flow_stats_
                    (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec, traffic_label)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (timestamp, dpid, src_ip, dst_ip, match.get('eth_src'), match.get('eth_dst'), match.get('ip_proto', 17), match.get('tcp_src') or 0, match.get('tcp_dst') or 0, delta_bytes, delta_bytes, delta_packets, delta_packets, 1.0, 'voip' if src_ip == '10.0.0.1' else 'bursty'))
            conn.commit(); cur.close(); conn.close()
        except Exception: 
            if conn: conn.close()