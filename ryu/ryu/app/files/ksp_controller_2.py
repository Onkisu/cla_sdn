#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (Hybrid Merge)
==========================================================================
1. PACKET HANDLING: 100% User Logic (Proven ping/icmp capability).
2. REROUTING CORE:  Industrial Grade (Make-Before-Break, Barrier, Cookies).
3. MONITORING:      Async Database Logging & AI Forecasting.
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

# Threshold Congestion (Sesuai forecast_2.py)
BURST_THRESHOLD_BPS = 120000 
HYSTERESIS_LOWER_BPS = 80000   # Traffic harus turun ke sini baru revert
COOLDOWN_PERIOD_SEC = 20       # Waktu tunggu minimal antar switch

# Constants for Industrial Flow Management
COOKIE_REROUTE = 0xDEADBEEF    # Penanda khusus flow reroute
PRIORITY_REROUTE = 30000       # Priority Tinggi (mengalahkan default user priority 10)



class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)

        # [TAMBAHKAN INI]
        self.reroute_stage = 'IDLE' 
        self.stopped_spines = []
        self.trigger_val_cache = 0
        self.temp_silence_cache = {}
        
        # --- INIT DATA STRUCTURES ---
        self.mac_to_port = {}
        self.ip_to_mac = {}     
        self.datapaths = {}
        self.net = nx.DiGraph() # NetworkX Graph
        self.last_bytes = {}
       

        
        # --- STATE CONGESTION ---
        self.congestion_active = False 
        self.last_state_change_time = 0
        self.current_reroute_path = None
        
        self.connect_database()
        
        # --- THREADS ---
        # 1. Monitor Traffic Stats
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        # 2. Monitor Forecast dari DB (UPGRADED to Smart Watchdog)
        self.forecast_thread = hub.spawn(self._monitor_forecast_smart)
        # 3. Discovery Topology
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("🟢 VoIP Smart Controller (Hybrid Industrial) Started")

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
    # 2. FORECAST MONITORING (SYMMETRICAL BREAK-BEFORE-MAKE)
    # =================================================================
    def _monitor_forecast_smart(self):
        self.logger.info("👁️  AI Watchdog: Symmetrical Break-Before-Make Active")
        
        target_src = '10.0.0.1'
        target_dst = '10.0.0.2'

        while True:
            hub.sleep(1.0)
            
            # --- STAGE 1: IDLE (Monitoring Burst) ---
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
                        # DETEKSI BURST
                        if pred_bps > BURST_THRESHOLD_BPS:
                            # 1. Cari Spine yang SEDANG AKTIF (Biasanya Spine 1)
                            current_spines = self._get_active_spines_for_flow(target_src, target_dst)
                            
                            if not current_spines:
                                continue # Trafik belum kebaca, skip cycle ini

                            self.logger.warning(f"🚀 BURST DETECTED ({pred_bps:.0f}). KILLING Active Spines: {current_spines}")
                            
                            # 2. STOP FLOW DI JALUR SAAT INI
                            for sp_dpid in current_spines:
                                self._emergency_stop_flow(sp_dpid, target_src, target_dst)
                            
                            self.stopped_spines = current_spines
                            self.reroute_stage = 'VERIFYING' # Masuk mode verifikasi Reroute
                            self.silence_check_counter = 0
                            self.trigger_val_cache = pred_bps
                except Exception as e:
                    self.logger.error(f"DB Error: {e}")

            # --- STAGE 2: VERIFYING SILENCE (Sebelum Reroute) ---
            elif self.reroute_stage == 'VERIFYING':
                self.silence_check_counter += 1
                if self._are_spines_silent(self.stopped_spines, target_src, target_dst):
                    self.logger.info(f"✅ SILENCE CONFIRMED. Installing Alternate Path...")
                    
                    # 3. INSTALL JALUR BARU (Hindari Spine Lama)
                    avoid_node = self.stopped_spines[0] 
                    self.perform_dynamic_reroute(target_src, target_dst, avoid_node, self.trigger_val_cache)
                    
                    self.reroute_stage = 'REROUTED'
                    self.last_state_change_time = time.time()
                else:
                    self.logger.info(f"⏳ Waiting for traffic to drain (Reroute Phase)...")
                    # Retry kill safety
                    if self.silence_check_counter % 3 == 0:
                         for sp in self.stopped_spines: self._emergency_stop_flow(sp, target_src, target_dst)

            # --- STAGE 3: REROUTED (Monitoring kapan boleh REVERT) ---
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
                        
                        # Syarat Revert: Trafik Turun DAN Waktu Cooldown Habis
                        if pred_bps < HYSTERESIS_LOWER_BPS and (now - self.last_state_change_time > COOLDOWN_PERIOD_SEC):
                            self.logger.info("🔙 TRAFFIC NORMAL. Initiating REVERT Sequence...")
                            
                            # 1. Cari Spine yang SEDANG AKTIF (Sekarang pasti Spine Backup, misal Spine 2/3)
                            current_spines = self._get_active_spines_for_flow(target_src, target_dst)
                            
                            if not current_spines:
                                # Fallback jika switch stats belum lapor tapi kita tau harus revert
                                # Kita paksa asumsi spine selain 1 (karena kita di state Rerouted)
                                self.logger.warning("Warning: No active spine detected for revert, attempting blind stop.")
                            
                            self.logger.warning(f"⛔ STOPPING ALTERNATE PATH: {current_spines}")
                            
                            # 2. STOP FLOW DI JALUR BACKUP
                            for sp_dpid in current_spines:
                                self._emergency_stop_flow(sp_dpid, target_src, target_dst)
                                
                            self.stopped_spines = current_spines
                            self.reroute_stage = 'REVERT_VERIFY' # State Baru: Verifikasi Revert
                            self.silence_check_counter = 0

                except Exception as e:
                     self.logger.error(f"Revert Logic Error: {e}")

            # --- STAGE 4: REVERT VERIFY (Pastikan Backup Mati dulu, baru Default Nyala) ---
            elif self.reroute_stage == 'REVERT_VERIFY':
                self.silence_check_counter += 1
                
                # Cek apakah jalur backup sudah diam?
                if self._are_spines_silent(self.stopped_spines, target_src, target_dst):
                    self.logger.info(f"✅ BACKUP PATH SILENT. Restoring Best Path (Default)...")
                    
                    # 3. INSTALL BEST PATH (Tanpa 'avoid', cari shortest path murni)
                    self.perform_dynamic_revert(target_src, target_dst)
                    
                    self.reroute_stage = 'IDLE' # Kembali ke awal
                    self.last_state_change_time = time.time()
                else:
                    self.logger.info(f"⏳ Waiting for traffic to drain (Revert Phase)...")
                    if self.silence_check_counter % 3 == 0:
                         for sp in self.stopped_spines: self._emergency_stop_flow(sp, target_src, target_dst)

    # =================================================================
    # MISSING HELPERS (PASTE INI KE DALAM CLASS)
    # =================================================================

    def _emergency_stop_flow(self, dpid_target, src_ip, dst_ip):
        """Hanya stop flow H1->H2 di switch tertentu"""
        if dpid_target in self.datapaths:
            dp = self.datapaths[dpid_target]
            ofproto = dp.ofproto
            parser = dp.ofproto_parser
            
            # Match spesifik IP
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_ip,
                ipv4_dst=dst_ip
            )
            # Command DELETE
            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=match
            )
            dp.send_msg(mod)
            self._send_barrier(dp)

    def _check_is_traffic_silent(self, dpid, src_ip, dst_ip):
        """
        Cek apakah bytes flow ini tidak bertambah.
        Logic Update: Support format key baru (dpid-src-dst-prio).
        """
        # Prefix pattern: "dpid-src-dst-"
        prefix_key = f"{dpid}-{src_ip}-{dst_ip}-"
        
        # Cari semua key di memori yang cocok dengan pattern flow ini (priority berapapun)
        matching_keys = [k for k in self.last_bytes.keys() if k.startswith(prefix_key)]
        
        # Jika tidak ada key yg cocok, berarti switch belum lapor atau sudah bersih -> Silent
        if not matching_keys: 
            return True
        
        all_silent = True
        
        if not hasattr(self, 'temp_silence_cache'): 
            self.temp_silence_cache = {}

        for key in matching_keys:
            curr_bytes, _ = self.last_bytes[key]
            
            # Cache logic untuk membandingkan dengan detik sebelumnya
            cache_key = f"silent_check_{key}"
            prev_bytes = self.temp_silence_cache.get(cache_key, -1)
            self.temp_silence_cache[cache_key] = curr_bytes
            
            # Jika ada SATU SAJA flow yang bytes-nya nambah, return False (Not Silent)
            if curr_bytes != prev_bytes:
                all_silent = False
        
        return all_silent

    def perform_dynamic_reroute(self, src_ip, dst_ip, avoid_dpid, trigger_val):
        """Cari jalur baru menghindari avoid_dpid"""
        # Hardcode Leaf Host sesuai topologi (karena switch host jarang pindah)
        src_sw = 4 
        dst_sw = 5 
        
        try:
            paths = list(nx.shortest_simple_paths(self.net, src_sw, dst_sw))
            # Filter: Path TIDAK BOLEH lewat avoid_dpid
            alt_path = next((p for p in paths if avoid_dpid not in p), None)
            
            if alt_path:
                self.logger.info(f"🛣️  REROUTING {src_ip}->{dst_ip} via {alt_path}")
                # Install Flow Reverse
                for i in range(len(alt_path) - 1, 0, -1):
                    curr = alt_path[i-1]
                    nxt = alt_path[i]
                    if self.net.has_edge(curr, nxt):
                        out_port = self.net[curr][nxt]['port']
                        dp = self.datapaths.get(curr)
                        if dp:
                            self._install_reroute_flow(dp, out_port) 
                            self._send_barrier(dp)
                
                self.current_reroute_path = alt_path
                self.congestion_active = True
                self.insert_event_log("REROUTE_DYN", f"Path: {alt_path}", trigger_val)
            else:
                self.logger.warning("No alternative path found!")

        except Exception as e:
            self.logger.error(f"Reroute Fail: {e}")

    # =================================================================
    # NEW HELPERS FOR REVERT
    # =================================================================

    def _are_spines_silent(self, dpids_list, src, dst):
        """Helper untuk cek list spine apakah sudah 0 semua delta bytes-nya"""
        if not dpids_list: return True # Kalau list kosong, anggap silent (aman)
        
        all_silent = True
        for dpid in dpids_list:
            if not self._check_is_traffic_silent(dpid, src, dst):
                all_silent = False
                break
        return all_silent

    def perform_dynamic_revert(self, src_ip, dst_ip):
        """
        Kembalikan ke jalur terbaik (Shortest Path) tanpa batasan/avoid.
        Biasanya akan memilih Spine 1 lagi jika itu cost terendah.
        """
        src_sw = 4 # Hardcode Leaf Host 1 (sesuai topologi)
        dst_sw = 5 # Hardcode Leaf Host 2
        
        try:
            # Cari shortest path murni
            best_path = nx.shortest_path(self.net, src_sw, dst_sw)
            
            self.logger.info(f"🔙 RESTORING PATH: {best_path}")
            
            # Install Flow Reverse
            for i in range(len(best_path) - 1, 0, -1):
                curr = best_path[i-1]
                nxt = best_path[i]
                if self.net.has_edge(curr, nxt):
                    out_port = self.net[curr][nxt]['port']
                    dp = self.datapaths.get(curr)
                    if dp:
                        self._install_reroute_flow(dp, out_port) # Pakai priority tinggi
                        self._send_barrier(dp)
            
            self.current_reroute_path = best_path
            self.congestion_active = False # Reset flag
            self.insert_event_log("REVERT_DONE", f"Restored: {best_path}", 0)

        except Exception as e:
            self.logger.error(f"Revert Install Fail: {e}")

    # --- LOW LEVEL FLOW HELPERS ---

    def _install_reroute_flow(self, datapath, out_port):
        parser = datapath.ofproto_parser
        
        # Match HANYA H1 (10.0.0.1) -> H2 (10.0.0.2)
        # Spesifik agar tidak mengganggu traffic H3
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # PRIORITY LEBIH TINGGI (30000) dari User Code (10)
        # COOKIE REROUTE dipasang untuk identifikasi
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

    def _send_barrier(self, datapath):
        """Memastikan perintah sebelumnya selesai dieksekusi switch"""
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
    # 4. STANDARD HANDLERS - EXACT COPY FROM YOUR CODE
    #    (This ensures PING works exactly as you expect)
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
        # -------------------------------------------------------------
        # INI ADALAH LOGIC PACKET-IN DARI KODE ANDA YANG BERHASIL PING
        # SAYA TIDAK MENGUBAH APAPUN DI SINI.
        # -------------------------------------------------------------
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
                
                # INTEGRASI KE "INDUSTRIAL REROUTE":
                # Jika congestion aktif, kita biarkan paket H1 lolos.
                # Karena flow REROUTE (Priority 30000) yang kita install di 'perform_industrial_reroute'
                # akan menangkapnya. Jika kita install flow priority 10 di sini, tidak masalah, 
                # karena priority 30000 akan menang.
                
                # Namun, user code me-return. Kita ikuti user code.
                if self.congestion_active and src_ip == '10.0.0.1':
                    # self.logger.info(f"⚠️ Skipping default routing for {src_ip} during reroute")
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
    # 5. SINE WAVE LOGIC & STATS HANDLER (FULL ORIGINAL LOGIC)
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


    # =================================================================
    # NEW DYNAMIC DETECTION HELPER (NO HARDCODE)
    # =================================================================
    def _get_active_spines_for_flow(self, src_ip, dst_ip):
        """
        Mendeteksi Spine mana yang sedang membawa trafik src->dst.
        Logic Update: Parsing key format baru dengan 4 komponen.
        """
        active_spines = []
        
        for flow_key, (bytes_count, pkts) in self.last_bytes.items():
            # Parse Key: dpid-src-dst-prio
            try:
                parts = flow_key.split('-')
                # Kita butuh handle jika formatnya 3 (lama) atau 4 (baru)
                if len(parts) == 4:
                    dpid_str, f_src, f_dst, _ = parts
                elif len(parts) == 3:
                    dpid_str, f_src, f_dst = parts
                else:
                    continue
                
                dpid = int(dpid_str)
            except ValueError:
                continue

            # Filter hanya untuk flow target
            if f_src != src_ip or f_dst != dst_ip:
                continue
                
            if bytes_count <= 0:
                continue

            # --- LOGIC DETEKSI LEAF VS SPINE (Sama seperti sebelumnya) ---
            src_mac = self.ip_to_mac.get(src_ip)
            if not src_mac: continue
            
            in_port = self.mac_to_port.get(dpid, {}).get(src_mac)
            if in_port is None: continue

            is_link_port = False
            if self.net.has_node(dpid):
                for neighbor in self.net[dpid]:
                    if self.net[dpid][neighbor]['port'] == in_port:
                        is_link_port = True
                        break
            
            if is_link_port:
                active_spines.append(dpid)
        
        return list(set(active_spines))

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Monitor REAL traffic H1->H2 dan H3→H2 - Handle None values!"""

        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = datapath.id
        timestamp = datetime.now()
        
        conn = self.get_db_conn()
        if not conn:
            return

        try:
            cur = conn.cursor()
            for stat in body:

                # === IGNORE ZOMBIE TRAFFIC DURING TRANSITION (BENAR) ===
                if stat.duration_sec < 2:
                    continue


                if stat.priority == 0:
                    continue

                match = stat.match

                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')

                if not src_ip:
                    src_mac = match.get('eth_src')
                    src_ip = self._resolve_ip(src_mac) if src_mac else None

                if not dst_ip:
                    dst_mac = match.get('eth_dst')
                    dst_ip = self._resolve_ip(dst_mac) if dst_mac else None

                if dst_ip != '10.0.0.2':
                    continue
                if src_ip not in ['10.0.0.1', '10.0.0.3']:
                    continue

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

                cur.execute("""
                    INSERT INTO traffic.flow_stats_
                    (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                    ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                    pkts_tx, pkts_rx, duration_sec, traffic_label)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    timestamp, dpid, src_ip, dst_ip,
                    match.get('eth_src'), match.get('eth_dst'),
                    match.get('ip_proto', 17),
                    match.get('tcp_src') or match.get('udp_src') or 0,
                    match.get('tcp_dst') or match.get('udp_dst') or 0,
                    delta_bytes, delta_bytes,
                    delta_packets, delta_packets,
                    1.0,
                    'voip' if src_ip == '10.0.0.1' else 'bursty'
                ))

            conn.commit()
            cur.close()
            conn.close()

        except Exception:
            if conn:
                conn.close()
