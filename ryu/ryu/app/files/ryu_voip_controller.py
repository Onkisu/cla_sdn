#!/usr/bin/env python3
"""
FIXED Ryu SDN Controller for VoIP Traffic Monitoring
- Aggregates ALL flows per DPID to match specific Sine Wave Target
- Logs Real (D-ITG) vs Scaled values
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
import psycopg2
from datetime import datetime
import random
import math
import time

# PostgreSQL Configuration
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class VoIPTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPTrafficMonitor, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.datapaths = {}
        self.db_conn = None
        self.last_bytes = {}
        self.start_time = time.time()
        
        self.connect_database()
        self.monitor_thread = hub.spawn(self._monitor)
        self.logger.info("VoIP Traffic Monitor (Total Aggregation Mode) Started")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}. Running without DB storage.")
            self.db_conn = None

    # --- SINE WAVE LOGIC (PER DPID TOTAL) ---
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
        
        # Add small randomness (jitter) so it's not a perfect smooth line
        noise = random.uniform(-0.1, 0.1) 
        
        target = mid + (amp * sine_value) + (mid * 0.05 * noise)
        return int(max(min_val, min(max_val, target)))

    def insert_flow_data(self, flow_data):
        if not self.db_conn: return
        try:
            cursor = self.db_conn.cursor()
            query = """
            INSERT INTO traffic.flow_stats_ 
            (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
             ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
             duration_sec, traffic_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, flow_data)
            self.db_conn.commit()
            cursor.close()
        except:
            self.db_conn.rollback()

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

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """
        Modified Handler with BURST LOGIC:
        1. Collect ALL valid flows.
        2. Calculate Target (Sine Wave).
        3. CHECK: If Real Traffic > Sine Wave -> Use Real Traffic (BURST!).
                  If Real Traffic < Sine Wave -> Boost to Sine Wave.
        4. Distribute & Insert.
        """
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = format(datapath.id, '016x')
        timestamp = datetime.now()
        # Opsional: Bulatkan timestamp jika ingin grafik sangat rapi
        # timestamp = timestamp.replace(microsecond=0) 
        
        elapsed_seconds = int(time.time() - self.start_time)
        
        valid_flows = []
        total_real_bytes_dpid = 0
        
        # --- LANGKAH 1: Kumpulkan Flow Valid & Hitung Total Real (TETAP SAMA) ---
        for stat in body:
            if stat.priority == 0: continue 
            
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

        # ================== BAGIAN INI YANG BARU (START) ==================

        # --- LANGKAH 2: Hitung Target (Sine Wave) ---
        sine_wave_target = self.get_target_total_bytes(elapsed_seconds)
        
        # --- LANGKAH 3: Tentukan Final Target & Scaling Factor ---
        # LOGIKA BARU: 
        # Jika Real Traffic > Sine Wave (ada Burst), gunakan Real Traffic agar Spike terlihat di DB.
        # Jika Real Traffic < Sine Wave (sepi), gunakan Sine Wave (Boost).
        
        if total_real_bytes_dpid > sine_wave_target:
            # Mode Spike / Burst: Biarkan data asli lewat (Scale = 1.0)
            final_target_bytes = total_real_bytes_dpid
            scaling_factor = 1.0
            status_msg = "BURST DETECTED!"
        else:
            # Mode Normal: Boost biar grafik cantik kayak gelombang
            final_target_bytes = sine_wave_target
            if total_real_bytes_dpid > 0:
                scaling_factor = final_target_bytes / total_real_bytes_dpid
            else:
                scaling_factor = 0 # Nanti dibagi rata
            status_msg = "Sine Wave Boost"

        self.logger.info(f"--- DPID {dpid} (Sec: {elapsed_seconds}) [{status_msg}] ---")
        self.logger.info(f"    Real: {total_real_bytes_dpid} B | SineTarget: {sine_wave_target} B | Final: {final_target_bytes} B")

        # --- LANGKAH 4: Distribusi & Insert ---
        flows_count = len(valid_flows)
        
        for flow in valid_flows:
            if total_real_bytes_dpid > 0:
                # Proportional Scaling
                simulated_bytes_tx = int(flow['real_bytes'] * scaling_factor)
            else:
                # Distribusi rata jika real traffic 0
                simulated_bytes_tx = int(final_target_bytes / flows_count)

            # ================== BAGIAN INI YANG BARU (END) ==================

            # --- SISA KODE LAMA (TETAP DIBAWAH SINI) ---
            
            # Asumsi ukuran paket VoIP rata-rata ~180-200 bytes
            simulated_pkts_tx = int(simulated_bytes_tx / 180)
            if simulated_pkts_tx == 0 and simulated_bytes_tx > 0: simulated_pkts_tx = 1

            # RX sedikit random (simulasi loss/jitter kecil)
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

    def _monitor(self):
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

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """
        Modified Handler:
        1. Collect ALL valid flows.
        2. Sum their REAL bytes (from D-ITG).
        3. Calculate Target Bytes based on Sine Wave.
        4. Calculate Scaling Factor.
        5. Distribute Target Bytes proportionally to flows.
        """
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = format(datapath.id, '016x')
        timestamp = datetime.now()
        elapsed_seconds = int(time.time() - self.start_time)
        
        valid_flows = []
        total_real_bytes_dpid = 0
        
        # --- LANGKAH 1: Kumpulkan Flow Valid & Hitung Total Real ---
        for stat in body:
            if stat.priority == 0: continue # Skip table-miss
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            if not src_ip or not dst_ip or dst_ip.endswith('.255'): continue
            
            # Hitung Delta (Real D-ITG traffic since last sec)
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
            
            # Simpan data flow sementara
            if delta_bytes >= 0: # Ambil semua flow aktif
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

        # --- LANGKAH 2: Hitung Target (Sine Wave) ---
        target_total_bytes = self.get_target_total_bytes(elapsed_seconds)
        
        # --- LANGKAH 3: Hitung Scaling Factor ---
        # Hindari pembagian dengan nol. Jika real 0, kita anggap 1 agar tidak error,
        # tapi nanti distribusinya akan kita bagi rata (force distribution).
        if total_real_bytes_dpid > 0:
            scaling_factor = target_total_bytes / total_real_bytes_dpid
        else:
            scaling_factor = 0 # Nanti kita handle khusus
            
        self.logger.info(f"--- DPID {dpid} Report (Sec: {elapsed_seconds}) ---")
        self.logger.info(f"    Real (D-ITG): {total_real_bytes_dpid} B | Target (Sine): {target_total_bytes} B | Scale: {scaling_factor:.2f}x")

        # --- LANGKAH 4: Distribusi & Insert ---
        flows_count = len(valid_flows)
        
        for flow in valid_flows:
            if total_real_bytes_dpid > 0:
                # Proportional Scaling
                simulated_bytes_tx = int(flow['real_bytes'] * scaling_factor)
            else:
                # Jika real traffic 0 tapi ada flow entry, bagi rata targetnya
                # agar grafik tetap jalan walau D-ITG diam sebentar
                simulated_bytes_tx = int(target_total_bytes / flows_count)

            # Asumsi ukuran paket VoIP rata-rata ~180-200 bytes
            simulated_pkts_tx = int(simulated_bytes_tx / 180)
            if simulated_pkts_tx == 0 and simulated_bytes_tx > 0: simulated_pkts_tx = 1

            # RX sedikit random (simulasi loss/jitter kecil)
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
            # Log detail per flow (opsional, bisa dikomentari agar tidak spam)
            # self.logger.info(f"    -> {flow['src_ip']} to {flow['dst_ip']}: {simulated_bytes_tx} B (Real: {flow['real_bytes']})")