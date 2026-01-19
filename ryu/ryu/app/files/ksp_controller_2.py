#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (SAFE VERSION)
==========================================================================
BREAK-BEFORE-MAKE Pattern dengan deadlock protection & health monitoring.
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
import threading
import time
import json
import random
import os

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

# Threshold Congestion
BURST_THRESHOLD_BPS = 120000 
HYSTERESIS_LOWER_BPS = 88000   
COOLDOWN_PERIOD_SEC = 20       

# Constants for Industrial Flow Management
COOKIE_REROUTE = 0xDEADBEEF    
PRIORITY_REROUTE = 30000       
PRIORITY_USER = 10             
PRIORITY_DEFAULT = 1           

# Safety Limits
MAX_STAGE_TIME_SEC = 15        # Max waktu per stage
HEALTH_CHECK_INTERVAL = 5      # Detik
STATE_FILE = '/tmp/controller_state.json'

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def safe_division(numerator, denominator):
    """Mencegah division by zero"""
    return numerator / denominator if denominator != 0 else 0

def write_state_file(state_data):
    """Tulis state controller ke file untuk monitoring external"""
    try:
        state_data['timestamp'] = datetime.now().isoformat()
        with open(STATE_FILE, 'w') as f:
            json.dump(state_data, f)
    except:
        pass

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        
        # Thread safety lock
        self.lock = threading.RLock()
        
        # State Management
        self.reroute_stage = 'IDLE'
        self.stage_start_time = time.time()
        self.stopped_spines = []
        self.trigger_val_cache = 0
        
        # Traffic counters dengan expiration
        self.last_bytes = {}
        self.last_bytes_timestamp = {}
        
        # Stability tracking
        self.stability_counter = 0
        self.required_stable_cycles = 10
        
        # Congestion state
        self.congestion_active = False
        self.current_reroute_path = None
        self.last_reroute_time = 0
        
        # Network topology
        self.datapaths = {}
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.net = nx.DiGraph()
        
        # Connection pooling
        self.db_pool = None
        self.connect_database_pool()
        
        # Health monitoring
        self.stats = {
            'reroute_count': 0,
            'revert_count': 0,
            'stuck_count': 0,
            'last_health_check': time.time()
        }
        
        # Threads
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast_safe)
        self.topology_thread = hub.spawn(self._discover_topology)
        self.health_thread = hub.spawn(self._health_monitor)
        
        self.logger.info("🟢 VoIP Smart Controller (SAFE BREAK-BEFORE-MAKE) Started")
        
        # Write initial state
        write_state_file({
            'state': 'IDLE',
            'congestion': False,
            'current_path': None
        })

    def connect_database_pool(self):
        """Gunakan connection pooling untuk efisiensi"""
        try:
            from psycopg2 import pool
            self.db_pool = pool.SimpleConnectionPool(1, 5, **DB_CONFIG)
            self.logger.info("✅ Database pool created (min=1, max=5)")
        except Exception as e:
            self.logger.warning(f"DB Pool Error: {e}")
            self.db_pool = None
    
    def get_db_connection(self):
        """Dapatkan koneksi dari pool"""
        if not self.db_pool:
            return None
        try:
            return self.db_pool.getconn()
        except Exception as e:
            self.logger.error(f"DB Connection Error: {e}")
            return None
    
    def return_db_connection(self, conn):
        """Kembalikan koneksi ke pool"""
        if self.db_pool and conn:
            self.db_pool.putconn(conn)

    def _write_state_file(self, data):
        """Tulis state ke file untuk monitoring"""
        try:
            data['timestamp'] = datetime.now().isoformat()
            with open(STATE_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            self.logger.debug(f"Failed to write state file: {e}")
    # =================================================================
    # HEALTH MONITOR & STUCK DETECTION
    # =================================================================
    def _health_monitor(self):
        """Deteksi dan recover dari stuck state"""
        while True:
            hub.sleep(HEALTH_CHECK_INTERVAL)
            
            with self.lock:
                current_time = time.time()
                stage_duration = current_time - self.stage_start_time
                
                # Jika stage terlalu lama, force recover
                if stage_duration > MAX_STAGE_TIME_SEC and self.reroute_stage != 'IDLE':
                    self.logger.error(f"⚠️ HEALTH ALERT: Stage {self.reroute_stage} stuck for {stage_duration:.1f}s")
                    self.stats['stuck_count'] += 1
                    
                    # Emergency recovery logic
                    if self.reroute_stage in ['VERIFYING', 'REVERT_VERIFY']:
                        self.logger.warning("🔄 Force finishing stage...")
                        # Install flow default ke Spine 3 sebagai fallback
                        self._emergency_fallback_to_spine3()
                    
                    # Reset ke IDLE
                    self.reroute_stage = 'IDLE'
                    self.stage_start_time = current_time
                    self.congestion_active = False
                    
                # Cleanup old traffic data (lebih dari 30 detik)
                expired_keys = []
                for key, ts in self.last_bytes_timestamp.items():
                    if current_time - ts > 30:
                        expired_keys.append(key)
                for key in expired_keys:
                    self.last_bytes.pop(key, None)
                    self.last_bytes_timestamp.pop(key, None)
    
    def _emergency_fallback_to_spine3(self):
        """Emergency recovery: install flow ke Spine 3"""
        target_src = '10.0.0.1'
        target_dst = '10.0.0.2'
        
        try:
            # Install flow ke Spine 3
            for dpid in [4, 3, 5]:
                if dpid in self.datapaths:
                    dp = self.datapaths[dpid]
                    # Tentukan out_port
                    if dpid == 4:
                        out_port = 3  # Port ke Spine 3
                    elif dpid == 3:
                        out_port = 2  # Port ke Leaf 5
                    elif dpid == 5:
                        out_port = 1  # Port ke H2
                    else:
                        continue
                    
                    self._install_flow_specific(dp, target_src, target_dst, out_port, 
                                               priority=PRIORITY_REROUTE, 
                                               cookie=COOKIE_REROUTE)
            
            self.current_reroute_path = [4, 3, 5]
            self.congestion_active = False
            self.logger.info("🔄 Emergency fallback to Spine 3 completed")
            
        except Exception as e:
            self.logger.error(f"Emergency fallback failed: {e}")

    # =================================================================
    # TOPOLOGY DISCOVERY
    # =================================================================
    def _discover_topology(self):
        """Periodic topology discovery"""
        while True:
            hub.sleep(5)
            with self.lock:
                self.net.clear()
                
                switches = get_switch(self, None)
                for s in switches:
                    self.net.add_node(s.dp.id)
                
                links = get_link(self, None)
                for l in links:
                    self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    # =================================================================
    # SAFE FORECAST MONITORING (BREAK-BEFORE-MAKE)
    # =================================================================
    def _monitor_forecast_safe(self):
        """FIXED VERSION: Break-Before-Make dengan timeout protection"""
        target_src = '10.0.0.1'
        target_dst = '10.0.0.2'
        
        self.logger.info("👁️  SAFE Monitor: Break-Before-Make Logic Active")
        
        while True:
            hub.sleep(1.0)
            
            with self.lock:
                current_stage = self.reroute_stage
                stage_duration = time.time() - self.stage_start_time
                
                # --- STAGE 1: IDLE (Monitor for burst) ---
                if current_stage == 'IDLE':
                    # Cooldown period check
                    if time.time() - self.last_reroute_time < COOLDOWN_PERIOD_SEC:
                        continue
                    
                    pred_bps = self._get_latest_prediction()
                    if pred_bps is None:
                        continue
                    
                    if pred_bps > BURST_THRESHOLD_BPS:
                        # Deteksi spine aktif yang membawa traffic H1->H2
                        current_spines = self._get_active_spines_for_flow(target_src, target_dst)
                        if not current_spines:
                            continue
                        
                        self.logger.warning(f"🚀 BURST DETECTED ({pred_bps:.0f} bps). Starting BREAK phase...")
                        
                        # BREAK PHASE: Stop semua flow di spine aktif
                        for sp_dpid in current_spines:
                            self._emergency_stop_flow_fixed(sp_dpid, target_src, target_dst)
                        
                        self.stopped_spines = current_spines
                        self.trigger_val_cache = pred_bps
                        
                        # CLEAN CACHE untuk spine yang di-stop
                        self._clean_cache_for_spines(current_spines, target_src, target_dst)
                        
                        # Pindah ke stage VERIFYING_SILENCE
                        self.reroute_stage = 'VERIFYING_SILENCE'
                        self.stage_start_time = time.time()
                        self.silence_check_counter = 0
                        
                        # Log dan update state
                        self._log_event("BREAK_STARTED", f"Stopped spines: {current_spines}", pred_bps)
                        self._write_state_file({
                            'state': 'VERIFYING_SILENCE',
                            'stopped_spines': current_spines,
                            'pred_bps': pred_bps
                        })
                
                # --- STAGE 2: VERIFYING_SILENCE (Wait for traffic to stop) ---
                elif current_stage == 'VERIFYING_SILENCE':
                    self.silence_check_counter += 1
                    
                    # TIMEOUT PROTECTION: Max 10 detik
                    if self.silence_check_counter > 10:
                        self.logger.warning(f"⚠️ SILENCE VERIFICATION TIMEOUT ({self.silence_check_counter}s). FORCING CONTINUE...")
                        all_silent = True  # Force continue
                    else:
                        all_silent = self._are_spines_silent_fixed(self.stopped_spines, target_src, target_dst)
                    
                    if all_silent:
                        self.logger.info(f"✅ Path clear. Starting MAKE phase...")
                        
                        # Tentukan spine yang dihindari
                        avoid_node = self.stopped_spines[0] if self.stopped_spines else 3
                        
                        # MAKE PHASE: Install jalur baru
                        success = self.perform_dynamic_reroute(target_src, target_dst, avoid_node, 
                                                            self.trigger_val_cache)
                        
                        if success:
                            self.reroute_stage = 'REROUTED'
                            self.stage_start_time = time.time()
                            self.stability_counter = 0
                            self.congestion_active = True
                            self.last_reroute_time = time.time()
                            self.stats['reroute_count'] += 1
                            
                            self._log_event("REROUTE_COMPLETE", 
                                        f"Path: {self.current_reroute_path}", 
                                        self.trigger_val_cache)
                            self._write_state_file({
                                'state': 'REROUTED',
                                'current_path': self.current_reroute_path,
                                'congestion': True
                            })
                        else:
                            # Jika gagal, fallback ke default path
                            self.logger.error("❌ Reroute installation failed. Fallback to default...")
                            self._force_install_default_path()
                            self.reroute_stage = 'IDLE'
                    else:
                        # Log progress jika belum silent
                        if self.silence_check_counter % 3 == 0:
                            self.logger.info(f"⏳ Waiting for silence... {self.silence_check_counter}/10s")
                        
                        # Periodic cleanup untuk pastikan cache fresh
                        if self.silence_check_counter % 5 == 0:
                            self._clean_cache_for_spines(self.stopped_spines, target_src, target_dst)
                
                # --- STAGE 3: REROUTED (Monitor for stability) ---
                elif current_stage == 'REROUTED':
                    pred_bps = self._get_latest_prediction()
                    if pred_bps is None:
                        continue
                    
                    # LOGIKA STABILITAS DENGAN HYSTERESIS
                    if pred_bps <= HYSTERESIS_LOWER_BPS:
                        self.stability_counter += 1
                        
                        if self.stability_counter % 3 == 0:  # Log setiap 3 cycle
                            self.logger.info(f"📉 Traffic calming ({pred_bps:.0f} bps). Stability: {self.stability_counter}/{self.required_stable_cycles}")
                        
                        if self.stability_counter >= self.required_stable_cycles:
                            self.logger.info("🛡️  STABLE TRAFFIC. Preparing revert...")
                            
                            # BREAK PHASE untuk revert: Stop jalur backup
                            current_backup_spines = self._get_active_spines_for_flow(target_src, target_dst)
                            if current_backup_spines:
                                for sp_dpid in current_backup_spines:
                                    self._emergency_stop_flow_fixed(sp_dpid, target_src, target_dst)
                                
                                self.stopped_spines = current_backup_spines
                                
                                # Clean cache untuk backup spines
                                self._clean_cache_for_spines(current_backup_spines, target_src, target_dst)
                                
                                self.reroute_stage = 'REVERT_VERIFY'
                                self.stage_start_time = time.time()
                                self.silence_check_counter = 0
                                
                                self._log_event("REVERT_STARTED", 
                                            f"Stopping backup spines: {current_backup_spines}", 
                                            0)
                                self._write_state_file({
                                    'state': 'REVERT_VERIFY',
                                    'stopped_spines': current_backup_spines
                                })
                            else:
                                # Jika tidak ada backup spines, langsung revert
                                self.logger.info("⚠️ No backup spines found. Direct revert...")
                                success = self.perform_dynamic_revert(target_src, target_dst)
                                if success:
                                    self.reroute_stage = 'IDLE'
                                    self.congestion_active = False
                    else:
                        # Reset jika traffic naik lagi
                        if self.stability_counter > 0:
                            self.logger.warning(f"⚠️ Stability reset. Traffic: {pred_bps:.0f} bps")
                        self.stability_counter = 0
                
                # --- STAGE 4: REVERT_VERIFY (Verify backup path is silent) ---
                elif current_stage == 'REVERT_VERIFY':
                    self.silence_check_counter += 1
                    
                    # TIMEOUT PROTECTION: Max 15 detik
                    if self.silence_check_counter > 15:
                        self.logger.warning(f"⚠️ REVERT VERIFICATION TIMEOUT ({self.silence_check_counter}s). FORCING REVERT...")
                        all_silent = True  # Force revert
                    else:
                        all_silent = self._are_spines_silent_fixed(self.stopped_spines, target_src, target_dst)
                    
                    if all_silent:
                        self.logger.info("✅ Backup path silent. Starting revert MAKE phase...")
                        
                        # MAKE PHASE untuk revert: Install jalur normal (Spine 3)
                        success = self.perform_dynamic_revert(target_src, target_dst)
                        
                        if success:
                            self.reroute_stage = 'IDLE'
                            self.stage_start_time = time.time()
                            self.congestion_active = False
                            self.stats['revert_count'] += 1
                            self.stability_counter = 0
                            
                            self._log_event("REVERT_COMPLETE", 
                                        f"Restored path: {self.current_reroute_path}", 
                                        0)
                            self._write_state_file({
                                'state': 'IDLE',
                                'current_path': self.current_reroute_path,
                                'congestion': False
                            })
                        else:
                            self.logger.error("❌ Revert failed. Emergency fallback...")
                            self._force_install_default_path()
                            self.reroute_stage = 'IDLE'
                    else:
                        # Jika belum silent, drain dengan periodic stop
                        if self.silence_check_counter % 3 == 0:
                            for sp in self.stopped_spines:
                                self._emergency_stop_flow_fixed(sp, target_src, target_dst)
                        
                        # Log progress
                        if self.silence_check_counter % 5 == 0:
                            self.logger.info(f"⏳ Draining backup path... {self.silence_check_counter}/15s")

    def _get_latest_prediction(self):
        """Ambil prediksi terbaru dari database dengan timeout"""
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            cur = conn.cursor()
            # Ambil prediksi yang tidak lebih dari 10 detik yang lalu
            cur.execute("""
                SELECT y_pred FROM forecast_1h 
                WHERE ts_created >= NOW() - INTERVAL '10 seconds'
                ORDER BY ts_created DESC LIMIT 1
            """)
            result = cur.fetchone()
            cur.close()
            
            if result:
                return float(result[0])
            return None
        except Exception as e:
            self.logger.error(f"Prediction query error: {e}")
            return None
        finally:
            self.return_db_connection(conn)

    # =================================================================
    # CORE REROUTE/REVERT FUNCTIONS
    # =================================================================
    

    def perform_dynamic_reroute(self, src_ip, dst_ip, avoid_dpid, trigger_val):
        """
        BREAK-BEFORE-MAKE: Sudah stop duluan, sekarang install jalur baru
        """
        with self.lock:
            try:
                src_sw = 4  # Leaf 4 (H1)
                dst_sw = 5  # Leaf 5 (H2)
                
                # Cari semua jalur alternatif
                all_paths = list(nx.shortest_simple_paths(self.net, src_sw, dst_sw))
                
                # Pilih jalur yang tidak melewati avoid_dpid
                alt_path = None
                for path in all_paths:
                    if avoid_dpid not in path:
                        alt_path = path
                        break
                
                if not alt_path:
                    self.logger.error("❌ No alternative path found!")
                    return False
                
                self.logger.info(f"🛣️  Installing REROUTE path: {alt_path}")
                
                # Install flow untuk setiap hop (dari belakang ke depan)
                for i in range(len(alt_path) - 1, 0, -1):
                    curr = alt_path[i-1]
                    nxt = alt_path[i]
                    
                    if self.net.has_edge(curr, nxt):
                        out_port = self.net[curr][nxt]['port']
                        dp = self.datapaths.get(curr)
                        
                        if dp:
                            self._install_flow_specific(dp, src_ip, dst_ip, out_port,
                                                       priority=PRIORITY_REROUTE,
                                                       cookie=COOKIE_REROUTE)
                            self._send_barrier(dp)
                
                self.current_reroute_path = alt_path
                self._log_event("REROUTE_COMPLETE", f"Path: {alt_path}", trigger_val)
                
                return True
                
            except Exception as e:
                self.logger.error(f"Reroute installation failed: {e}")
                return False

    def perform_dynamic_revert(self, src_ip, dst_ip):
        """
        Revert ke Spine 3 dengan BREAK-BEFORE-MAKE
        """
        with self.lock:
            try:
                src_sw = 4
                dst_sw = 5
                preferred_spine = 3
                
                # Cek ketersediaan Spine 3
                if (self.net.has_edge(src_sw, preferred_spine) and 
                    self.net.has_edge(preferred_spine, dst_sw)):
                    
                    target_path = [src_sw, preferred_spine, dst_sw]
                    self.logger.info(f"🔙 REVERTING to Spine 3: {target_path}")
                    
                    # Install flow ke Spine 3
                    for i in range(len(target_path) - 1, 0, -1):
                        curr = target_path[i-1]
                        nxt = target_path[i]
                        
                        if self.net.has_edge(curr, nxt):
                            out_port = self.net[curr][nxt]['port']
                            dp = self.datapaths.get(curr)
                            
                            if dp:
                                self._install_flow_specific(dp, src_ip, dst_ip, out_port,
                                                          priority=PRIORITY_REROUTE,
                                                          cookie=COOKIE_REROUTE)
                                self._send_barrier(dp)
                    
                    self.current_reroute_path = target_path
                    self._log_event("REVERT_COMPLETE", f"Back to Spine 3", 0)
                    
                    return True
                else:
                    self.logger.warning("Spine 3 unavailable. Using shortest path...")
                    # Fallback ke shortest path
                    try:
                        fallback_path = nx.shortest_path(self.net, src_sw, dst_sw)
                        self.logger.info(f"Fallback path: {fallback_path}")
                        
                        for i in range(len(fallback_path) - 1, 0, -1):
                            curr = fallback_path[i-1]
                            nxt = fallback_path[i]
                            
                            if self.net.has_edge(curr, nxt):
                                out_port = self.net[curr][nxt]['port']
                                dp = self.datapaths.get(curr)
                                
                                if dp:
                                    self._install_flow_specific(dp, src_ip, dst_ip, out_port,
                                                              priority=PRIORITY_REROUTE,
                                                              cookie=COOKIE_REROUTE)
                                    self._send_barrier(dp)
                        
                        self.current_reroute_path = fallback_path
                        return True
                    except Exception as e:
                        self.logger.error(f"Fallback also failed: {e}")
                        return False
                        
            except Exception as e:
                self.logger.error(f"Revert failed: {e}")
                return False
    

    def _emergency_stop_flow_fixed(self, dpid_target, src_ip, dst_ip):
        """Stop flow dengan cache cleanup"""
        if dpid_target not in self.datapaths:
            return
        
        dp = self.datapaths[dpid_target]
        parser = dp.ofproto_parser
        
        # 1. CLEAN CACHE DULU sebelum delete flow
        self._clean_cache_for_spines([dpid_target], src_ip, dst_ip)
        
        # 2. Delete flow dari switch
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=src_ip,
            ipv4_dst=dst_ip
        )
        
        # Delete flow dengan priority reroute
        mod_reroute = parser.OFPFlowMod(
            datapath=dp,
            command=dp.ofproto.OFPFC_DELETE_STRICT,
            out_port=dp.ofproto.OFPP_ANY,
            out_group=dp.ofproto.OFPG_ANY,
            match=match,
            priority=PRIORITY_REROUTE
        )
        dp.send_msg(mod_reroute)
        
        # Juga delete flow dengan priority user
        mod_user = parser.OFPFlowMod(
            datapath=dp,
            command=dp.ofproto.OFPFC_DELETE_STRICT,
            out_port=dp.ofproto.OFPP_ANY,
            out_group=dp.ofproto.OFPG_ANY,
            match=match,
            priority=PRIORITY_USER
        )
        dp.send_msg(mod_user)
        
        self._send_barrier(dp)
        self.logger.debug(f"Stopped flow on DPID {dpid_target} and cleaned cache")

    def _clean_cache_for_spines(self, spines, src, dst):
        """Bersihkan cache untuk spine-spine tertentu"""
        current_time = time.time()
        
        for dpid in spines:
            prefix = f"{dpid}-{src}-{dst}"
            
            # Hapus semua entry yang match
            keys_to_delete = [k for k in self.last_bytes.keys() if k.startswith(prefix)]
            for key in keys_to_delete:
                self.last_bytes.pop(key, None)
                self.last_bytes_timestamp.pop(key, None)
            
            # Juga hapus dari silence cache
            if hasattr(self, 'temp_silence_cache'):
                silence_keys = [k for k in self.temp_silence_cache.keys() 
                            if k.startswith(f"silence_check_{prefix}")]
                for key in silence_keys:
                    self.temp_silence_cache.pop(key, None)

    def _are_spines_silent_fixed(self, dpids_list, src, dst):
        """FIXED: Cek silence dengan data freshness"""
        if not dpids_list:
            return True  # Empty list = silent
        
        current_time = time.time()
        
        for dpid in dpids_list:
            prefix = f"{dpid}-{src}-{dst}"
            matching_keys = [k for k in self.last_bytes.keys() if k.startswith(prefix)]
            
            if not matching_keys:
                # Tidak ada entry = silent
                continue
            
            # Cek setiap entry untuk data FRESH (≤ 2 detik)
            for key in matching_keys:
                last_update = self.last_bytes_timestamp.get(key, 0)
                
                # Jika data ≤ 2 detik yang lalu = NOT SILENT
                if current_time - last_update <= 2:
                    # Data fresh, cek apakah bytes bertambah
                    curr_bytes, _ = self.last_bytes[key]
                    
                    # Bandingkan dengan cache sebelumnya
                    cache_key = f"silence_check_{key}"
                    prev_bytes = getattr(self, 'temp_silence_cache', {}).get(cache_key, -1)
                    
                    if not hasattr(self, 'temp_silence_cache'):
                        self.temp_silence_cache = {}
                    
                    if prev_bytes == -1:
                        # First check, set cache dan anggap NOT silent
                        self.temp_silence_cache[cache_key] = curr_bytes
                        return False
                    
                    if curr_bytes != prev_bytes:
                        # Bytes berubah = NOT silent
                        self.temp_silence_cache[cache_key] = curr_bytes
                        return False
        
        return True  # Semua spine silent

    def _force_install_default_path(self):
        """Force install path default 4->3->5"""
        try:
            path = [4, 3, 5]
            for i in range(len(path) - 1, 0, -1):
                curr = path[i-1]
                nxt = path[i]
                if self.net.has_edge(curr, nxt):
                    out_port = self.net[curr][nxt]['port']
                    dp = self.datapaths.get(curr)
                    if dp:
                        self._install_flow_specific(dp, '10.0.0.1', '10.0.0.2', 
                                                out_port, PRIORITY_REROUTE, COOKIE_REROUTE)
            self.logger.info("🔄 Default path (4->3->5) force installed")
        except Exception as e:
            self.logger.error(f"Force install failed: {e}")

    # =================================================================
    # HELPER FUNCTIONS
    # =================================================================
    def _install_flow_specific(self, datapath, src_ip, dst_ip, out_port, priority, cookie):
        """Install flow dengan parameter lengkap"""
        parser = datapath.ofproto_parser
        
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=src_ip,
            ipv4_dst=dst_ip
        )
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=inst,
            cookie=cookie,
            idle_timeout=0,
            hard_timeout=0,
            flags=datapath.ofproto.OFPFF_SEND_FLOW_REM
        )
        datapath.send_msg(mod)
    
    def _send_barrier(self, datapath):
        """Ensure command completion"""
        datapath.send_msg(datapath.ofproto_parser.OFPBarrierRequest(datapath))
    
    def _get_active_spines_for_flow(self, src_ip, dst_ip):
        """Deteksi spine aktif yang membawa traffic H1->H2 - VERSI DIPERBAIKI"""
        active_spines = []
        current_time = time.time()
        
        for flow_key, (bytes_count, _) in self.last_bytes.items():
            # Parse flow key dengan berbagai format
            parts = flow_key.split('-')
            
            try:
                # Format: "dpid-src-dst" atau "dpid-src-dst-prio"
                if len(parts) >= 3:
                    dpid = int(parts[0])
                    flow_src = parts[1]
                    flow_dst = parts[2]
                else:
                    continue
                    
                # Filter untuk flow H1->H2 di spine (dpid 1-3)
                if (flow_src == src_ip and flow_dst == dst_ip and 
                    1 <= dpid <= 3):  # Hanya spine 1, 2, 3
                    
                    # Cek data freshness (tidak lebih dari 10 detik)
                    if current_time - self.last_bytes_timestamp.get(flow_key, 0) <= 10:
                        active_spines.append(dpid)
                        
            except (ValueError, IndexError) as e:
                self.logger.debug(f"Error parsing flow key {flow_key}: {e}")
                continue
        
        # Remove duplicates
        return list(set(active_spines))
    
    def _check_is_traffic_silent(self, dpid, src_ip, dst_ip):
        """Check if traffic on a specific dpid has stopped"""
        current_time = time.time()
        prefix = f"{dpid}-{src_ip}-{dst_ip}"
        
        # Cari semua flow yang match
        matching_keys = [k for k in self.last_bytes.keys() if k.startswith(prefix)]
        
        if not matching_keys:
            # No flow entries at all -> silent
            return True
        
        # If there are flows, check if any has been updated in the last 3 seconds
        for key in matching_keys:
            last_update = self.last_bytes_timestamp.get(key, 0)
            if current_time - last_update <= 3:
                # Data is fresh (within 3 seconds) -> not silent
                return False
        
        # All flows are stale (older than 3 seconds) -> silent
        return True
    
    
    
    def _log_event(self, event_type, description, trigger_value):
        """Log event ke database"""
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events
                (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, description, trigger_value))
            conn.commit()
            cur.close()
        except Exception as e:
            self.logger.error(f"Event log error: {e}")
        finally:
            self.return_db_connection(conn)

    # =================================================================
    # STATISTICS MONITORING
    # =================================================================
    def _monitor_traffic(self):
        """Monitor traffic statistics"""
        while True:
            with self.lock:
                for dp in list(self.datapaths.values()):
                    self._request_stats(dp)
            hub.sleep(1)
    
    def _request_stats(self, datapath):
        """Request flow statistics"""
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)
    
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Handle flow statistics reply - PERBAIKAN: Catat semua switch"""
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = datapath.id
        
        current_time = time.time()
        timestamp = datetime.now()
        
        # Dapatkan koneksi DB
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            cur = conn.cursor()
            
            for stat in body:
                # Skip default flows (priority 0)
                if stat.priority == 0:
                    continue
                
                match = stat.match
                
                # 1. Coba ambil IP dari match langsung
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                
                # 2. Jika tidak ada IP, coba resolve dari MAC
                if not src_ip:
                    src_mac = match.get('eth_src')
                    if src_mac:
                        src_ip = self._resolve_ip(src_mac)
                
                if not dst_ip:
                    dst_mac = match.get('eth_dst')
                    if dst_mac:
                        dst_ip = self._resolve_ip(dst_mac)
                
                # 3. Tentukan apakah ini traffic yang kita minati
                is_target_traffic = False
                traffic_label = 'other'
                
                if dst_ip == '10.0.0.2':
                    if src_ip == '10.0.0.1':
                        is_target_traffic = True
                        traffic_label = 'voip'
                    elif src_ip == '10.0.0.3':
                        is_target_traffic = True
                        traffic_label = 'bursty'
                
                # 4. Update traffic counters (untuk semua traffic, tidak hanya target)
                if src_ip and dst_ip:
                    flow_key = f"{dpid}-{src_ip}-{dst_ip}"
                    byte_count = stat.byte_count
                    packet_count = stat.packet_count
                    
                    # Calculate deltas
                    if flow_key in self.last_bytes:
                        last_bytes, last_packets = self.last_bytes[flow_key]
                        delta_bytes = max(0, byte_count - last_bytes)
                        delta_packets = max(0, packet_count - last_packets)
                    else:
                        delta_bytes = byte_count
                        delta_packets = packet_count
                    
                    # Update cache
                    self.last_bytes[flow_key] = (byte_count, packet_count)
                    self.last_bytes_timestamp[flow_key] = current_time
                    
                    # 5. Insert ke DB jika ada traffic DAN ini traffic target kita
                    if delta_bytes > 0 and is_target_traffic:
                        try:
                            cur.execute("""
                                INSERT INTO traffic.flow_stats_
                                (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                                ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                                pkts_tx, pkts_rx, duration_sec, traffic_label)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                timestamp, dpid, src_ip, dst_ip,
                                match.get('eth_src'), match.get('eth_dst'),
                                match.get('ip_proto', 17),
                                match.get('tcp_src') or match.get('udp_src') or 0,
                                match.get('tcp_dst') or match.get('udp_dst') or 0,
                                delta_bytes, delta_bytes,
                                delta_packets, delta_packets,
                                stat.duration_sec,  # JANGAN hardcode 1.0!
                                traffic_label
                            ))
                        except Exception as e:
                            self.logger.debug(f"DB insert error for dpid {dpid}: {e}")
            
            conn.commit()
            cur.close()
            
            # Debug logging untuk memastikan semua switch tercatat
            if random.random() < 0.1:  # Log 10% dari waktu
                dpids_with_traffic = set()
                for flow_key in self.last_bytes.keys():
                    parts = flow_key.split('-')
                    if len(parts) >= 1:
                        try:
                            dpids_with_traffic.add(int(parts[0]))
                        except:
                            pass
                
                if dpids_with_traffic:
                    self.logger.debug(f"📊 Active switches: {sorted(dpids_with_traffic)}")
            
        except Exception as e:
            self.logger.error(f"Flow stats processing error: {e}")
        finally:
            self.return_db_connection(conn)
    
    def _insert_flow_stats(self, dpid, src_ip, dst_ip, match, delta_bytes, delta_packets, duration):
        """Insert flow statistics ke database"""
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.flow_stats_
                (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                 ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                 pkts_tx, pkts_rx, duration_sec, traffic_label)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(), dpid, src_ip, dst_ip,
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
        except Exception as e:
            self.logger.error(f"Flow stats insert error: {e}")
        finally:
            self.return_db_connection(conn)
    
    def _resolve_ip(self, mac):
        """Resolve IP dari MAC address - versi lebih robust"""
        if not mac:
            return None
        
        # Coba dari cache
        ip = self.ip_to_mac.get(mac)
        if ip:
            return ip
        
        # Coba reverse lookup dari cache yang ada
        for cached_ip, cached_mac in self.ip_to_mac.items():
            if cached_mac == mac:
                return cached_ip
        
        # Jika tidak ditemukan, coba kueri ARP table switch (jika ada)
        # Fallback: return None
        return None

    # =================================================================
    # PACKET HANDLERS (Unchanged from your working code)
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
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority, 
                                   match=match, instructions=inst, idle_timeout=idle_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, 
                                   instructions=inst, idle_timeout=idle_timeout)
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
        
        # MAC learning
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port
        
        # Protocol extraction
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt:
            self.ip_to_mac[ip_pkt.src] = src
        
        # Default routing untuk H1 dan H3 (Spine 2)
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            COLLISION_PORT = 3  # Port ke Spine 2
            
            if (dpid == 4 and src_ip == '10.0.0.1') or (dpid == 6 and src_ip == '10.0.0.3'):
                # Skip jika congestion active dan ini H1 (biarkan reroute handle)
                if self.congestion_active and src_ip == '10.0.0.1':
                    return
                
                actions = [parser.OFPActionOutput(COLLISION_PORT)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                
                self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id, idle_timeout=0)
                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data
                
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                        in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return
        
        # ARP handling
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.ip_to_mac:
                target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                e = ethernet.ethernet(dst=src, src=target_mac, ethertype=ether_types.ETH_TYPE_ARP)
                a = arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=arp_pkt.dst_ip, 
                           dst_mac=src, dst_ip=arp_pkt.src_ip)
                p = packet.Packet()
                p.add_protocol(e)
                p.add_protocol(a)
                p.serialize()
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, 
                                         in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
                datapath.send_msg(out)
                return
        
        # Standard switching logic
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
            self.add_flow(datapath, PRIORITY_DEFAULT, match, actions, msg.buffer_id, idle_timeout=60)
        
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, 
                                 in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)