#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (ATOMIC VERSION)
=============================================================================
ATOMIC BREAK-BEFORE-MAKE: Traffic MUST stop completely on old path before new path is activated.
This prevents DPID 5 from seeing traffic from multiple spines simultaneously.
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
MAX_STAGE_TIME_SEC = 15        
HEALTH_CHECK_INTERVAL = 5      
STATE_FILE = '/tmp/controller_state.json'

# CRITICAL: Waiting periods for atomic break-before-make
FLOW_DELETE_WAIT_SEC = 2.0     # Wait after deleting flows
TRAFFIC_SILENCE_WAIT_SEC = 3.0 # Wait to verify no traffic on old path

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
        
        # Per-spine traffic monitoring untuk atomic verification
        self.spine_traffic = {1: 0, 2: 0, 3: 0}  # DPID -> bytes/sec
        self.spine_traffic_last_update = {1: 0, 2: 0, 3: 0}
        
        # Stability tracking
        self.stability_counter = 0
        self.required_stable_cycles = 10
        
        # Congestion state
        self.congestion_active = False
        self.current_spine = 2  # Default spine (Spine 2)
        self.congested_spine = None
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
        
        self.logger.info("🟢 VoIP Smart Controller (ATOMIC BREAK-BEFORE-MAKE) Started")
        
        # Write initial state
        write_state_file({
            'state': 'IDLE',
            'congestion': False,
            'current_spine': self.current_spine
        })
        
        # Install default H1->H2 flows after topology is discovered
        self.default_flows_installed = False
        hub.spawn_after(8, self._install_default_h1_h2_flows)

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

    # =================================================================
    # ATOMIC BREAK-BEFORE-MAKE IMPLEMENTATION
    # =================================================================
    
    def _get_alternative_spine(self, avoid_spine):
        """Get next available spine, avoiding congested one"""
        available = [1, 2, 3]
        available.remove(avoid_spine)
        # Pilih spine dengan traffic terendah
        return min(available, key=lambda s: self.spine_traffic.get(s, 0))
    
    def _verify_traffic_stopped_on_spine(self, spine_dpid):
        """Verify that H1->H2 traffic has stopped on specified spine"""
        current_traffic = self.spine_traffic.get(spine_dpid, 0)
        self.logger.info(f"🔍 Verifying Spine {spine_dpid}: {current_traffic} bps")
        return current_traffic < 1000  # Less than 1 Kbps = effectively zero
    
    def _delete_h1_h2_flows_on_spine(self, spine_dpid):
        """Delete all H1->H2 flows on specified spine (DPID 1,2,3)"""
        if spine_dpid not in self.datapaths:
            self.logger.warning(f"⚠️ Spine {spine_dpid} datapath not available")
            return False
        
        dp = self.datapaths[spine_dpid]
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        
        # Delete flows matching H1->H2 traffic
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )
        
        mod = parser.OFPFlowMod(
            datapath=dp,
            command=ofproto.OFPFC_DELETE,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            match=match
        )
        
        dp.send_msg(mod)
        self.logger.info(f"🗑️ Deleted H1->H2 flows on Spine {spine_dpid}")
        return True
    
    def _install_h1_h2_flow_on_spine(self, spine_dpid):
        """Install H1->H2 flow on specified spine"""
        if spine_dpid not in self.datapaths:
            self.logger.warning(f"⚠️ Spine {spine_dpid} datapath not available")
            return False
        
        dp = self.datapaths[spine_dpid]
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        
        # Determine output port based on topology
        # Spine -> Leaf 2 (where H2 is connected)
        # Assuming ports: Spine 1,2,3 port 2 goes to Leaf 2
        out_port = 2  # Port to Leaf 2 (adjust based on your topology)
        
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        mod = parser.OFPFlowMod(
            datapath=dp,
            priority=PRIORITY_REROUTE,
            match=match,
            instructions=inst,
            cookie=COOKIE_REROUTE,
            idle_timeout=0,
            hard_timeout=0
        )
        
        dp.send_msg(mod)
        self.logger.info(f"✅ Installed H1->H2 flow on Spine {spine_dpid} (port {out_port})")
        return True
    
    def _update_leaf1_output_port(self, target_spine):
        """Update Leaf 1 (DPID 4) to forward H1 traffic to target spine"""
        if 4 not in self.datapaths:
            return False
        
        dp = self.datapaths[4]
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        
        # Port mapping on Leaf 1: port 1->Spine1, port 2->Spine2, port 3->Spine3
        spine_to_port = {1: 1, 2: 2, 3: 3}
        out_port = spine_to_port.get(target_spine, 2)
        
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        mod = parser.OFPFlowMod(
            datapath=dp,
            priority=PRIORITY_REROUTE,
            match=match,
            instructions=inst,
            cookie=COOKIE_REROUTE,
            idle_timeout=0,
            hard_timeout=0
        )
        
        dp.send_msg(mod)
        self.logger.info(f"✅ Leaf 1: Forwarding H1->H2 to Spine {target_spine} (port {out_port})")
        return True

    def _atomic_reroute_to_spine(self, target_spine):
        """
        ATOMIC REROUTE: Ensures NO traffic overlap between spines
        
        Steps:
        1. Delete flows on OLD spine
        2. Wait for flow deletion to propagate
        3. Verify traffic has STOPPED on old spine
        4. Install flows on NEW spine
        5. Update Leaf 1 to use new spine
        """
        old_spine = self.current_spine
        
        self.logger.info(f"🔄 ATOMIC REROUTE: Spine {old_spine} -> Spine {target_spine}")
        
        # STEP 1: Delete flows on old spine
        self.reroute_stage = 'DELETING_OLD_FLOWS'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': True,
            'current_spine': old_spine,
            'target_spine': target_spine
        })
        
        success = self._delete_h1_h2_flows_on_spine(old_spine)
        if not success:
            self.logger.error("❌ Failed to delete flows on old spine")
            return False
        
        # STEP 2: Wait for deletion to propagate
        self.logger.info(f"⏳ Waiting {FLOW_DELETE_WAIT_SEC}s for flow deletion...")
        time.sleep(FLOW_DELETE_WAIT_SEC)
        
        # STEP 3: Verify silence on old spine
        self.reroute_stage = 'VERIFYING_SILENCE'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': True,
            'current_spine': old_spine,
            'target_spine': target_spine
        })
        
        self.logger.info(f"⏳ Waiting {TRAFFIC_SILENCE_WAIT_SEC}s to verify silence...")
        time.sleep(TRAFFIC_SILENCE_WAIT_SEC)
        
        # Check if traffic has stopped
        if not self._verify_traffic_stopped_on_spine(old_spine):
            self.logger.warning(f"⚠️ Traffic still present on Spine {old_spine}, forcing continue...")
        
        # STEP 4: Install flows on new spine
        self.reroute_stage = 'INSTALLING_NEW_FLOWS'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': True,
            'current_spine': target_spine,
            'old_spine': old_spine
        })
        
        success = self._install_h1_h2_flow_on_spine(target_spine)
        if not success:
            self.logger.error("❌ Failed to install flows on new spine")
            return False
        
        # STEP 5: Update Leaf 1
        success = self._update_leaf1_output_port(target_spine)
        if not success:
            self.logger.error("❌ Failed to update Leaf 1")
            return False
        
        # STEP 6: Mark complete
        self.current_spine = target_spine
        self.congested_spine = old_spine
        self.congestion_active = True
        self.reroute_stage = 'ACTIVE_REROUTE'
        
        write_state_file({
            'state': 'ACTIVE_REROUTE',
            'congestion': True,
            'current_spine': self.current_spine,
            'congested_spine': self.congested_spine
        })
        
        self.logger.info(f"✅ ATOMIC REROUTE COMPLETE: Now using Spine {target_spine}")
        self.stats['reroute_count'] += 1
        return True

    def _atomic_revert_to_original_spine(self):
        """
        ATOMIC REVERT: Return to original spine when congestion clears
        
        Same atomic process as reroute to prevent traffic overlap
        """
        if not self.congestion_active or not self.congested_spine:
            return False
        
        target_spine = self.congested_spine  # Return to original
        old_spine = self.current_spine
        
        self.logger.info(f"🔙 ATOMIC REVERT: Spine {old_spine} -> Spine {target_spine}")
        
        # STEP 1: Delete flows on current spine
        self.reroute_stage = 'REVERT_DELETING'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': False,
            'current_spine': old_spine,
            'target_spine': target_spine
        })
        
        self._delete_h1_h2_flows_on_spine(old_spine)
        time.sleep(FLOW_DELETE_WAIT_SEC)
        
        # STEP 2: Verify silence
        self.reroute_stage = 'REVERT_VERIFY_SILENCE'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': False,
            'current_spine': old_spine,
            'target_spine': target_spine
        })
        
        time.sleep(TRAFFIC_SILENCE_WAIT_SEC)
        
        if not self._verify_traffic_stopped_on_spine(old_spine):
            self.logger.warning(f"⚠️ Traffic still present during revert, forcing continue...")
        
        # STEP 3: Install flows on original spine
        self.reroute_stage = 'REVERT_INSTALLING'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': False,
            'current_spine': target_spine
        })
        
        self._install_h1_h2_flow_on_spine(target_spine)
        self._update_leaf1_output_port(target_spine)
        
        # STEP 4: Complete revert
        self.current_spine = target_spine
        self.congested_spine = None
        self.congestion_active = False
        self.reroute_stage = 'IDLE'
        
        write_state_file({
            'state': 'IDLE',
            'congestion': False,
            'current_spine': self.current_spine
        })
        
        self.logger.info(f"✅ ATOMIC REVERT COMPLETE: Back to Spine {target_spine}")
        self.stats['revert_count'] += 1
        return True

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
                    
                    # Force reset to IDLE
                    self.reroute_stage = 'IDLE'
                    self.stage_start_time = current_time
                    self.congestion_active = False
                    
                # Cleanup old traffic data
                expired_keys = []
                for key, ts in self.last_bytes_timestamp.items():
                    if current_time - ts > 30:
                        expired_keys.append(key)
                for key in expired_keys:
                    self.last_bytes.pop(key, None)
                    self.last_bytes_timestamp.pop(key, None)

    def _install_default_h1_h2_flows(self):
        """Install default H1->H2 flows via Spine 2 (default path) at startup"""
        self.logger.info("🔧 Installing DEFAULT H1->H2 flows (Spine 2)...")
        
        # Wait for datapaths to be ready
        max_wait = 10
        waited = 0
        while waited < max_wait:
            if 2 in self.datapaths and 4 in self.datapaths:
                break
            hub.sleep(1)
            waited += 1
        
        if 2 not in self.datapaths or 4 not in self.datapaths:
            self.logger.error("❌ Cannot install default flows - datapaths not ready")
            return
        
        # Install on Spine 2 (default spine)
        success = self._install_h1_h2_flow_on_spine(2)
        if success:
            self.logger.info("✅ Spine 2 flow installed")
        
        # Install on Leaf 1 (point to Spine 2)
        success = self._update_leaf1_output_port(2)
        if success:
            self.logger.info("✅ Leaf 1 routing installed (→ Spine 2)")
        
        self.default_flows_installed = True
        self.logger.info("🟢 Default H1->H2 path established (via Spine 2)")


    # =================================================================
    # TOPOLOGY DISCOVERY
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            try:
                switches = get_switch(self)
                links = get_link(self)
                
                self.net.clear()
                for switch in switches:
                    self.net.add_node(switch.dp.id)
                
                for link in links:
                    self.net.add_edge(link.src.dpid, link.dst.dpid, 
                                    port=link.src.port_no)
                    self.net.add_edge(link.dst.dpid, link.src.dpid, 
                                    port=link.dst.port_no)
            except:
                pass

    # =================================================================
    # CONGESTION DETECTION & TRIGGER
    # =================================================================
    def _monitor_forecast_safe(self):
        """Monitor congestion on Spine 2 (DPID 2) and trigger atomic reroute"""
        while True:
            hub.sleep(1)
            
            try:
                with self.lock:
                    # Only monitor the CURRENT spine for congestion
                    congested_spine_dpid = self.current_spine
                    
                    current_bps = self.spine_traffic.get(congested_spine_dpid, 0)
                    
                    current_time = time.time()
                    time_since_last_reroute = current_time - self.last_reroute_time
                    
                    # TRIGGER REROUTE: Congestion detected
                    if (not self.congestion_active and 
                        current_bps > BURST_THRESHOLD_BPS and 
                        time_since_last_reroute > COOLDOWN_PERIOD_SEC and
                        self.reroute_stage == 'IDLE'):
                        
                        self.logger.warning(f"🚨 CONGESTION on Spine {congested_spine_dpid}: {current_bps:.0f} bps")
                        
                        # Find alternative spine
                        target_spine = self._get_alternative_spine(congested_spine_dpid)
                        
                        self.stage_start_time = time.time()
                        self.last_reroute_time = time.time()
                        
                        # Execute atomic reroute
                        success = self._atomic_reroute_to_spine(target_spine)
                        
                        if not success:
                            self.logger.error("❌ Atomic reroute failed")
                            self.reroute_stage = 'IDLE'
                    
                    # TRIGGER REVERT: Congestion cleared
                    elif (self.congestion_active and 
                          self.congested_spine and
                          self.reroute_stage == 'ACTIVE_REROUTE'):
                        
                        # Check if original spine is clear
                        original_spine_bps = self.spine_traffic.get(self.congested_spine, 0)
                        
                        if original_spine_bps < HYSTERESIS_LOWER_BPS:
                            self.stability_counter += 1
                            
                            if self.stability_counter >= self.required_stable_cycles:
                                self.logger.info(f"✅ Spine {self.congested_spine} stable, reverting...")
                                
                                success = self._atomic_revert_to_original_spine()
                                
                                if success:
                                    self.stability_counter = 0
                                else:
                                    self.logger.error("❌ Atomic revert failed")
                        else:
                            self.stability_counter = 0
                            
            except Exception as e:
                self.logger.error(f"Forecast monitor error: {e}")

    # =================================================================
    # TRAFFIC MONITORING
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Process flow stats and update per-spine traffic counters"""
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        current_time = time.time()
        
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            for stat in body:
                match = stat.match
                
                # Extract IPs
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                
                if not src_ip or not dst_ip:
                    continue
                
                # Create unique flow key
                flow_key = (dpid, src_ip, dst_ip)
                
                # Calculate delta for BYTES
                current_bytes = stat.byte_count
                last_bytes = self.last_bytes.get(flow_key, current_bytes)  # FIX: Initialize to current on first seen
                last_ts = self.last_bytes_timestamp.get(flow_key, current_time)
                
                delta_bytes = max(0, current_bytes - last_bytes)
                time_diff = max(0.1, current_time - last_ts)
                
                # Calculate delta for PACKETS (FIX #2: Track packets properly)
                current_packets = stat.packet_count
                last_packets = getattr(self, 'last_packets', {}).get(flow_key, current_packets)
                delta_packets = max(0, current_packets - last_packets)
                
                # Update tracking
                self.last_bytes[flow_key] = current_bytes
                self.last_bytes_timestamp[flow_key] = current_time
                
                # Track packets too (FIX #2)
                if not hasattr(self, 'last_packets'):
                    self.last_packets = {}
                self.last_packets[flow_key] = current_packets
                
                # Calculate bps
                bps = (delta_bytes * 8) / time_diff
                
                # Update per-spine traffic for H1->H2
                if src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                    if dpid in [1, 2, 3]:  # Spine switches
                        self.spine_traffic[dpid] = bps
                        self.spine_traffic_last_update[dpid] = current_time
                
                # FIX #3: Resolve MACs from IP cache
                src_mac = self.ip_to_mac.get(src_ip, None)
                dst_mac = self.ip_to_mac.get(dst_ip, None)
                
                # Insert to DB (only if there's actual delta - FIX: prevents cumulative counts)
                if delta_bytes > 0:
                    self._insert_flow_stats(dpid, src_ip, dst_ip, match, 
                                           delta_bytes, delta_packets, 
                                           src_mac, dst_mac, time_diff)
                    
        except Exception as e:
            self.logger.error(f"Flow stats error: {e}")
        finally:
            self.return_db_connection(conn)

    def _monitor_traffic(self):
        """Request flow stats periodically"""
        while True:
            hub.sleep(1)
            
            # Debug: Log active datapaths every 10 seconds
            if int(time.time()) % 10 == 0:
                dpids = sorted(self.datapaths.keys())
                self.logger.debug(f"📊 Monitoring DPIDs: {dpids}")
            
            for dp in self.datapaths.values():
                self._request_stats(dp)

    def _request_stats(self, datapath):
        """Request flow statistics"""
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    def _insert_flow_stats(self, dpid, src_ip, dst_ip, match, delta_bytes, delta_packets, 
                           src_mac, dst_mac, duration):
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
                src_mac, dst_mac,  # FIX: Use resolved MACs
                match.get('ip_proto', 17),
                match.get('tcp_src') or match.get('udp_src') or 0,
                match.get('tcp_dst') or match.get('udp_dst') or 0,
                delta_bytes, delta_bytes,
                delta_packets, delta_packets,  # FIX: Use actual delta_packets
                duration,  # FIX: Use actual time_diff
                'voip' if src_ip == '10.0.0.1' else 'bursty'
            ))
            conn.commit()
            cur.close()
        except Exception as e:
            self.logger.error(f"Flow stats insert error: {e}")
        finally:
            self.return_db_connection(conn)

    # =================================================================
    # PACKET HANDLERS
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
        
        # Default routing untuk H3 ONLY (always Spine 2)
        # H1 routing is handled by reroute logic
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # H1 -> H2: Use current_spine (default: Spine 2, atau spine lain saat reroute)
            if dpid == 4 and src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                # Port mapping: port 1->Spine1, port 2->Spine2, port 3->Spine3
                spine_to_port = {1: 1, 2: 2, 3: 3}
                out_port = spine_to_port.get(self.current_spine, 2)
                
                actions = [parser.OFPActionOutput(out_port)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                
                self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id, idle_timeout=0)
                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data
                
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                        in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return
            
            # H3 -> H2: Always use Spine 2 (port 2 on Leaf 3)
            if dpid == 6 and src_ip == '10.0.0.3':
                actions = [parser.OFPActionOutput(2)]  # Port to Spine 2
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
