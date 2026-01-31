#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP QoS with FORECAST-BASED Rerouting
=============================================================================
FORECAST MODE: Read y_pred from forecast_1h table to predict congestion
PROACTIVE: Reroute H1->H2 BEFORE burst happens
CLEAN DELETION: Remove ALL flows to prevent counter accumulation
H3 ISOLATION: H3 always uses Spine 2, never rerouted
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
from datetime import datetime, timedelta
import threading
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

# Forecast Configuration
FORECAST_CHECK_INTERVAL = 3         # Check forecast setiap 3 detik
FORECAST_THRESHOLD_BPS = 200000     # 100 Kbps threshold untuk reroute
FORECAST_LEAD_TIME_SEC = 10         # Reroute 10 detik sebelum predicted congestion
REVERT_THRESHOLD_BPS = 80900        # Revert jika forecast < 70 Kbps

# Stability
STABILITY_CYCLES_REQUIRED = 8      # Butuh 8 cycle stabil sebelum revert

# OpenFlow
COOKIE_REROUTE = 0xDEADBEEF    
PRIORITY_REROUTE = 30000       
PRIORITY_USER = 10             
PRIORITY_DEFAULT = 1           

# Timing
FLOW_DELETE_WAIT_SEC = 2.0 
TRAFFIC_SETTLE_WAIT_SEC = 2.0 
STATE_FILE = '/tmp/controller_state.json'

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def write_state_file(state_data):
    """Write controller state to file for external monitoring"""
    try:
        state_data['timestamp'] = datetime.now().isoformat()
        with open(STATE_FILE, 'w') as f:
            json.dump(state_data, f)
    except:
        pass

class VoIPForecastController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPForecastController, self).__init__(*args, **kwargs)
        
        # Thread safety
        self.lock = threading.RLock()
        
        # State Management
        self.reroute_stage = 'IDLE'
        self.stage_start_time = time.time()
        
        # Traffic counters - CRITICAL: Reset on reroute
        self.last_bytes = {}
        self.last_bytes_timestamp = {}
        self.last_packets = {}
        
        # Per-spine traffic monitoring
        self.spine_traffic = {1: 0, 2: 0, 3: 0}
        
        # Stability tracking
        self.stability_counter = 0
        
        # Congestion state
        self.congestion_active = False
        self.current_spine = 2          # START: Both H1 and H3 via Spine 2
        self.original_spine = 2         # Store original for revert
        self.last_reroute_time = 0
        
        # Forecast tracking
        self.last_forecast_value = 0
        self.last_forecast_time = None
        
        # Network topology
        self.datapaths = {}
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.net = nx.DiGraph()
        
        # Database
        self.db_pool = None
        self.connect_database_pool()
        
        # Stats
        self.stats = {
            'forecast_reroute': 0,
            'forecast_revert': 0,
            'total_reroutes': 0,
            'total_reverts': 0
        }
        
        # Threads
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        hub.spawn_after(15, self._start_forecast)  # Tunggu lebih lama, pastikan default flows selesai
        self.topology_thread = hub.spawn(self._discover_topology)
        
        self.logger.info("🟢 VoIP Forecast Controller Started")
        self.logger.info("📊 Forecast source: forecast_1h.y_pred (DPID 5)")
        
        write_state_file({
            'state': 'IDLE',
            'congestion': False,
            'current_spine': self.current_spine,
            'forecast_mode': True
        })
        
        # Install default flows
        self.default_flows_installed = False
        hub.spawn_after(2, self._install_default_flows)
        

    def _start_forecast(self):
        # Wait for default flows to be installed
        max_wait = 20
        waited = 0
        while not self.default_flows_installed and waited < max_wait:
            hub.sleep(1)
            waited += 1
        
        if not self.default_flows_installed:
            self.logger.warning("⚠️ Starting forecast without default flows!")
        
        self.forecast_thread = hub.spawn(self._forecast_monitor)
        self.logger.info("📊 Forecast monitor started")


    def connect_database_pool(self):
        """Create database connection pool"""
        try:
            from psycopg2 import pool
            self.db_pool = pool.SimpleConnectionPool(1, 5, **DB_CONFIG)
            self.logger.info("✅ Database pool created")
        except Exception as e:
            self.logger.warning(f"⚠️ DB Pool Error: {e}")
            self.db_pool = None
    
    def get_db_connection(self):
        """Get connection from pool"""
        if not self.db_pool:
            return None
        try:
            return self.db_pool.getconn()
        except Exception as e:
            self.logger.error(f"❌ DB Connection Error: {e}")
            return None
    
    def return_db_connection(self, conn):
        """Return connection to pool"""
        if self.db_pool and conn:
            self.db_pool.putconn(conn)

    # =================================================================
    # FORECAST MECHANISM - Read from forecast_1h table
    # =================================================================
    
    def _get_latest_forecast(self):
        """
        Get latest forecast from forecast_1h table
        Returns: predicted throughput (bps) for DPID 5
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            cur = conn.cursor()
            
            # Get latest forecast for DPID 5 (Leaf 2 - where H2 is connected)
            cur.execute("""
                SELECT y_pred, ts_created
                FROM forecast_1h
                ORDER BY ts_created DESC
                LIMIT 1
            """)
            
            result = cur.fetchone()
            cur.close()
            
            if result:
                y_pred_kbps = result[0]  # Assuming y_pred is in Kbps
                ts_created = result[1]
                
            
                
                
                return {
                    'predicted_bps': y_pred_kbps,
                    'timestamp': ts_created,
                    'age_seconds': (datetime.now(ts_created.tzinfo) - ts_created).total_seconds()
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Forecast query error: {e}")
            return None
        finally:
            self.return_db_connection(conn)
    

    def _forecast_monitor(self):
        """
        Monitor forecast and trigger proactive rerouting
        """
        while True:
            hub.sleep(FORECAST_CHECK_INTERVAL)
            
            try:
                with self.lock:
                    # Skip if not in stable state
                    if self.reroute_stage not in ['IDLE', 'ACTIVE_REROUTE']:
                        continue
                    
                    forecast = self._get_latest_forecast()
                    
                    if not forecast:
                        continue
                    
                    predicted_bps = forecast['predicted_bps']
                    forecast_age = forecast['age_seconds']
                    
                    # Only use recent forecasts (< 60 seconds old)
                    if forecast_age > 60:
                        self.logger.debug(f"⏰ Forecast too old ({forecast_age:.0f}s), skipping")
                        continue
                    
                    self.last_forecast_value = predicted_bps
                    self.last_forecast_time = forecast['timestamp']
                    
                    # Log forecast value
                    if int(time.time()) % 10 == 0:  # Every 10 seconds
                        self.logger.info(f"📊 Forecast: {predicted_bps:.0f} bps (age: {forecast_age:.1f}s)")
                    
                    # === TRIGGER PROACTIVE REROUTE ===
                    if ( self.reroute_stage == 'IDLE' and
                        predicted_bps > FORECAST_THRESHOLD_BPS):
                        
                        self.logger.warning(f"🔮 FORECAST ALERT: Predicted {predicted_bps:.0f} bps > {FORECAST_THRESHOLD_BPS} bps")
                        self.logger.info(f"🚀 PROACTIVE REROUTE: Moving H1->H2 from Spine {self.current_spine}")
                        
                        # Select alternative spine
                        target_spine = self._get_alternative_spine(self.current_spine)
                        
                        self.stage_start_time = time.time()
                        self.last_reroute_time = time.time()
                        self.stats['forecast_reroute'] += 1
                        
                        success = self._atomic_reroute_to_spine(target_spine)
                        
                        if not success:
                            self.logger.error("❌ Proactive reroute failed")
                            self.reroute_stage = 'IDLE'
                    
                    # === TRIGGER REVERT ===
                    # === TRIGGER REVERT (DENGAN CEK SPINE 2) ===
                    elif (self.congestion_active and 
                          self.reroute_stage == 'ACTIVE_REROUTE' and
                          predicted_bps < REVERT_THRESHOLD_BPS):
                        
                        self.stability_counter += 1
                        self.logger.debug(f"✓ Stability check {self.stability_counter}/{STABILITY_CYCLES_REQUIRED} (forecast: {predicted_bps:.0f} bps)")
                        
                        if self.stability_counter >= STABILITY_CYCLES_REQUIRED:
                    
                          
                            self.logger.info(f"✅ Forecast & Spine 2 ({spine2_load:.0f} bps) stable. Reverting...")
                                
                            self.stats['forecast_revert'] += 1
                            success = self._atomic_revert_to_original_spine()
                                
                            if success:
                                 self.stability_counter = 0
                            else:
                                 self.logger.error("❌ Revert failed")

                              
                        else:
                            self.logger.warning(f"⚠️ [REVERT BLOCKED] ")
                            self.stability_counter = 0 # Reset counter, tunggu lagi
                    else:
                        # Reset stability counter if forecast goes back up
                        if self.congestion_active and predicted_bps >= REVERT_THRESHOLD_BPS:
                            self.stability_counter = 0
                            
            except Exception as e:
                self.logger.error(f"❌ Forecast monitor error: {e}")

    # =================================================================
    # COMPLETE FLOW DELETION - Prevents Counter Accumulation
    # =================================================================
    # =========================================================
    # NEW FUNCTION: CEK BEBAN SPINE 2 (MAIN PATH)
    # =========================================================
    def get_spine2_load(self):
        """
        Mengambil throughput aktual Spine 2 (bits/sec) dari Database.
        Digunakan untuk memastikan jalan utama kosong sebelum Revert.
        """
        try:
            # Query user (optimized with LIMIT 1)
            query = """
                WITH x AS (
                    SELECT
                        date_trunc('second', timestamp) AS detik,
                        dpid,
                        sum(bytes_tx) AS total_bytes
                    FROM traffic.flow_stats_
                    WHERE timestamp >= NOW() - INTERVAL '10 seconds'
                    GROUP BY detik, dpid
                )
                SELECT
                    detik AS ts,
                    MAX(CASE WHEN dpid = 2 THEN total_bytes * 8 END) AS thp_2
                FROM x
                GROUP BY detik
                ORDER BY ts DESC
                LIMIT 1;
            """
            
            # Gunakan cursor baru biar thread-safe
            with self.db_conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                
                if row and row[1] is not None:
                    load = float(row[1])
                    # self.logger.info(f"[SPINE-2 CHECK] Load: {load} bps") # Uncomment kalau mau debug
                    return load
                    
        except Exception as e:
            self.logger.error(f"[SPINE-2 CHECK] Error fetching DB: {e}")
        
        # FAIL-SAFE: Kalau error/null, anggap MACET (999999) biar GAK REVERT.
        return None  
    
    def _get_alternative_spine(self, avoid_spine): 
        """Get alternative spine (avoiding current one)"""
        available = [1, 2, 3]
        available.remove(avoid_spine)
        # Choose spine with lowest traffic
        return min(available, key=lambda s: self.spine_traffic.get(s, 0))
    
    def _delete_all_h1_h2_flows(self):
        """Delete ALL H1->H2 flows from ALL switches (for complete cleanup)"""
        self.logger.info("🗑️ DELETING ALL H1->H2 flows from ALL switches...")
        
        switches_to_clean = [1, 2, 3, 4, 5]
        deleted_count = 0
        
        for dpid in switches_to_clean:
            if dpid not in self.datapaths:
                continue
            
            dp = self.datapaths[dpid]
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            
            match = parser.OFPMatch(
                eth_type=0x0800,
                ip_proto=17,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2',
                udp_dst=9000
            )
            
            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=match
            )
            
            dp.send_msg(mod)
            deleted_count += 1
            self.logger.info(f"  ✓ DPID {dpid}: Deleted H1->H2 flows")
        
        # Reset counters
        keys_to_reset = []
        for key in list(self.last_bytes.keys()):
            dpid, src_ip, dst_ip, udp_dst = key
            if src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                keys_to_reset.append(key)
        
        for key in keys_to_reset:
            self.last_bytes.pop(key, None)
            self.last_bytes_timestamp.pop(key, None)
            self.last_packets.pop(key, None)
        
        self.logger.info(f"  ✓ Reset {len(keys_to_reset)} traffic counters")
        self.logger.info(f"🗑️ Complete: Deleted flows from {deleted_count} switches")

    def _delete_h1_h2_flows_on_spine(self, spine_dpid):
        """Delete H1->H2 flows on SPECIFIC spine AND Leaf 1"""
        # Delete from spine
        if spine_dpid in self.datapaths:
            dp = self.datapaths[spine_dpid]
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            
            match = parser.OFPMatch(
                eth_type=0x0800,
                ip_proto=17,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2',
                udp_dst=9000
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
        
        # CRITICAL: Also delete LOW PRIORITY flows from Leaf 1
        if 4 in self.datapaths:
            dp = self.datapaths[4]
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            
            match = parser.OFPMatch(
                eth_type=0x0800,
                ip_proto=17,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2',
                udp_dst=9000
            )
            
            # Delete with STRICT priority to remove only low-priority flows
            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofproto.OFPFC_DELETE_STRICT,
                priority=PRIORITY_USER,  # Only delete priority 10
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=match
            )
            
            dp.send_msg(mod)
            self.logger.info(f"🗑️ Deleted low-priority H1->H2 flows on Leaf 1")
    
    def _install_h1_h2_flow_on_spine(self, spine_dpid):
        """Install H1->H2 flow on specified spine"""
        if spine_dpid not in self.datapaths:
            self.logger.warning(f"⚠️ Spine {spine_dpid} not available")
            return False
        
        dp = self.datapaths[spine_dpid]
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        
        # Output port to Leaf 2 (where H2 is)
        out_port = 2
        
        match = parser.OFPMatch(
            eth_type=0x0800,
            ip_proto=17,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',   
            udp_dst=9000
        )
        
        actions = [
            parser.OFPActionSetQueue(1),   # queue 1 = VoIP high priority
            parser.OFPActionOutput(out_port)
        ]

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
        self.logger.info(f"✅ Spine {spine_dpid}: Installed H1->H2 flow (port {out_port})")
        return True
    
    
    def _update_leaf1_output_port(self, target_spine):
        """Update Leaf 1 to forward H1->H2 to target spine"""
        if 4 not in self.datapaths:
            self.logger.warning("⚠️ Leaf 1 (DPID 4) not available")
            return False
        
        dp = self.datapaths[4]
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        
        # Port mapping: 1->Spine1, 2->Spine2, 3->Spine3
        spine_to_port = {1: 1, 2: 2, 3: 3}
        out_port = spine_to_port.get(target_spine, 2)
        
        match = parser.OFPMatch(
            eth_type=0x0800,
            ip_proto=17,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            udp_dst=9000
        )
        
        actions = [
            parser.OFPActionSetQueue(1),   # queue 1 = VoIP high priority
            parser.OFPActionOutput(out_port)
        ]

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
        self.logger.info(f"✅ Leaf 1: Routing H1->H2 via Spine {target_spine} (port {out_port})")
        return True

    def _atomic_reroute_to_spine(self, target_spine):
        """
        MAKE-BEFORE-BREAK REROUTE: Zero packet loss
        
        Steps:
        1. Install NEW flows on target spine
        2. Update Leaf 1 routing
        3. Brief overlap (0.5s)
        4. Delete OLD flows on old spine
        """
        old_spine = self.current_spine
        
        self.logger.info(f"🔄 MAKE-BEFORE-BREAK REROUTE: Spine {old_spine} → Spine {target_spine}")
        
        # STEP 1: Install NEW path first
        self.reroute_stage = 'INSTALLING_NEW_PATH'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': True,
            'target_spine': target_spine
        })
        
        success = self._install_h1_h2_flow_on_spine(target_spine)
        if not success:
            self.logger.error("❌ Failed to install on new spine")
            self.reroute_stage = 'IDLE'
            return False
        
        success = self._update_leaf1_output_port(target_spine)
        if not success:
            self.logger.error("❌ Failed to update Leaf 1")
            self.reroute_stage = 'IDLE'
            return False

        hub.sleep(0.1)
        self.logger.info("✅ New path installed")
        
        # STEP 2: Brief overlap to allow traffic to switch
        self.reroute_stage = 'TRAFFIC_SWITCHING'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': True,
            'old_spine': old_spine,
            'new_spine': target_spine
        })
        
        self.logger.info("⏳ Waiting 0.5s for traffic to switch...")
        hub.sleep(0.05)
        
        # STEP 3: Delete OLD flows
        self.reroute_stage = 'DELETING_OLD_FLOWS'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': True,
            'old_spine': old_spine
        })
        
        self._delete_h1_h2_flows_on_spine(old_spine)
        
        self.logger.info("⏳ Waiting 0.3s for deletion to propagate...")
        hub.sleep(0.05)
        
        # STEP 4: Complete
        self.current_spine = target_spine
        self.original_spine = old_spine
        self.congestion_active = True
        self.reroute_stage = 'ACTIVE_REROUTE'
        self.stats['total_reroutes'] += 1
        
        write_state_file({
            'state': 'ACTIVE_REROUTE',
            'congestion': True,
            'current_spine': self.current_spine,
            'original_spine': self.original_spine
        })
        
        self.logger.info(f"✅ REROUTE COMPLETE: H1->H2 now via Spine {target_spine} (make-before-break)")
        return True

    def _atomic_revert_to_original_spine(self):
        """
        MAKE-BEFORE-BREAK REVERT: Return to original spine with zero loss
        """
        if not self.congestion_active or not self.original_spine:
            self.logger.warning("⚠️ Cannot revert: not in rerouted state")
            return False
        
        target_spine = self.original_spine
        old_spine = self.current_spine
        
        self.logger.info(f"🔙 MAKE-BEFORE-BREAK REVERT: Spine {old_spine} → Spine {target_spine}")
        
        # STEP 1: Install on original spine
        self.reroute_stage = 'REVERT_INSTALLING'
        write_state_file({
            'state': self.reroute_stage,
            'congestion': False,
            'target_spine': target_spine
        })
        
        self._install_h1_h2_flow_on_spine(target_spine)
        self._update_leaf1_output_port(target_spine)
        
        self.logger.info("✅ Original path installed")
        
        # STEP 2: Brief overlap
        hub.sleep(0.4)
        
        # STEP 3: Delete from current spine
        self.reroute_stage = 'REVERT_DELETING'
        
        self._delete_h1_h2_flows_on_spine(old_spine)
        
        hub.sleep(0.3)
        
        # STEP 4: Complete
        self.current_spine = target_spine
        self.original_spine = target_spine
        self.congestion_active = False
        self.reroute_stage = 'IDLE'
        self.stats['total_reverts'] += 1
        
        write_state_file({
            'state': 'IDLE',
            'congestion': False,
            'current_spine': self.current_spine
        })
        
        self.logger.info(f"✅ REVERT COMPLETE: H1->H2 back to Spine {target_spine} (make-before-break)")
        return True

    # =================================================================
    # TOPOLOGY & INITIALIZATION
    # =================================================================
    
    def _discover_topology(self):
        """Discover network topology periodically"""
        while True:
            hub.sleep(5)
            try:
                switches = get_switch(self)
                links = get_link(self)
                
                self.net.clear()
                for switch in switches:
                    self.net.add_node(switch.dp.id)
                
                for link in links:
                    self.net.add_edge(link.src.dpid, link.dst.dpid, port=link.src.port_no)
                    self.net.add_edge(link.dst.dpid, link.src.dpid, port=link.dst.port_no)
            except:
                pass

    def _install_default_flows(self):
        """
        Install default flows at startup
        BOTH H1 and H3 start via Spine 2
        """
        self.logger.info("🔧 Installing DEFAULT flows...")
        
        # Wait for datapaths
        max_wait = 12
        waited = 0
        while waited < max_wait:
            if all(dpid in self.datapaths for dpid in [2, 4, 5, 6]):
                break
            hub.sleep(1)
            waited += 1
        
        if not all(dpid in self.datapaths for dpid in [2, 4, 6]):
            self.logger.error("❌ Not all datapaths ready")
            return
        
        # === H1->H2: Via Spine 2 (default, can be rerouted) ===
        self._install_h1_h2_flow_on_spine(2)
        self._update_leaf1_output_port(2)
        self.logger.info("✅ H1->H2 path: Leaf1 → Spine2 → Leaf2")
        
        # === H3->H2: PERMANENTLY via Spine 2 (NEVER reroute) ===
        self._install_h3_h2_permanent_flow()
        self.logger.info("✅ H3->H2 path: Leaf3 → Spine2 → Leaf2 (PERMANENT)")
        
        self.default_flows_installed = True
        self.logger.info("🟢 Default paths established (both via Spine 2)")

    def _install_h3_h2_permanent_flow(self):
        """
        Install H3->H2 flow PERMANENTLY on Spine 2 and Leaf 3
        This flow is NEVER deleted or rerouted
        """
        # Leaf 3 (DPID 6): H3->H2 → Spine 2
        if 6 in self.datapaths:
            dp = self.datapaths[6]
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.3',
                ipv4_dst='10.0.0.2',
                ip_proto=17,
                udp_dst=9001

            )
            
            actions = [
                parser.OFPActionSetQueue(2),
                parser.OFPActionOutput(2)
            ]

            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
            
            mod = parser.OFPFlowMod(
                datapath=dp,
                priority=PRIORITY_REROUTE + 100,  # Higher priority to prevent override
                match=match,
                instructions=inst,
                idle_timeout=0,
                hard_timeout=0
            )
            
            dp.send_msg(mod)
            self.logger.info("✅ Leaf 3: H3->H2 → Spine 2 (port 2) [PERMANENT]")
        
        # Spine 2: H3->H2 → Leaf 2
        if 2 in self.datapaths:
            dp = self.datapaths[2]
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.3',
                ipv4_dst='10.0.0.2',
                ip_proto=17,
                udp_dst=9001

            )
            
            actions = [
                parser.OFPActionSetQueue(2),
                parser.OFPActionOutput(2)
            ]

            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
            
            mod = parser.OFPFlowMod(
                datapath=dp,
                priority=PRIORITY_REROUTE + 100,  # Higher priority
                match=match,
                instructions=inst,
                idle_timeout=0,
                hard_timeout=0
            )
            
            dp.send_msg(mod)
            self.logger.info("✅ Spine 2: H3->H2 → Leaf 2 (port 2) [PERMANENT]")

    # =================================================================
    # TRAFFIC MONITORING
    # =================================================================
    
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """
        Process flow stats with proper delta calculation
        CRITICAL: Deltas prevent counter accumulation
        """
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        current_time = time.time()
        
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            for stat in body:
                match = stat.match
                
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                udp_dst = match.get('udp_dst')
                
                if not src_ip or not dst_ip:
                    continue
                
                flow_key = (dpid, src_ip, dst_ip, udp_dst)
                
                # Current values
                current_bytes = stat.byte_count
                current_packets = stat.packet_count
                
                # Get last values (initialize to current on first observation)
                last_bytes = self.last_bytes.get(flow_key, current_bytes)
                last_packets = self.last_packets.get(flow_key, current_packets)
                last_ts = self.last_bytes_timestamp.get(flow_key, current_time)
                
                # Calculate deltas (PREVENTS ACCUMULATION)
                delta_bytes = max(0, current_bytes - last_bytes)
                delta_packets = max(0, current_packets - last_packets)
                time_diff = max(0.1, current_time - last_ts)
                
                # Update tracking
                self.last_bytes[flow_key] = current_bytes
                self.last_packets[flow_key] = current_packets
                self.last_bytes_timestamp[flow_key] = current_time
                
                # Calculate bps
                bps = (delta_bytes * 8) / time_diff
                
                # Update spine traffic for H1->H2
                if src_ip == '10.0.0.1' and dst_ip == '10.0.0.2' and udp_dst == 9000 and dpid in [1,2,3]:
                    self.spine_traffic[dpid] = bps
                
                # Resolve MACs
                src_mac = self.ip_to_mac.get(src_ip)
                dst_mac = self.ip_to_mac.get(dst_ip)
                
                # Insert to DB
                # H1->H2: Always insert (for monitoring)
                if src_ip == '10.0.0.1' and dst_ip == '10.0.0.2' and udp_dst == 9000:
                    self._insert_flow_stats(
                        dpid, src_ip, dst_ip, match,
                        delta_bytes, delta_packets,
                        src_mac, dst_mac, time_diff
                    )
                # Other traffic: Only if there's actual traffic
                elif delta_bytes > 0:
                    self._insert_flow_stats(
                        dpid, src_ip, dst_ip, match,
                        delta_bytes, delta_packets,
                        src_mac, dst_mac, time_diff
                    )
        
        except Exception as e:
            self.logger.error(f"❌ Flow stats error: {e}")
        finally:
            self.return_db_connection(conn)

    def _monitor_traffic(self):
        """Request flow stats periodically"""
        while True:
            hub.sleep(1)
            for dp in self.datapaths.values():
                self._request_stats(dp)

    def _request_stats(self, datapath):
        """Request flow statistics"""
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    def _insert_flow_stats(self, dpid, src_ip, dst_ip, match, delta_bytes, delta_packets, 
                           src_mac, dst_mac, duration):
        """Insert flow stats to database"""
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
                src_mac, dst_mac,
                match.get('ip_proto', 17),
                match.get('tcp_src') or match.get('udp_src') or 0,
                match.get('tcp_dst') or match.get('udp_dst') or 1,
                delta_bytes, delta_bytes,
                delta_packets, delta_packets,
                duration,
                'voip' if src_ip == '10.0.0.1' else 'bursty'
            ))
            conn.commit()
            cur.close()
        except Exception as e:
            self.logger.error(f"❌ DB insert error: {e}")
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
                self.logger.info(f"🔌 Switch connected: DPID {datapath.id}")
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]
                self.logger.warning(f"🔌 Switch disconnected: DPID {datapath.id}")
    
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Handle switch connection"""
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Install table-miss flow
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
    
    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        """Add flow to switch"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        if buffer_id:
            mod = parser.OFPFlowMod(
                datapath=datapath, buffer_id=buffer_id, priority=priority, 
                match=match, instructions=inst, idle_timeout=idle_timeout
            )
        else:
            mod = parser.OFPFlowMod(
                datapath=datapath, priority=priority, match=match, 
                instructions=inst, idle_timeout=idle_timeout
            )
        
        datapath.send_msg(mod)
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Handle packet-in events"""
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id
        
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        # Ignore LLDP and IPv6
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        if eth.ethertype == ether_types.ETH_TYPE_IPV6:
            return
        
        dst = eth.dst
        src = eth.src
        
        # MAC learning
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port
        
        # Extract protocols
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt:
            self.ip_to_mac[ip_pkt.src] = src
        
        udp_pkt = pkt.get_protocol(udp.udp)
        udp_dst = udp_pkt.dst_port if udp_pkt else None
        
        # === SPECIAL HANDLING FOR H1->H2 (Leaf 1 - DPID 4) ===
        if ip_pkt and dpid == 4:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            if src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                # CRITICAL: Skip flow installation during reroute to prevent duplicate flows
                if self.reroute_stage in ['INSTALLING_NEW_PATH', 'TRAFFIC_SWITCHING', 'DELETING_OLD_FLOWS',
                                        'REVERT_INSTALLING', 'REVERT_DELETING']:
                    # Just forward packet, don't install flow
                    spine_to_port = {1: 1, 2: 2, 3: 3}
                    out_port = spine_to_port.get(self.current_spine, 2)
                    actions = [
                        parser.OFPActionSetQueue(1),   # queue 1 = VoIP high priority
                        parser.OFPActionOutput(out_port)
                    ]

                    data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                    out = parser.OFPPacketOut(
                        datapath=datapath, buffer_id=msg.buffer_id,
                        in_port=in_port, actions=actions, data=data
                    )
                    datapath.send_msg(out)
                    return
                
                # Normal: Route via current_spine and install flow
                spine_to_port = {1: 1, 2: 2, 3: 3}
                out_port = spine_to_port.get(self.current_spine, 2)
                
                actions = [
                    parser.OFPActionSetQueue(1),   # queue 1 = VoIP high priority
                    parser.OFPActionOutput(out_port)
                ]
                   
                if udp_dst == 9000:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ip_proto=17,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        udp_dst=9000
                    )
                else:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip
                    )


                
                self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data
                
                out = parser.OFPPacketOut(
                    datapath=datapath, buffer_id=msg.buffer_id,
                    in_port=in_port, actions=actions, data=data
                )
                datapath.send_msg(out)
                return
        
        # === SPECIAL HANDLING FOR H3->H2 (Leaf 3 - DPID 6) ===
        if ip_pkt and dpid == 6:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            if src_ip == '10.0.0.3' and dst_ip == '10.0.0.2':
                # ALWAYS port 2 (Spine 2)
                actions = [
                    parser.OFPActionSetQueue(2),
                    parser.OFPActionOutput(2)
                ]

                if udp_dst == 9001:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ip_proto=17,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        udp_dst=9001
                    )
                else:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip
                    )
  
                self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data
                
                out = parser.OFPPacketOut(
                    datapath=datapath, buffer_id=msg.buffer_id,
                    in_port=in_port, actions=actions, data=data
                )
                datapath.send_msg(out)
                return
        
        # === SPECIAL HANDLING FOR Leaf 2 (DPID 5) - DESTINATION ===
        if ip_pkt and dpid == 5:
            udp_dst = udp_pkt.dst_port if udp_pkt else None
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            if dst_ip == '10.0.0.2':
                if dst in self.mac_to_port[dpid]:
                    out_port = self.mac_to_port[dpid][dst]
                else:
                    out_port = ofproto.OFPP_FLOOD
                
                if out_port != ofproto.OFPP_FLOOD:
                    queue_id = 1 if udp_dst == 9000 else 2

                    actions = [
                        parser.OFPActionSetQueue(queue_id),
                        parser.OFPActionOutput(out_port)
                    ]

                if udp_dst == 9000 or udp_dst == 9001:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ip_proto=17,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        udp_dst=udp_dst
                    )
                    
                    self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)

                # Forward packet (ICMP + UDP semua)
                data = None
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data

                out = parser.OFPPacketOut(
                    datapath=datapath, buffer_id=msg.buffer_id,
                    in_port=in_port, actions=actions, data=data
                )
                datapath.send_msg(out)
                return
        
        # === ARP HANDLING ===
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.ip_to_mac:
                target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                
                e = ethernet.ethernet(
                    dst=src, src=target_mac, ethertype=ether_types.ETH_TYPE_ARP
                )
                a = arp.arp(
                    opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=arp_pkt.dst_ip, 
                    dst_mac=src, dst_ip=arp_pkt.src_ip
                )
                
                p = packet.Packet()
                p.add_protocol(e)
                p.add_protocol(a)
                p.serialize()
                
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(
                    datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, 
                    in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=p.data
                )
                datapath.send_msg(out)
                return
        
        # === STANDARD SWITCHING LOGIC ===
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD
        
        actions = []
        is_leaf = dpid >= 4
        is_spine = dpid <= 3
        
        if out_port == ofproto.OFPP_FLOOD:
            if is_leaf:
                # Flood to host ports and uplinks
                if in_port <= 3:  # From spine
                    actions.append(parser.OFPActionOutput(4))  # To host
                    actions.append(parser.OFPActionOutput(5))  # To host (if exists)
                else:  # From host
                    actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
            elif is_spine:
                actions.append(parser.OFPActionOutput(ofproto.OFPP_FLOOD))
        else:
            actions = [
                parser.OFPActionSetQueue(1),   # queue 1 = VoIP high priority
                parser.OFPActionOutput(out_port)
            ]

        
        # Install flow if not flooding
        if out_port != ofproto.OFPP_FLOOD:
            
            # --- [BARIS BARU] ---
            # Bikin match default dulu (berdasarkan MAC Address & Port)
            # Biar paket ARP/Non-IP tetap punya variabel 'match'
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            # --------------------

            # Baru cek detail IP (kalau ada)
            if ip_pkt and udp_pkt:
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=0x0800,
                    ip_proto=17,
                    ipv4_src=ip_pkt.src,
                    ipv4_dst=ip_pkt.dst,
                    udp_dst=udp_pkt.dst_port
                )
            elif ip_pkt:
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=0x0800,
                    ipv4_src=ip_pkt.src,
                    ipv4_dst=ip_pkt.dst
                )

            # Sekarang aman, 'match' pasti sudah terisi (minimal yang L2 tadi)
            self.add_flow(datapath, PRIORITY_DEFAULT, match, actions, msg.buffer_id, idle_timeout=60)
        
        # Send packet out
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        
        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, 
            in_port=in_port, actions=actions, data=data
        )
        datapath.send_msg(out)