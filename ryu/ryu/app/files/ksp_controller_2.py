#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP QoS with FORECAST-BASED Rerouting
=============================================================================
FORECAST MODE: Read y_pred from forecast_1h table to predict congestion
PROACTIVE: Reroute H1->H2 BEFORE burst happens
CLEAN DELETION: Remove ALL flows to prevent counter accumulation
H3 ISOLATION: H3 always uses Spine 2, never rerouted

UPDATED: Support iperf3 for TCP traffic on ports 9001 (burst) and 9003 (steady)
         Port 9000 remains UDP for VoIP using D-ITG
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
FORECAST_THRESHOLD_BPS = 40000000     # 40 Mbps threshold untuk reroute
FORECAST_LEAD_TIME_SEC = 10         # Reroute 10 detik sebelum predicted congestion
REVERT_THRESHOLD_BPS = 250000000    # Revert jika forecast < 250 Mbps

# Stability
STABILITY_CYCLES_REQUIRED = 8      # Butuh 8 cycle stabil sebelum revert
REVERT_COOLDOWN_SEC = 60

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
        self.last_revert_time = 0
        
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
        
        self.logger.info("🟢 VoIP Forecast Controller Started (iperf3 compatible)")
        self.logger.info("📊 Forecast source: forecast_1h.y_pred (DPID 5)")
        self.logger.info("🔌 Port 9000: UDP VoIP (D-ITG)")
        self.logger.info("🔌 Port 9001: TCP Burst (iperf3)")
        self.logger.info("🔌 Port 9003: TCP Steady (iperf3)")
        
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
                
                # Convert to bps
                y_pred_bps = y_pred_kbps * 1000
                
                self.last_forecast_value = y_pred_bps
                self.last_forecast_time = ts_created
                
                return y_pred_bps
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Forecast query error: {e}")
            return None
        finally:
            self.return_db_connection(conn)
    
    def _forecast_monitor(self):
        """
        Main forecast monitoring loop
        Checks forecast and triggers reroute if needed
        """
        self.logger.info("📊 Forecast monitor thread started")
        
        while True:
            try:
                hub.sleep(FORECAST_CHECK_INTERVAL)
                
                # Skip if already rerouting
                if self.reroute_stage != 'IDLE':
                    continue
                
                forecast_bps = self._get_latest_forecast()
                
                if forecast_bps is None:
                    continue
                
                # Check for congestion prediction
                if not self.congestion_active:
                    if forecast_bps > FORECAST_THRESHOLD_BPS:
                        time_since_last = time.time() - self.last_reroute_time
                        
                        if time_since_last > 30:  # Cooldown 30 seconds
                            self.logger.warning(
                                f"🔮 FORECAST CONGESTION: {forecast_bps/1e6:.1f} Mbps > "
                                f"{FORECAST_THRESHOLD_BPS/1e6:.1f} Mbps threshold"
                            )
                            self.logger.info(f"⚡ Triggering PROACTIVE reroute")
                            
                            self.stats['forecast_reroute'] += 1
                            hub.spawn(self._execute_reroute_sequence)
                
                # Check for revert condition
                elif self.congestion_active:
                    if forecast_bps < REVERT_THRESHOLD_BPS:
                        self.stability_counter += 1
                        self.logger.info(
                            f"📉 Stability: {self.stability_counter}/{STABILITY_CYCLES_REQUIRED} "
                            f"({forecast_bps/1e6:.1f} Mbps < {REVERT_THRESHOLD_BPS/1e6:.1f} Mbps)"
                        )
                        
                        if self.stability_counter >= STABILITY_CYCLES_REQUIRED:
                            time_since_revert = time.time() - self.last_revert_time
                            
                            if time_since_revert > REVERT_COOLDOWN_SEC:
                                self.logger.info(f"✅ FORECAST REVERT: Traffic stable")
                                self.stats['forecast_revert'] += 1
                                hub.spawn(self._execute_revert_sequence)
                            else:
                                wait_more = REVERT_COOLDOWN_SEC - time_since_revert
                                self.logger.info(f"⏳ Revert cooldown: {wait_more:.0f}s remaining")
                                self.stability_counter = 0
                    else:
                        # Reset stability if forecast goes up again
                        if self.stability_counter > 0:
                            self.logger.info(
                                f"⚠️ Stability reset: {forecast_bps/1e6:.1f} Mbps >= threshold"
                            )
                        self.stability_counter = 0
                
            except Exception as e:
                self.logger.error(f"❌ Forecast monitor error: {e}")
                hub.sleep(5)

    # =================================================================
    # REROUTE LOGIC
    # =================================================================
    
    def _execute_reroute_sequence(self):
        """Execute complete reroute sequence with state machine"""
        with self.lock:
            if self.reroute_stage != 'IDLE':
                self.logger.warning("⚠️ Already rerouting, skipping")
                return
            
            self.logger.info("="*80)
            self.logger.info("🔄 STARTING REROUTE SEQUENCE")
            self.logger.info("="*80)
            
            # STAGE 1: Delete all flows
            self.reroute_stage = 'DELETING_ALL_FLOWS'
            write_state_file({
                'state': self.reroute_stage,
                'congestion': True,
                'current_spine': self.current_spine
            })
            
            self.logger.info("1️⃣ STAGE: DELETING_ALL_FLOWS")
            self._delete_all_user_flows()
            
            # STAGE 2: Wait for deletion
            self.reroute_stage = 'WAITING_SETTLE'
            write_state_file({
                'state': self.reroute_stage,
                'congestion': True,
                'current_spine': self.current_spine
            })
            
            self.logger.info(f"2️⃣ STAGE: WAITING_SETTLE ({FLOW_DELETE_WAIT_SEC}s)")
            hub.sleep(FLOW_DELETE_WAIT_SEC)
            
            # Reset counters BEFORE installing new path
            self.logger.info("🔄 Resetting traffic counters")
            self.last_bytes = {}
            self.last_bytes_timestamp = {}
            self.last_packets = {}
            self.spine_traffic = {1: 0, 2: 0, 3: 0}
            
            # STAGE 3: Install new path
            self.reroute_stage = 'INSTALLING_NEW_PATH'
            target_spine = 1 if self.current_spine == 2 else 2
            
            write_state_file({
                'state': self.reroute_stage,
                'congestion': True,
                'current_spine': target_spine
            })
            
            self.logger.info(f"3️⃣ STAGE: INSTALLING_NEW_PATH (Spine {target_spine})")
            self._install_h1_path(target_spine)
            
            # Update state
            self.congestion_active = True
            self.current_spine = target_spine
            self.last_reroute_time = time.time()
            self.stats['total_reroutes'] += 1
            
            # STAGE 4: Wait for traffic to settle
            self.logger.info(f"⏳ Waiting {TRAFFIC_SETTLE_WAIT_SEC}s for traffic to settle")
            hub.sleep(TRAFFIC_SETTLE_WAIT_SEC)
            
            # STAGE 5: Done
            self.reroute_stage = 'ACTIVE_REROUTE'
            write_state_file({
                'state': self.reroute_stage,
                'congestion': True,
                'current_spine': self.current_spine
            })
            
            self.logger.info("="*80)
            self.logger.info(f"✅ REROUTE COMPLETE: H1→H2 now via Spine {self.current_spine}")
            self.logger.info(f"📊 Total reroutes: {self.stats['total_reroutes']}")
            self.logger.info("="*80)
            
            # Return to IDLE after short delay
            hub.sleep(2)
            self.reroute_stage = 'IDLE'
    
    def _execute_revert_sequence(self):
        """Execute complete revert sequence"""
        with self.lock:
            if self.reroute_stage != 'IDLE':
                self.logger.warning("⚠️ Already processing, skipping revert")
                return
            
            self.logger.info("="*80)
            self.logger.info("🔙 STARTING REVERT SEQUENCE")
            self.logger.info("="*80)
            
            # STAGE 1: Delete flows
            self.reroute_stage = 'REVERT_DELETING'
            write_state_file({
                'state': self.reroute_stage,
                'congestion': False,
                'current_spine': self.current_spine
            })
            
            self.logger.info("1️⃣ STAGE: REVERT_DELETING")
            self._delete_all_user_flows()
            
            # STAGE 2: Wait
            self.reroute_stage = 'REVERT_SETTLE'
            self.logger.info(f"2️⃣ STAGE: REVERT_SETTLE ({FLOW_DELETE_WAIT_SEC}s)")
            hub.sleep(FLOW_DELETE_WAIT_SEC)
            
            # Reset counters
            self.logger.info("🔄 Resetting traffic counters")
            self.last_bytes = {}
            self.last_bytes_timestamp = {}
            self.last_packets = {}
            self.spine_traffic = {1: 0, 2: 0, 3: 0}
            
            # STAGE 3: Install original path
            self.reroute_stage = 'REVERT_INSTALLING'
            write_state_file({
                'state': self.reroute_stage,
                'congestion': False,
                'current_spine': self.original_spine
            })
            
            self.logger.info(f"3️⃣ STAGE: REVERT_INSTALLING (Spine {self.original_spine})")
            self._install_h1_path(self.original_spine)
            
            # Update state
            self.congestion_active = False
            self.current_spine = self.original_spine
            self.last_revert_time = time.time()
            self.stability_counter = 0
            self.stats['total_reverts'] += 1
            
            # Wait for settle
            self.logger.info(f"⏳ Waiting {TRAFFIC_SETTLE_WAIT_SEC}s for traffic to settle")
            hub.sleep(TRAFFIC_SETTLE_WAIT_SEC)
            
            # Done
            self.reroute_stage = 'IDLE'
            write_state_file({
                'state': self.reroute_stage,
                'congestion': False,
                'current_spine': self.current_spine
            })
            
            self.logger.info("="*80)
            self.logger.info(f"✅ REVERT COMPLETE: H1→H2 back to Spine {self.current_spine}")
            self.logger.info(f"📊 Total reverts: {self.stats['total_reverts']}")
            self.logger.info("="*80)

    def _delete_all_user_flows(self):
        """Delete ALL user flows (PRIORITY_USER and PRIORITY_REROUTE)"""
        for dpid in [4, 5]:  # Leaf 1 and Leaf 2
            if dpid not in self.datapaths:
                continue
            
            datapath = self.datapaths[dpid]
            parser = datapath.ofproto_parser
            ofproto = datapath.ofproto
            
            # Delete PRIORITY_USER flows
            match = parser.OFPMatch()
            mod = parser.OFPFlowMod(
                datapath=datapath,
                command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                priority=PRIORITY_USER,
                match=match
            )
            datapath.send_msg(mod)
            
            # Delete PRIORITY_REROUTE flows
            mod = parser.OFPFlowMod(
                datapath=datapath,
                command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                priority=PRIORITY_REROUTE,
                cookie=COOKIE_REROUTE,
                cookie_mask=0xFFFFFFFFFFFFFFFF,
                match=match
            )
            datapath.send_msg(mod)
            
            self.logger.info(f"🗑️ Deleted flows on DPID {dpid}")

    def _install_h1_path(self, target_spine):
        """
        Install H1->H2 path via specified spine
        Supports both UDP (port 9000) and TCP (ports 9001, 9003)
        """
        if 4 not in self.datapaths or 5 not in self.datapaths:
            self.logger.error("❌ Datapaths not ready")
            return
        
        dp_leaf1 = self.datapaths[4]
        dp_leaf2 = self.datapaths[5]
        
        parser4 = dp_leaf1.ofproto_parser
        parser5 = dp_leaf2.ofproto_parser
        
        # Leaf 1: H1 -> Spine
        out_port = target_spine  # Port 1, 2, or 3 untuk Spine 1, 2, 3
        
        # UDP VoIP (port 9000) - High priority queue
        match_udp = parser4.OFPMatch(
            eth_type=0x0800,
            ip_proto=17,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            udp_dst=9000
        )
        actions_udp = [
            parser4.OFPActionSetQueue(1),  # VoIP queue
            parser4.OFPActionOutput(out_port)
        ]
        self.add_flow(dp_leaf1, PRIORITY_REROUTE, match_udp, actions_udp, 
                     cookie=COOKIE_REROUTE, idle_timeout=0)
        
        # TCP Burst (port 9001 - iperf3) - Bursty queue
        match_tcp_9001 = parser4.OFPMatch(
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            tcp_dst=9001
        )
        actions_tcp_9001 = [
            parser4.OFPActionSetQueue(2),  # Bursty queue
            parser4.OFPActionOutput(out_port)
        ]
        self.add_flow(dp_leaf1, PRIORITY_REROUTE, match_tcp_9001, actions_tcp_9001,
                     cookie=COOKIE_REROUTE, idle_timeout=0)
        
        # TCP Steady (port 9003 - iperf3) - Bursty queue
        match_tcp_9003 = parser4.OFPMatch(
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            tcp_dst=9003
        )
        actions_tcp_9003 = [
            parser4.OFPActionSetQueue(2),  # Bursty queue
            parser4.OFPActionOutput(out_port)
        ]
        self.add_flow(dp_leaf1, PRIORITY_REROUTE, match_tcp_9003, actions_tcp_9003,
                     cookie=COOKIE_REROUTE, idle_timeout=0)
        
        # Leaf 2: Spine -> H2
        in_port = target_spine
        
        # UDP VoIP
        match_udp_l2 = parser5.OFPMatch(
            in_port=in_port,
            eth_type=0x0800,
            ip_proto=17,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            udp_dst=9000
        )
        actions_udp_l2 = [
            parser5.OFPActionSetQueue(1),
            parser5.OFPActionOutput(4)  # H2 port
        ]
        self.add_flow(dp_leaf2, PRIORITY_REROUTE, match_udp_l2, actions_udp_l2,
                     cookie=COOKIE_REROUTE, idle_timeout=0)
        
        # TCP port 9001
        match_tcp_9001_l2 = parser5.OFPMatch(
            in_port=in_port,
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            tcp_dst=9001
        )
        actions_tcp_9001_l2 = [
            parser5.OFPActionSetQueue(2),
            parser5.OFPActionOutput(4)
        ]
        self.add_flow(dp_leaf2, PRIORITY_REROUTE, match_tcp_9001_l2, actions_tcp_9001_l2,
                     cookie=COOKIE_REROUTE, idle_timeout=0)
        
        # TCP port 9003
        match_tcp_9003_l2 = parser5.OFPMatch(
            in_port=in_port,
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2',
            tcp_dst=9003
        )
        actions_tcp_9003_l2 = [
            parser5.OFPActionSetQueue(2),
            parser5.OFPActionOutput(4)
        ]
        self.add_flow(dp_leaf2, PRIORITY_REROUTE, match_tcp_9003_l2, actions_tcp_9003_l2,
                     cookie=COOKIE_REROUTE, idle_timeout=0)
        
        self.logger.info(f"✅ Installed H1→H2 path via Spine {target_spine}")
        self.logger.info(f"   - UDP port 9000 (VoIP): Queue 1")
        self.logger.info(f"   - TCP port 9001 (Burst): Queue 2")
        self.logger.info(f"   - TCP port 9003 (Steady): Queue 2")

    # =================================================================
    # DEFAULT FLOWS (Initial Setup)
    # =================================================================
    
    def _install_default_flows(self):
        """Install default flows at startup - H1 and H3 both via Spine 2"""
        hub.sleep(3)  # Wait for topology discovery
        
        if 4 not in self.datapaths or 5 not in self.datapaths or 6 not in self.datapaths:
            self.logger.warning("⚠️ Not all datapaths ready for default flows")
            hub.spawn_after(2, self._install_default_flows)
            return
        
        self.logger.info("🔧 Installing DEFAULT flows")
        
        # H1 -> H2 via Spine 2
        self._install_h1_path(2)
        
        # H3 -> H2 via Spine 2 (ALWAYS fixed)
        dp_leaf3 = self.datapaths[6]
        dp_leaf2 = self.datapaths[5]
        parser6 = dp_leaf3.ofproto_parser
        parser5 = dp_leaf2.ofproto_parser
        
        # Leaf 3: H3 -> Spine 2
        # UDP (port 9000)
        match_h3_udp = parser6.OFPMatch(
            eth_type=0x0800,
            ip_proto=17,
            ipv4_src='10.0.0.3',
            ipv4_dst='10.0.0.2',
            udp_dst=9000
        )
        actions_h3_udp = [
            parser6.OFPActionSetQueue(1),
            parser6.OFPActionOutput(2)  # Spine 2
        ]
        self.add_flow(dp_leaf3, PRIORITY_USER, match_h3_udp, actions_h3_udp, idle_timeout=0)
        
        # TCP (port 9001 - iperf3 burst)
        match_h3_tcp_9001 = parser6.OFPMatch(
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.3',
            ipv4_dst='10.0.0.2',
            tcp_dst=9001
        )
        actions_h3_tcp_9001 = [
            parser6.OFPActionSetQueue(2),
            parser6.OFPActionOutput(2)
        ]
        self.add_flow(dp_leaf3, PRIORITY_USER, match_h3_tcp_9001, actions_h3_tcp_9001, idle_timeout=0)
        
        # TCP (port 9003 - iperf3 steady)
        match_h3_tcp_9003 = parser6.OFPMatch(
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.3',
            ipv4_dst='10.0.0.2',
            tcp_dst=9003
        )
        actions_h3_tcp_9003 = [
            parser6.OFPActionSetQueue(2),
            parser6.OFPActionOutput(2)
        ]
        self.add_flow(dp_leaf3, PRIORITY_USER, match_h3_tcp_9003, actions_h3_tcp_9003, idle_timeout=0)
        
        # Leaf 2: From Spine 2 to H2
        # UDP
        match_l2_udp = parser5.OFPMatch(
            in_port=2,
            eth_type=0x0800,
            ip_proto=17,
            ipv4_src='10.0.0.3',
            ipv4_dst='10.0.0.2',
            udp_dst=9000
        )
        actions_l2_udp = [
            parser5.OFPActionSetQueue(1),
            parser5.OFPActionOutput(4)
        ]
        self.add_flow(dp_leaf2, PRIORITY_USER, match_l2_udp, actions_l2_udp, idle_timeout=0)
        
        # TCP port 9001
        match_l2_tcp_9001 = parser5.OFPMatch(
            in_port=2,
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.3',
            ipv4_dst='10.0.0.2',
            tcp_dst=9001
        )
        actions_l2_tcp_9001 = [
            parser5.OFPActionSetQueue(2),
            parser5.OFPActionOutput(4)
        ]
        self.add_flow(dp_leaf2, PRIORITY_USER, match_l2_tcp_9001, actions_l2_tcp_9001, idle_timeout=0)
        
        # TCP port 9003
        match_l2_tcp_9003 = parser5.OFPMatch(
            in_port=2,
            eth_type=0x0800,
            ip_proto=6,
            ipv4_src='10.0.0.3',
            ipv4_dst='10.0.0.2',
            tcp_dst=9003
        )
        actions_l2_tcp_9003 = [
            parser5.OFPActionSetQueue(2),
            parser5.OFPActionOutput(4)
        ]
        self.add_flow(dp_leaf2, PRIORITY_USER, match_l2_tcp_9003, actions_l2_tcp_9003, idle_timeout=0)
        
        self.logger.info("✅ Default flows installed")
        self.logger.info("   H1→H2: Spine 2 (UDP:9000, TCP:9001, TCP:9003)")
        self.logger.info("   H3→H2: Spine 2 (UDP:9000, TCP:9001, TCP:9003) [FIXED]")
        
        self.default_flows_installed = True

    # =================================================================
    # OPENFLOW EVENT HANDLERS
    # =================================================================
    
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Handle switch connection"""
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id
        
        self.datapaths[dpid] = datapath
        self.mac_to_port.setdefault(dpid, {})
        
        # Install table-miss flow
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        
        self.logger.info(f"🔌 Switch connected: DPID {dpid}")

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        """Handle switch state changes"""
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, 
                 cookie=0, idle_timeout=0, hard_timeout=0):
        """Add flow to switch"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        if buffer_id:
            mod = parser.OFPFlowMod(
                datapath=datapath, buffer_id=buffer_id,
                priority=priority, match=match, instructions=inst,
                cookie=cookie, idle_timeout=idle_timeout, hard_timeout=hard_timeout
            )
        else:
            mod = parser.OFPFlowMod(
                datapath=datapath, priority=priority,
                match=match, instructions=inst,
                cookie=cookie, idle_timeout=idle_timeout, hard_timeout=hard_timeout
            )
        datapath.send_msg(mod)

    # =================================================================
    # TOPOLOGY DISCOVERY
    # =================================================================
    
    def _discover_topology(self):
        """Discover network topology"""
        while True:
            try:
                switch_list = get_switch(self, None)
                self.net.clear()
                
                for switch in switch_list:
                    dpid = switch.dp.id
                    self.net.add_node(dpid)
                
                links = get_link(self, None)
                for link in links:
                    src = link.src.dpid
                    dst = link.dst.dpid
                    self.net.add_edge(src, dst, port=link.src.port_no)
                    self.net.add_edge(dst, src, port=link.dst.port_no)
                
            except Exception as e:
                pass
            
            hub.sleep(10)

    # =================================================================
    # TRAFFIC MONITORING
    # =================================================================
    
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        """Process port statistics"""
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        # Only monitor Leaf 2 (DPID 5) - where H2 is connected
        if dpid != 5:
            return
        
        current_time = time.time()
        
        for stat in body:
            port_no = stat.port_no
            
            # Monitor uplink ports (1, 2, 3 = to Spines)
            if port_no not in [1, 2, 3]:
                continue
            
            key = (dpid, port_no)
            
            if key in self.last_bytes and key in self.last_bytes_timestamp:
                time_diff = current_time - self.last_bytes_timestamp[key]
                
                if time_diff > 0:
                    byte_diff = stat.tx_bytes - self.last_bytes[key]
                    throughput_bps = (byte_diff * 8) / time_diff
                    
                    # Map port to spine number
                    spine_num = port_no
                    self.spine_traffic[spine_num] = throughput_bps
            
            self.last_bytes[key] = stat.tx_bytes
            self.last_bytes_timestamp[key] = current_time

    def _monitor_traffic(self):
        """Send port stats requests periodically"""
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(3)
    
    def _request_stats(self, datapath):
        """Request port statistics"""
        parser = datapath.ofproto_parser
        req = parser.OFPPortStatsRequest(datapath, 0, datapath.ofproto.OFPP_ANY)
        datapath.send_msg(req)

    # =================================================================
    # PACKET HANDLING
    # =================================================================
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """Handle packet-in events"""
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id
        
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        
        dst = eth.dst
        src = eth.src
        
        # Learn MAC
        self.mac_to_port[dpid][src] = in_port
        
        # Parse protocols
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        udp_pkt = pkt.get_protocol(udp.udp)
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        
        # === SPECIAL HANDLING FOR H3->H2 (Leaf 3 - DPID 6) ===
        if ip_pkt and dpid == 6:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            if src_ip == '10.0.0.3' and dst_ip == '10.0.0.2':
                # ALWAYS port 2 (Spine 2)
                # Detect protocol and port
                if udp_pkt and udp_pkt.dst_port == 9000:
                    queue_id = 1  # VoIP
                elif tcp_pkt and tcp_pkt.dst_port in [9001, 9003]:
                    queue_id = 2  # Bursty
                else:
                    queue_id = 1  # Default
                
                actions = [
                    parser.OFPActionSetQueue(queue_id),
                    parser.OFPActionOutput(2)
                ]
                
                # Install specific flow
                if udp_pkt and udp_pkt.dst_port == 9000:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ip_proto=17,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        udp_dst=9000
                    )
                    self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                elif tcp_pkt and tcp_pkt.dst_port in [9001, 9003]:
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ip_proto=6,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        tcp_dst=tcp_pkt.dst_port
                    )
                    self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                else:
                    # Other traffic
                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip
                    )
                    self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                
                # Forward packet
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
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            if dst_ip == '10.0.0.2':
                if dst in self.mac_to_port[dpid]:
                    out_port = self.mac_to_port[dpid][dst]
                else:
                    out_port = ofproto.OFPP_FLOOD
                
                if out_port != ofproto.OFPP_FLOOD:
                    # Detect queue based on protocol & port
                    queue_id = 1  # Default VoIP queue
                    
                    if udp_pkt:
                        queue_id = 1 if udp_pkt.dst_port == 9000 else 2
                    elif tcp_pkt:
                        queue_id = 2  # TCP = bursty queue
                    
                    actions = [
                        parser.OFPActionSetQueue(queue_id),
                        parser.OFPActionOutput(out_port)
                    ]
                    
                    # Install flow for UDP or TCP
                    if udp_pkt and udp_pkt.dst_port == 9000:
                        match = parser.OFPMatch(
                            eth_type=0x0800,
                            ip_proto=17,
                            ipv4_src=src_ip,
                            ipv4_dst=dst_ip,
                            udp_dst=9000
                        )
                        self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                    elif tcp_pkt and tcp_pkt.dst_port in [9001, 9003]:
                        match = parser.OFPMatch(
                            eth_type=0x0800,
                            ip_proto=6,
                            ipv4_src=src_ip,
                            ipv4_dst=dst_ip,
                            tcp_dst=tcp_pkt.dst_port
                        )
                        self.add_flow(datapath, PRIORITY_USER, match, actions, msg.buffer_id)
                
                # Forward packet
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
            # Default match based on MAC
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            
            # Refine match if IP packet
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