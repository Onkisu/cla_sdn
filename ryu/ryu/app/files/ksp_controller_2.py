#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FIXED)
- Fix: Clear state management with flow tracking
- Fix: Proper priority-based flow management
- Fix: No zombie traffic or doubling
- Fix: Correct reroute and revert behavior
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
BURST_THRESHOLD_BPS = 120000
LOWER_THRESHOLD_BPS = 80000  # Untuk revert
COOLDOWN_PERIOD = 30  # seconds

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        # --- DATA STRUCTURES ---
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.datapaths = {}
        self.net = nx.DiGraph()
        
        # --- TRAFFIC MONITORING ---
        self.flow_stats = {}  # {(dpid, src_ip, dst_ip): (bytes, packets, timestamp)}
        
        # --- ROUTING STATE ---
        self.congestion_active = False
        self.reroute_priority = 40000  # High priority for reroute
        self.default_priority = 10     # Low priority for default
        self.last_congestion_time = 0
        self.current_reroute_path = None
        
        # --- FLOW TRACKING (Prevent duplicates) ---
        self.installed_flows = {}  # {dpid: [(match_key, priority, cookie)]}
        self.cookie_counter = 1000  # For unique flow identification
        
        # --- PATHS CONFIGURATION ---
        self.default_path = {
            4: {'out_port': 3, 'next_hop': 1},  # Leaf1 -> Spine2
            1: {'out_port': 3, 'next_hop': 5},  # Spine2 -> Leaf2
            5: {'out_port': 1, 'next_hop': None}  # Leaf2 -> H2
        }
        
        self.reroute_path = {
            4: {'out_port': 1, 'next_hop': 1},   # Leaf1 -> Spine1
            1: {'out_port': 1, 'next_hop': 5},   # Spine1 -> Leaf2
            5: {'out_port': 1, 'next_hop': None} # Leaf2 -> H2
        }
        
        # Connect to database
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)
        
        self.logger.info("✅ VoIP Controller FIXED VERSION Started")

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
            self.logger.error(f"DB Connection Error: {e}")
            return None

    # =================================================================
    # 1. TOPOLOGY DISCOVERY
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            try:
                self.net.clear()
                switches = get_switch(self, None)
                for s in switches:
                    self.net.add_node(s.dp.id)
                
                links = get_link(self, None)
                for l in links:
                    self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)
                    
                self.logger.debug(f"Topology updated: {len(switches)} switches, {len(links)} links")
            except Exception as e:
                self.logger.error(f"Topology discovery error: {e}")

    # =================================================================
    # 2. FORECAST MONITORING & REROUTE LOGIC
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)
            
            # Skip if no database connection
            if not self.db_conn:
                continue
                
            try:
                conn = self.get_db_conn()
                if not conn:
                    continue
                    
                cur = conn.cursor()
                cur.execute("""
                    SELECT y_pred FROM forecast_1h 
                    ORDER BY ts_created DESC LIMIT 1
                """)
                result = cur.fetchone()
                cur.close()
                conn.close()
                
                if not result:
                    continue
                    
                pred_bps = result[0]
                current_time = time.time()
                
                # REROUTE TRIGGER: Congestion predicted
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️ CONGESTION PREDICTED: {pred_bps:.0f} bps -> REROUTING!")
                    self.apply_reroute()
                    self.last_congestion_time = current_time
                    
                # REVERT TRIGGER: Traffic stabilized
                elif self.congestion_active:
                    delta_t = current_time - self.last_congestion_time
                    if pred_bps < LOWER_THRESHOLD_BPS and delta_t > COOLDOWN_PERIOD:
                        self.logger.info(f"✅ TRAFFIC STABLE ({pred_bps:.0f} bps) -> REVERTING.")
                        self.revert_to_default()
                        
            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")

    def apply_reroute(self):
        """Apply reroute path (H1 via Spine1)"""
        self.logger.info("🚀 Applying REROUTE: H1 -> Spine1")
        
        # 1. First, remove existing flows for H1->H2
        self.remove_voip_flows('10.0.0.1', '10.0.0.2')
        
        # 2. Install new flows along reroute path
        for dpid, config in self.reroute_path.items():
            if dpid not in self.datapaths:
                continue
                
            dp = self.datapaths[dpid]
            parser = dp.ofproto_parser
            
            # Create match for H1->H2 traffic
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2'
            )
            
            # Create actions
            actions = [parser.OFPActionOutput(config['out_port'])]
            
            # Install flow with HIGH priority
            self.install_flow(
                dp, 
                match, 
                actions, 
                priority=self.reroute_priority,
                idle_timeout=0,
                hard_timeout=0,
                cookie=self.cookie_counter + 1
            )
            
            self.logger.debug(f"  Installed reroute flow on dpid={dpid}, port={config['out_port']}")
        
        # 3. Update state
        self.congestion_active = True
        self.current_reroute_path = [4, 1, 5]  # Leaf1 -> Spine1 -> Leaf2
        
        # 4. Log to database
        self.insert_event_log("REROUTE_ACTIVE", f"H1 rerouted via Spine1", BURST_THRESHOLD_BPS)
        
        self.logger.info("✅ REROUTE Complete")

    def revert_to_default(self):
        """Revert to default path (H1 via Spine2)"""
        self.logger.info("🔄 Reverting to DEFAULT: H1 -> Spine2")
        
        # 1. Remove reroute flows (high priority only)
        self.remove_voip_flows('10.0.0.1', '10.0.0.2', target_priority=self.reroute_priority)
        
        # 2. Wait a bit for flow removal to complete
        hub.sleep(0.5)
        
        # 3. Install default flows
        for dpid, config in self.default_path.items():
            if dpid not in self.datapaths:
                continue
                
            dp = self.datapaths[dpid]
            parser = dp.ofproto_parser
            
            # Create match for H1->H2 traffic
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2'
            )
            
            # Create actions
            actions = [parser.OFPActionOutput(config['out_port'])]
            
            # Install flow with LOW priority
            self.install_flow(
                dp,
                match,
                actions,
                priority=self.default_priority,
                idle_timeout=0,
                hard_timeout=0,
                cookie=self.cookie_counter + 2
            )
            
            self.logger.debug(f"  Installed default flow on dpid={dpid}, port={config['out_port']}")
        
        # 4. Update state
        self.congestion_active = False
        self.current_reroute_path = None
        
        # 5. Log to database
        self.insert_event_log("REROUTE_REVERT", "H1 restored to default path via Spine2", 0)
        
        self.logger.info("✅ REVERT Complete")

    # =================================================================
    # 3. FLOW MANAGEMENT FUNCTIONS
    # =================================================================
    def install_flow(self, datapath, match, actions, priority=1, 
                    idle_timeout=0, hard_timeout=0, buffer_id=None, cookie=0):
        """Install flow with tracking to prevent duplicates"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Create instruction
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # Create flow mod
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=inst,
            idle_timeout=idle_timeout,
            hard_timeout=hard_timeout,
            buffer_id=buffer_id if buffer_id else ofproto.OFP_NO_BUFFER,
            cookie=cookie,
            flags=ofproto.OFPFF_SEND_FLOW_REM if cookie > 0 else 0
        )
        
        # Send flow mod
        datapath.send_msg(mod)
        
        # Track installed flow
        dpid = datapath.id
        match_key = str(match)
        
        if dpid not in self.installed_flows:
            self.installed_flows[dpid] = []
        
        # Check if flow already installed
        for flow in self.installed_flows[dpid]:
            if flow[0] == match_key and flow[1] == priority:
                self.logger.debug(f"Flow already exists on dpid={dpid}: {match_key}")
                return
        
        # Add to tracking
        self.installed_flows[dpid].append((match_key, priority, cookie))
        
        self.logger.debug(f"Installed flow on dpid={dpid}, priority={priority}")

    def remove_voip_flows(self, src_ip, dst_ip, target_priority=None):
        """Remove specific VoIP flows (can target specific priority)"""
        self.logger.debug(f"Removing VoIP flows: {src_ip}->{dst_ip}")
        
        for dpid, dp in self.datapaths.items():
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            
            # Create match
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_ip,
                ipv4_dst=dst_ip
            )
            
            # Create flow mod for deletion
            if target_priority:
                # Delete specific priority only
                mod = parser.OFPFlowMod(
                    datapath=dp,
                    command=ofproto.OFPFC_DELETE_STRICT,
                    priority=target_priority,
                    match=match,
                    out_port=ofproto.OFPP_ANY,
                    out_group=ofproto.OFPG_ANY
                )
            else:
                # Delete all priorities
                mod = parser.OFPFlowMod(
                    datapath=dp,
                    command=ofproto.OFPFC_DELETE,
                    match=match,
                    out_port=ofproto.OFPP_ANY,
                    out_group=ofproto.OFPG_ANY
                )
            
            dp.send_msg(mod)
            
            # Remove from tracking
            if dpid in self.installed_flows:
                match_key = str(match)
                self.installed_flows[dpid] = [
                    flow for flow in self.installed_flows[dpid] 
                    if flow[0] != match_key or (target_priority and flow[1] != target_priority)
                ]
        
        # Send barrier to ensure deletion completes
        self.send_barrier_request(dp)
        
        self.logger.debug(f"VoIP flows removed: {src_ip}->{dst_ip}")

    def send_barrier_request(self, datapath):
        """Send barrier request to ensure flow operations complete"""
        parser = datapath.ofproto_parser
        req = parser.OFPBarrierRequest(datapath)
        datapath.send_msg(req)

    def insert_event_log(self, event_type, description, trigger_value=0):
        """Insert event into database"""
        conn = self.get_db_conn()
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
            conn.close()
        except Exception as e:
            self.logger.error(f"Failed to insert event log: {e}")

    # =================================================================
    # 4. CORE HANDLERS (PacketIn, Switch Features, etc.)
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """Handle switch connection/disconnection"""
        datapath = ev.datapath
        dpid = datapath.id
        
        if ev.state == MAIN_DISPATCHER:
            if dpid not in self.datapaths:
                self.datapaths[dpid] = datapath
                self.logger.info(f"Switch connected: dpid={dpid}")
                
                # Initialize default flows for Leaf1 if connected
                if dpid == 4 and not self.congestion_active:
                    self._install_initial_flows(datapath)
                    
        elif ev.state == DEAD_DISPATCHER:
            if dpid in self.datapaths:
                del self.datapaths[dpid]
                # Clean up tracking
                if dpid in self.installed_flows:
                    del self.installed_flows[dpid]
                self.logger.info(f"Switch disconnected: dpid={dpid}")

    def _install_initial_flows(self, datapath):
        """Install initial default flows for new switch"""
        if datapath.id == 4:  # Leaf1
            parser = datapath.ofproto_parser
            
            # Default flow for H1->H2 (via Spine2)
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src='10.0.0.1',
                ipv4_dst='10.0.0.2'
            )
            actions = [parser.OFPActionOutput(3)]  # Port to Spine2
            
            self.install_flow(
                datapath,
                match,
                actions,
                priority=self.default_priority,
                idle_timeout=0,
                hard_timeout=0,
                cookie=self.cookie_counter + 2
            )
            
            self.logger.info(f"Initial default flow installed on dpid={datapath.id}")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Handle switch features - install table-miss flow"""
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Install table-miss flow entry
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, 
                                         ofproto.OFPCML_NO_BUFFER)]
        
        self.install_flow(datapath, match, actions, priority=0, 
                         idle_timeout=0, hard_timeout=0)

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
        
        # Ignore LLDP packets
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        
        dst_mac = eth.dst
        src_mac = eth.src
        
        # Update MAC learning table
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src_mac] = in_port
        
        # Handle ARP
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self._handle_arp(datapath, in_port, eth, arp_pkt, msg.buffer_id)
            return
        
        # Handle IP packets
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Update IP to MAC mapping
            self.ip_to_mac[src_ip] = src_mac
            
            # Special handling for VoIP traffic (H1->H2)
            if src_ip == '10.0.0.1' and dst_ip == '10.0.0.2':
                self._handle_voip_traffic(datapath, in_port, src_ip, dst_ip, 
                                         src_mac, dst_mac, msg.buffer_id, msg.data)
                return
        
        # Standard learning switch behavior for other traffic
        self._handle_learning_switch(datapath, in_port, src_mac, dst_mac, 
                                   msg.buffer_id, msg.data)

    def _handle_voip_traffic(self, datapath, in_port, src_ip, dst_ip, 
                           src_mac, dst_mac, buffer_id, data):
        """Handle VoIP traffic (H1->H2)"""
        dpid = datapath.id
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        
        # Determine output port based on current state
        if dpid == 4:  # Leaf1
            if self.congestion_active:
                out_port = 1  # Reroute: Spine1
                priority = self.reroute_priority
            else:
                out_port = 3  # Default: Spine2
                priority = self.default_priority
                
            # Create match
            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_ip,
                ipv4_dst=dst_ip
            )
            
            # Create actions
            actions = [parser.OFPActionOutput(out_port)]
            
            # Install flow
            self.install_flow(
                datapath,
                match,
                actions,
                priority=priority,
                idle_timeout=300,  # 5 minutes idle timeout
                hard_timeout=0,
                buffer_id=buffer_id,
                cookie=self.cookie_counter + (1 if self.congestion_active else 2)
            )
            
            # Send packet out if needed
            if buffer_id == ofproto.OFP_NO_BUFFER:
                out = parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=ofproto.OFP_NO_BUFFER,
                    in_port=in_port,
                    actions=actions,
                    data=data
                )
                datapath.send_msg(out)
                
            self.logger.debug(f"VoIP flow installed on dpid={dpid}, port={out_port}")

    def _handle_arp(self, datapath, in_port, eth, arp_pkt, buffer_id):
        """Handle ARP packets"""
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        
        # Update ARP table
        self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
        
        # Handle ARP request
        if arp_pkt.opcode == arp.ARP_REQUEST:
            # Check if we know the destination IP
            if arp_pkt.dst_ip in self.ip_to_mac:
                target_mac = self.ip_to_mac[arp_pkt.dst_ip]
                
                # Build ARP reply
                e = ethernet.ethernet(
                    dst=eth.src,
                    src=target_mac,
                    ethertype=ether_types.ETH_TYPE_ARP
                )
                a = arp.arp(
                    opcode=arp.ARP_REPLY,
                    src_mac=target_mac,
                    src_ip=arp_pkt.dst_ip,
                    dst_mac=eth.src,
                    dst_ip=arp_pkt.src_ip
                )
                
                # Create and send packet
                p = packet.Packet()
                p.add_protocol(e)
                p.add_protocol(a)
                p.serialize()
                
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=ofproto.OFP_NO_BUFFER,
                    in_port=ofproto.OFPP_CONTROLLER,
                    actions=actions,
                    data=p.data
                )
                datapath.send_msg(out)

    def _handle_learning_switch(self, datapath, in_port, src_mac, dst_mac, 
                              buffer_id, data):
        """Standard learning switch behavior"""
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Learn source MAC address
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src_mac] = in_port
        
        # Determine output port
        if dst_mac in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst_mac]
        else:
            out_port = ofproto.OFPP_FLOOD
        
        # Create actions
        actions = [parser.OFPActionOutput(out_port)]
        
        # Install flow if not flooding
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(
                in_port=in_port,
                eth_dst=dst_mac,
                eth_src=src_mac
            )
            self.install_flow(datapath, match, actions, priority=1, 
                            idle_timeout=60, hard_timeout=0, buffer_id=buffer_id)
        
        # Send packet out
        if buffer_id == ofproto.OFP_NO_BUFFER:
            out = parser.OFPPacketOut(
                datapath=datapath,
                buffer_id=buffer_id,
                in_port=in_port,
                actions=actions,
                data=data
            )
            datapath.send_msg(out)

    # =================================================================
    # 5. TRAFFIC MONITORING
    # =================================================================
    def _monitor_traffic(self):
        """Periodically request flow statistics"""
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
            hub.sleep(2)  # Check every 2 seconds

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Handle flow statistics replies"""
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()
        
        for stat in body:
            # Skip table-miss flows
            if stat.priority == 0:
                continue
            
            # Extract match information
            match = stat.match
            src_ip = match.get('ipv4_src')
            dst_ip = match.get('ipv4_dst')
            
            # Only track VoIP traffic (H1/H3 -> H2)
            if not src_ip or not dst_ip or dst_ip != '10.0.0.2':
                continue
                
            if src_ip not in ['10.0.0.1', '10.0.0.3']:
                continue
            
            # Calculate traffic rate
            flow_key = (dpid, src_ip, dst_ip)
            current_bytes = stat.byte_count
            current_packets = stat.packet_count
            
            if flow_key in self.flow_stats:
                last_bytes, last_packets, last_time = self.flow_stats[flow_key]
                
                # Calculate time delta
                time_delta = (timestamp - last_time).total_seconds()
                if time_delta > 0:
                    # Calculate rates
                    byte_rate = (current_bytes - last_bytes) / time_delta
                    packet_rate = (current_packets - last_packets) / time_delta
                    
                    # Store in database
                    self._store_flow_stats(
                        timestamp, dpid, src_ip, dst_ip,
                        match, byte_rate, packet_rate,
                        stat.duration_sec, stat.duration_nsec
                    )
            
            # Update flow stats
            self.flow_stats[flow_key] = (current_bytes, current_packets, timestamp)

    def _store_flow_stats(self, timestamp, dpid, src_ip, dst_ip, 
                         match, byte_rate, packet_rate, duration_sec, duration_nsec):
        """Store flow statistics in database"""
        conn = self.get_db_conn()
        if not conn:
            return
            
        try:
            cur = conn.cursor()
            
            # Determine traffic label
            if src_ip == '10.0.0.1':
                traffic_label = 'voip'
            elif src_ip == '10.0.0.3':
                traffic_label = 'bursty'
            else:
                traffic_label = 'other'
            
            # Insert into database
            cur.execute("""
                INSERT INTO traffic.flow_stats_
                (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                 ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                 pkts_tx, pkts_rx, duration_sec, traffic_label)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s)
            """, (
                timestamp, dpid, src_ip, dst_ip,
                match.get('eth_src', ''), match.get('eth_dst', ''),
                match.get('ip_proto', 17),
                match.get('tcp_src') or match.get('udp_src') or 0,
                match.get('tcp_dst') or match.get('udp_dst') or 0,
                byte_rate, byte_rate,  # Same for tx/rx in this simple model
                packet_rate, packet_rate,
                duration_sec + (duration_nsec / 1e9),
                traffic_label
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to store flow stats: {e}")

    # =================================================================
    # 6. UTILITY FUNCTIONS
    # =================================================================
    def _resolve_ip(self, mac):
        """Resolve IP address from MAC"""
        if not mac:
            return None
            
        for ip, m in self.ip_to_mac.items():
            if m == mac:
                return ip
        return None

    def cleanup(self):
        """Clean up on controller shutdown"""
        if self.db_conn:
            self.db_conn.close()
        self.logger.info("Controller shutdown complete")


if __name__ == '__main__':
    # For standalone testing
    from ryu.cmd import manager
    import sys
    sys.argv = ['ryu-manager', __file__]
    manager.main()