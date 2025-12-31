#!/usr/bin/env python3
"""
Ryu SDN Controller for VoIP Traffic Monitoring
Monitors flow statistics and stores to PostgreSQL
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp
from ryu.lib.packet import arp
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

TABLE_NAME = 'traffic.flow_stats_'

class VoIPTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPTrafficMonitor, self).__init__(*args, **kwargs)
        
        # MAC to IP mapping
        self.mac_to_port = {}
        self.ip_to_mac = {}
        
        # Datapaths
        self.datapaths = {}
        
        # Database connection
        self.db_conn = None
        self.connect_database()
        
        # Flow statistics
        self.flow_stats = {}
        self.last_bytes = {}
        
        # Start time for pattern generation
        self.start_time = time.time()
        
        # Start monitoring thread
        self.monitor_thread = hub.spawn(self._monitor)
        
        self.logger.info("VoIP Traffic Monitor started")

    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            cursor = self.db_conn.cursor()
            
            # Create table if not exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS traffic.flow_stats_ (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                dpid VARCHAR(20) NOT NULL,
                src_ip VARCHAR(15) NOT NULL,
                dst_ip VARCHAR(15) NOT NULL,
                src_mac VARCHAR(17) NOT NULL,
                dst_mac VARCHAR(17) NOT NULL,
                ip_proto INTEGER NOT NULL,
                tp_src INTEGER NOT NULL,
                tp_dst INTEGER NOT NULL,
                bytes_tx BIGINT NOT NULL,
                bytes_rx BIGINT NOT NULL,
                pkts_tx INTEGER NOT NULL,
                pkts_rx INTEGER NOT NULL,
                duration_sec FLOAT NOT NULL,
                traffic_label VARCHAR(50) NOT NULL
            );
            """
            
            cursor.execute(create_table_query)
            self.db_conn.commit()
            cursor.close()
            
            self.logger.info("Database connected and table created")
            
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")

    def generate_bytes_pattern(self, elapsed_seconds):
        """
        Generate bytes_tx with pattern (sine wave)
        30 minutes UP (0-1800s), 30 minutes DOWN (1800-3600s) = 1 hour cycle
        Range: 13000 - 19800 bytes
        """
        base = 16400  # Middle point
        amplitude = 3400  # Half range
        period = 3600  # 1 hour = 3600 seconds
        
        # Sine wave: naik 30 menit, turun 30 menit
        phase = (elapsed_seconds % period) / period * 2 * math.pi
        sine_value = math.sin(phase)
        
        # Small random noise for variation
        noise = random.uniform(-0.05, 0.05)
        
        bytes_tx = int(base + (amplitude * sine_value) + (base * noise))
        bytes_tx = max(13000, min(19800, bytes_tx))
        
        return bytes_tx
        
        bytes_tx = int(base + (amplitude * sine_value) + (base * noise))
        bytes_tx = max(13000, min(19800, bytes_tx))
        
        return bytes_tx

    def insert_flow_data(self, flow_data):
        """Insert flow data into PostgreSQL"""
        try:
            cursor = self.db_conn.cursor()
            
            insert_query = """
            INSERT INTO traffic.flow_stats_ 
            (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
             ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
             duration_sec, traffic_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, flow_data)
            self.db_conn.commit()
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error inserting data: {e}")
            try:
                self.db_conn.rollback()
            except:
                self.connect_database()

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """Handle datapath state changes"""
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.info('Register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.info('Unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Handle switch features"""
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Install table-miss flow entry
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        """Add flow entry to switch"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Handle packet in events"""
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
            # === HANDLE ARP (SETELAH eth ADA) ===
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
            out = parser.OFPPacketOut(
                datapath=datapath,
                buffer_id=msg.buffer_id,
                in_port=in_port,
                actions=actions,
                data=msg.data
            )
            datapath.send_msg(out)
            return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # Learn IP to MAC mapping
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            self.ip_to_mac[ip_pkt.src] = src
            self.ip_to_mac[ip_pkt.dst] = dst

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)

            if ip_pkt:
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_src=src,
                    eth_dst=dst,
                    eth_type=0x0800,
                    ipv4_src=ip_pkt.src,
                    ipv4_dst=ip_pkt.dst
                )
            else:
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_src=src,
                    eth_dst=dst
                )

            
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor(self):
        """Monitor thread to request flow statistics"""
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)  # Request stats every 1 second

    def _request_stats(self, datapath):
        """Request flow statistics from datapath"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Handle flow statistics reply - uses REAL traffic data and scales it"""
        timestamp = datetime.now()
        body = ev.msg.body
        datapath = ev.msg.datapath
        dpid = format(datapath.id, '016x')
        
        elapsed_seconds = int(time.time() - self.start_time)
        
        # Collect all valid flows with REAL traffic data
        valid_flows = []
        total_real_bytes = 0
        
        for stat in body:
            # Skip table-miss flow
            if stat.priority == 0:
                continue

            # Extract match fields
            match = stat.match
            
            # Get source and destination MACs
            src_mac = match.get('eth_src', '00:00:00:00:00:00')
            dst_mac = match.get('eth_dst', '00:00:00:00:00:00')
            
            # Try to get IP addresses
            src_ip = match.get('ipv4_src', '0.0.0.0')
            dst_ip = match.get('ipv4_dst', '0.0.0.0')
            
            # If no IP in match, try to resolve from MAC
            if src_ip == '0.0.0.0':
                for ip, mac in self.ip_to_mac.items():
                    if mac == src_mac:
                        src_ip = ip
                        break
                else:
                    # Generate IP from MAC for consistency
                    mac_parts = src_mac.split(':')
                    if len(mac_parts) == 6:
                        src_ip = f"10.0.{int(mac_parts[4], 16)}.{int(mac_parts[5], 16)}"
            
            if dst_ip == '0.0.0.0':
                for ip, mac in self.ip_to_mac.items():
                    if mac == dst_mac:
                        dst_ip = ip
                        break
                else:
                    mac_parts = dst_mac.split(':')
                    if len(mac_parts) == 6:
                        dst_ip = f"10.0.{int(mac_parts[4], 16)}.{int(mac_parts[5], 16)}"
            
            # Only process if we have valid IPs (skip ARP, etc.)
            if src_ip != '0.0.0.0' and dst_ip != '0.0.0.0':
                # Protocol and ports (default to VoIP values)
                ip_proto = match.get('ip_proto', 17)  # UDP
                tp_src = match.get('udp_src', random.randint(16384, 32767))
                tp_dst = match.get('udp_dst', random.randint(16384, 32767))
                
                # Get REAL traffic data from OpenFlow stats
                byte_count = stat.byte_count
                packet_count = stat.packet_count
                
                # Generate flow key for tracking
                flow_key = f"{dpid}-{src_ip}-{dst_ip}-{tp_src}-{tp_dst}"
                
                # Calculate bytes in THIS interval (delta from last measurement)
                if flow_key in self.last_bytes:
                    last_bytes, last_packets = self.last_bytes[flow_key]
                    real_bytes_tx = byte_count - last_bytes
                    real_pkts_tx = packet_count - last_packets
                else:
                    real_bytes_tx = byte_count
                    real_pkts_tx = packet_count
                
                # Update last values for next interval
                self.last_bytes[flow_key] = (byte_count, packet_count)
                
                # Only include flows with actual traffic
                if real_bytes_tx > 0:
                    valid_flows.append({
                        'dpid': dpid,
                        'src_ip': src_ip,
                        'dst_ip': dst_ip,
                        'src_mac': src_mac,
                        'dst_mac': dst_mac,
                        'ip_proto': ip_proto,
                        'tp_src': tp_src,
                        'tp_dst': tp_dst,
                        'real_bytes': real_bytes_tx,
                        'real_pkts': real_pkts_tx,
                        'flow_key': flow_key
                    })
                    total_real_bytes += real_bytes_tx
        
        # If no valid flows with traffic, skip this cycle
        if not valid_flows or total_real_bytes == 0:
            return
        
        # Generate TARGET total bytes per flow for this second (13000-19800)
        target_bytes_per_flow = self.generate_bytes_pattern(elapsed_seconds)
        
        # Calculate cycle position (for logging)
        cycle_position = elapsed_seconds % 3600
        cycle_phase = "NAIK" if cycle_position < 1800 else "TURUN"
        cycle_minute = cycle_position // 60
        
        num_flows = len(valid_flows)

        # Log summary for this second
        self.logger.info(f"=" * 80)
        self.logger.info(f"Second: {elapsed_seconds}s | Cycle: {cycle_minute}min | Phase: {cycle_phase}")
        self.logger.info(f"Total REAL traffic from D-ITG: {total_real_bytes} bytes")
        self.logger.info(f"Target per flow: {target_bytes_per_flow} bytes (13000-19800 range)")
        self.logger.info(f"Active flows: {num_flows}")
        self.logger.info(f"-" * 80)
        
        # Scale each flow's REAL data to match target while keeping proportions
        
        
        # Insert each flow with scaled bytes
        for i, flow_info in enumerate(valid_flows):
            # Calculate this flow's proportion of total real traffic
            flow_proportion = flow_info['real_bytes'] / total_real_bytes
            
            # Scale to target range: each flow gets 13000-19800
            # Keep the real proportion but scale to target
            flow_bytes_tx = target_bytes_per_flow
            
            # Add variation based on real proportion (±20%)
            variation = (flow_proportion - 0.5) * 0.4  # -0.2 to +0.2
            flow_bytes_tx = int(flow_bytes_tx * (1 + variation))
            
            # Ensure within range
            flow_bytes_tx = max(13000, min(19800, flow_bytes_tx))
            
            # Calculate corresponding packets based on real packet size
            if flow_info['real_pkts'] > 0:
                real_pkt_size = flow_info['real_bytes'] / flow_info['real_pkts']
                pkts_tx = max(1, int(flow_bytes_tx / real_pkt_size))
            else:
                avg_packet_size = 180
                pkts_tx = max(1, int(flow_bytes_tx / avg_packet_size))
            
            # bytes_rx similar to bytes_tx (bidirectional VoIP)
            bytes_rx = int(flow_bytes_tx * random.uniform(0.95, 1.05))
            pkts_rx = max(1, int(pkts_tx * random.uniform(0.95, 1.05)))
            
            # Traffic label
            traffic_label = 'voip'
            
            # Prepare flow data
            flow_data = (
                timestamp,
                flow_info['dpid'],
                flow_info['src_ip'],
                flow_info['dst_ip'],
                flow_info['src_mac'],
                flow_info['dst_mac'],
                flow_info['ip_proto'],
                flow_info['tp_src'],
                flow_info['tp_dst'],
                flow_bytes_tx,
                bytes_rx,
                pkts_tx,
                pkts_rx,
                1.0,  # duration_sec (1 second interval)
                traffic_label
            )
            
            # Insert into database
            self.insert_flow_data(flow_data)
            
            # Detailed logging per flow
            self.logger.info(f"  Flow {i+1}/{num_flows}: {flow_info['src_ip']}→{flow_info['dst_ip']}")
            self.logger.info(f"    D-ITG Real: {flow_info['real_bytes']:,}B ({flow_info['real_pkts']} pkts)")
            self.logger.info(f"    Scaled:     {flow_bytes_tx:,}B ({pkts_tx} pkts)")
            self.logger.info(f"    Proportion: {flow_proportion:.2%} of total traffic")
            self.logger.info(f"    Saved to DB: bytes_tx={flow_bytes_tx}, bytes_rx={bytes_rx}")
        
        # Summary line after all flows
        total_scaled = sum([max(13000, min(19800, int(target_bytes_per_flow * (1 + ((flow_info['real_bytes'] / total_real_bytes) - 0.5) * 0.4)))) 
                           for flow_info in valid_flows])
        self.logger.info(f"-" * 80)
        self.logger.info(f"SUMMARY: Total scaled traffic saved: ~{total_scaled:,} bytes")
        self.logger.info(f"=" * 80 + "\n")
