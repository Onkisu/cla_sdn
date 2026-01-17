#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FINAL FIXED VERSION)
- Logika Reroute yang Konsisten (Statis atau Dinamis, Pilih SATU)
- Flow Deletion Lengkap (Forward + Reverse)
- Cooldown Logic yang Benar
- Threshold yang Stabil
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
from ryu.topology import api
from ryu.topology.api import get_switch, get_link
import networkx as nx
import psycopg2
from datetime import datetime
import time
import itertools

# PostgreSQL Configuration
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# Threshold Congestion - DIUBAH LEBIH TINGGI untuk menghindari flapping
BURST_THRESHOLD_BPS = 150000  # Naik dari 120k
LOWER_THRESHOLD_BPS = 50000   # Turun dari 80k untuk hysteresis lebih lebar

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
        self.reroute_priority = 40000 
        self.default_priority = 10    
        self.last_reroute_time = 0
        self.cooldown_period = 30  
        
        # --- FLAPPING PROTECTION ---
        self.consecutive_high_preds = 0  # Counter prediksi tinggi berturut-turut
        self.consecutive_low_preds = 0   # Counter prediksi rendah berturut-turut
        self.required_high_for_reroute = 3  # Butuh 3 prediksi tinggi berturut-turut
        self.required_low_for_revert = 5     # Butuh 5 prediksi rendah berturut-turut
        
        # Path definitions
        self.DEFAULT_PATH = {
            4: 3,  # Leaf1 -> Spine2 (Port 3)
            2: 1,  # Spine2 -> Leaf2 (Port 1)
            5: 1   # Leaf2 -> H2 (Port 1)
        }
        
        self.REROUTE_PATH = {
            4: 1,  # Leaf1 -> Spine1 (Port 1)
            1: 1,  # Spine1 -> Leaf2 (Port 1)
            5: 1   # Leaf2 -> H2 (Port 1)
        }
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("✅ VoIP Controller (FINAL FIXED) Started")
        self.logger.info(f"⚙️ Config: Burst={BURST_THRESHOLD_BPS}, Revert={LOWER_THRESHOLD_BPS}")

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
                self.logger.debug(f"Topology updated: {len(switches)} switches, {len(links)*2} links")
            except Exception as e:
                self.logger.error(f"Topology Error: {e}")

    # =================================================================
    # 2. FORECAST MONITORING & REROUTE LOGIC (FIXED)
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)  # Check every 2 seconds
            
            # Skip jika masih dalam cooldown
            if time.time() - self.last_reroute_time < self.cooldown_period:
                continue
                
            conn = self.get_db_conn()
            if not conn: 
                continue
                
            try:
                cur = conn.cursor()
                # Ambil prediksi terakhir
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

                # --- LOGIKA DENGAN HYSTERESIS & FLAPPING PROTECTION ---
                
                if pred_bps > BURST_THRESHOLD_BPS:
                    self.consecutive_high_preds += 1
                    self.consecutive_low_preds = 0
                    
                    # TRIGGER REROUTE hanya jika:
                    # 1. Belum active
                    # 2. Sudah dapat X prediksi tinggi berturut-turut
                    # 3. Sudah melewati cooldown
                    if (not self.congestion_active and 
                        self.consecutive_high_preds >= self.required_high_for_reroute and
                        (current_time - self.last_reroute_time) > self.cooldown_period):
                        
                        self.logger.warning(
                            f"⚠️ CONGESTION PREDICTED: {pred_bps:.0f} bps "
                            f"(>{BURST_THRESHOLD_BPS}) - REROUTING!"
                        )
                        self.apply_static_reroute(trigger_val=pred_bps)
                        
                elif pred_bps < LOWER_THRESHOLD_BPS:
                    self.consecutive_low_preds += 1
                    self.consecutive_high_preds = 0
                    
                    # TRIGGER REVERT hanya jika:
                    # 1. Sedang active
                    # 2. Sudah dapat Y prediksi rendah berturut-turut
                    # 3. Sudah melewati cooldown
                    if (self.congestion_active and 
                        self.consecutive_low_preds >= self.required_low_for_revert and
                        (current_time - self.last_reroute_time) > self.cooldown_period):
                        
                        self.logger.info(
                            f"✅ TRAFFIC STABLE: {pred_bps:.0f} bps "
                            f"(<{LOWER_THRESHOLD_BPS}) - REVERTING."
                        )
                        self.revert_to_default()
                        
                else:
                    # Reset counters jika di antara threshold
                    self.consecutive_high_preds = 0
                    self.consecutive_low_preds = 0
                    
            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")
                if conn: 
                    conn.close()

    def send_barrier_request(self, datapath):
        """Mencegah Packet-In memproses paket sebelum flow terpasang"""
        parser = datapath.ofproto_parser
        req = parser.OFPBarrierRequest(datapath)
        datapath.send_msg(req)

    def apply_static_reroute(self, trigger_val=0):
        """
        Reroute statis ke Spine 1 - KONSISTEN dengan Packet-In handler
        """
        self.logger.warning("🚀 REROUTING H1 VoIP to Spine 1 (Static)...")
        
        # 1. Hapus semua flow terkait H1<->H2
        self.delete_voip_flows()
        
        # 2. Install flow baru di semua switch di path
        for dpid, out_port in self.REROUTE_PATH.items():
            dp = self.datapaths.get(dpid)
            if dp:
                parser = dp.ofproto_parser
                match = parser.OFPMatch(
                    eth_type=0x0800, 
                    ipv4_src='10.0.0.1', 
                    ipv4_dst='10.0.0.2'
                )
                actions = [parser.OFPActionOutput(out_port)]
                self.add_flow(dp, self.reroute_priority, match, actions, idle_timeout=0)
                
                # Install reverse flow juga
                match_rev = parser.OFPMatch(
                    eth_type=0x0800, 
                    ipv4_src='10.0.0.2', 
                    ipv4_dst='10.0.0.1'
                )
                # Untuk reverse, kita perlu tahu port yang benar
                # Ini disederhanakan - sesuaikan dengan topologi Anda
                if dpid == 5:  # Leaf2
                    rev_port = 2  # Port ke Spine1
                elif dpid == 1:  # Spine1
                    rev_port = 1  # Port ke Leaf1
                elif dpid == 4:  # Leaf1
                    rev_port = 1  # Port ke H1
                    
                actions_rev = [parser.OFPActionOutput(rev_port)]
                self.add_flow(dp, self.reroute_priority, match_rev, actions_rev, idle_timeout=0)
                
                self.logger.info(f"   + Rule Installed: DPID {dpid} -> Port {out_port}")

        # 3. Update state
        self.congestion_active = True
        self.last_reroute_time = time.time()
        self.insert_event_log("REROUTE_ACTIVE", "H1 rerouted to Spine 1", trigger_val)

    def revert_to_default(self):
        """Kembalikan ke path default (Spine 2)"""
        self.logger.warning("🔄 REVERTING H1 VoIP to default path (Spine 2)...")
        
        # 1. Hapus semua flow terkait H1<->H2
        self.delete_voip_flows()
        
        # 2. Install flow default di semua switch
        for dpid, out_port in self.DEFAULT_PATH.items():
            dp = self.datapaths.get(dpid)
            if dp:
                parser = dp.ofproto_parser
                match = parser.OFPMatch(
                    eth_type=0x0800, 
                    ipv4_src='10.0.0.1', 
                    ipv4_dst='10.0.0.2'
                )
                actions = [parser.OFPActionOutput(out_port)]
                self.add_flow(dp, self.default_priority, match, actions, idle_timeout=0)
                
                # Install reverse flow
                match_rev = parser.OFPMatch(
                    eth_type=0x0800, 
                    ipv4_src='10.0.0.2', 
                    ipv4_dst='10.0.0.1'
                )
                # Port reverse - sesuaikan dengan topologi
                if dpid == 5:  # Leaf2
                    rev_port = 2  # Port ke Spine2
                elif dpid == 2:  # Spine2
                    rev_port = 3  # Port ke Leaf1
                elif dpid == 4:  # Leaf1
                    rev_port = 1  # Port ke H1
                    
                actions_rev = [parser.OFPActionOutput(rev_port)]
                self.add_flow(dp, self.default_priority, match_rev, actions_rev, idle_timeout=0)
                
                self.logger.info(f"   + Default Rule: DPID {dpid} -> Port {out_port}")
        
        # 3. Reset state
        self.congestion_active = False
        self.last_reroute_time = time.time()
        self.insert_event_log("REROUTE_REVERT", "H1 VoIP restored to Spine 2", 0)

    def delete_voip_flows(self):
        """Hapus SEMUA flow VoIP (forward & reverse) dan reset cache"""
        self.logger.info("🧹 Cleaning ALL VoIP flows (H1<->H2)...")
        
        # Patterns to delete
        patterns = [
            {'eth_type': 0x0800, 'ipv4_src': '10.0.0.1', 'ipv4_dst': '10.0.0.2'},
            {'eth_type': 0x0800, 'ipv4_src': '10.0.0.2', 'ipv4_dst': '10.0.0.1'}
        ]
        
        for dpid, dp in self.datapaths.items():
            parser = dp.ofproto_parser
            ofp = dp.ofproto
            
            for pattern in patterns:
                match = parser.OFPMatch(**pattern)
                mod = parser.OFPFlowMod(
                    datapath=dp,
                    command=ofp.OFPFC_DELETE,
                    out_port=ofp.OFPP_ANY,
                    out_group=ofp.OFPG_ANY,
                    match=match
                )
                dp.send_msg(mod)
            
            self.send_barrier_request(dp)
        
        # Hapus semua cache terkait H1 dan H2
        keys_to_delete = [
            k for k in self.last_bytes.keys() 
            if '10.0.0.1' in k or '10.0.0.2' in k
        ]
        for k in keys_to_delete:
            del self.last_bytes[k]
            
        self.logger.info(f"   + Cleared {len(keys_to_delete)} stat entries")

    def insert_event_log(self, event_type, description, trigger_value=0):
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
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"DB Insert Error: {e}")

    # =================================================================
    # 3. HANDLERS (PacketIn) - DIBAIKI untuk KONSISTENSI
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                # Auto-init rule untuk Leaf 1 saat konek
                if datapath.id == 4: 
                    self.logger.info("DPID 4 Connected: Install Default Rules.")
                    time.sleep(1)
                    self.revert_to_default() 
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
            mod = parser.OFPFlowMod(
                datapath=datapath, 
                buffer_id=buffer_id, 
                priority=priority, 
                match=match, 
                instructions=inst, 
                idle_timeout=idle_timeout,
                hard_timeout=0
            )
        else:
            mod = parser.OFPFlowMod(
                datapath=datapath, 
                priority=priority, 
                match=match, 
                instructions=inst, 
                idle_timeout=idle_timeout,
                hard_timeout=0
            )
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Packet-In handler yang KONSISTEN dengan flow yang diinstall"""
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP: 
            return
        
        dst = eth.dst
        src = eth.src
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        
        if ip_pkt: 
            self.ip_to_mac[ip_pkt.src] = src

        # =================================================================
        # LOGIKA ROUTING: SIMPLE & KONSISTEN
        # =================================================================
        if ip_pkt and ip_pkt.src == '10.0.0.1' and ip_pkt.dst == '10.0.0.2':
            # VoIP traffic H1->H2
            
            if dpid == 4:  # Leaf1
                if self.congestion_active:
                    # Reroute path: Port 1 ke Spine1
                    out_port = 1
                else:
                    # Default path: Port 3 ke Spine2
                    out_port = 3
                    
                match = parser.OFPMatch(
                    eth_type=0x0800,
                    ipv4_src='10.0.0.1',
                    ipv4_dst='10.0.0.2'
                )
                actions = [parser.OFPActionOutput(out_port)]
                priority = self.reroute_priority if self.congestion_active else self.default_priority
                
                self.add_flow(datapath, priority, match, actions, msg.buffer_id, idle_timeout=60)
                
                # Send packet out
                data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                out = parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=msg.buffer_id,
                    in_port=in_port,
                    actions=actions,
                    data=data
                )
                datapath.send_msg(out)
                return

        # --- ARP HANDLER ---
        if arp_pkt:
            self.handle_arp(datapath, in_port, arp_pkt, src)
            return

        # --- DEFAULT L2 SWITCHING ---
        self.handle_l2_switching(datapath, in_port, dst, src, msg)

    def handle_arp(self, datapath, in_port, arp_pkt, src_mac):
        """Handle ARP packets"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
        
        if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.ip_to_mac:
            target_mac = self.ip_to_mac[arp_pkt.dst_ip]
            e = ethernet.ethernet(
                dst=src_mac,
                src=target_mac,
                ethertype=ether_types.ETH_TYPE_ARP
            )
            a = arp.arp(
                opcode=arp.ARP_REPLY,
                src_mac=target_mac,
                src_ip=arp_pkt.dst_ip,
                dst_mac=src_mac,
                dst_ip=arp_pkt.src_ip
            )
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

    def handle_l2_switching(self, datapath, in_port, dst_mac, src_mac, msg):
        """Handle L2 switching for non-VoIP traffic"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id
        
        # MAC learning
        self.mac_to_port.setdefault(dpid, {})
        
        if dst_mac in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst_mac]
        else:
            out_port = ofproto.OFPP_FLOOD

        # Install flow only if we know the destination
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst_mac, eth_src=src_mac)
            actions = [parser.OFPActionOutput(out_port)]
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=60)

        # Send packet out
        actions = [parser.OFPActionOutput(out_port)]
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
            
        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )
        datapath.send_msg(out)

    # =================================================================
    # 4. MONITORING (DIBAIKI)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in list(self.datapaths.values()):
                if dp.id:
                    try:
                        req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                        dp.send_msg(req)
                    except Exception as e:
                        self.logger.error(f"Flow stats request error: {e}")
            hub.sleep(1)

    def _resolve_ip(self, mac):
        """Resolve IP from MAC address"""
        if not mac: 
            return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: 
                return ip
        return None

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Handle flow stats reply dengan proteksi flapping"""
        # Skip stats jika baru saja reroute (5 detik)
        if time.time() - self.last_reroute_time < 5.0:
            return
            
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()

        for stat in body:
            if stat.priority == 0: 
                continue
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            # Only monitor VoIP and bursty traffic to H2
            if dst_ip != '10.0.0.2': 
                continue
            if src_ip not in ['10.0.0.1', '10.0.0.3']: 
                continue
            
            flow_key = f"{dpid}-{src_ip}-{dst_ip}"
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            # Calculate delta
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            
            # Handle counter reset
            if byte_count < prev_b:
                delta_b = byte_count
                delta_p = packet_count
            else:
                delta_b = byte_count - prev_b
                delta_p = packet_count - prev_p
            
            # Update cache
            self.last_bytes[flow_key] = (byte_count, packet_count)
            
            if delta_b <= 0: 
                continue
            
            # Prepare data for DB
            ip_proto = match.get('ip_proto') or 17
            tp_src = match.get('tcp_src') or match.get('udp_src') or 0
            tp_dst = match.get('tcp_dst') or match.get('udp_dst') or 0
            traffic_label = 'voip' if src_ip == '10.0.0.1' else 'bursty'
            
            # Insert to DB
            conn = self.get_db_conn()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO traffic.flow_stats_
                        (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                        ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                        pkts_tx, pkts_rx, duration_sec, traffic_label)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        timestamp, dpid, src_ip, dst_ip,
                        match.get('eth_src', ''), match.get('eth_dst', ''),
                        ip_proto, tp_src, tp_dst, delta_b, delta_b,
                        delta_p, delta_p, 1.0, traffic_label
                    ))
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as e:
                    self.logger.error(f"DB Insert Error: {e}")
                    if conn:
                        conn.close()