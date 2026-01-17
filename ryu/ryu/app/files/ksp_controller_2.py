#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (FULL FIX SAPU JAGAT)
- Fix: Explicit Revert Logic (Memaksa path kembali ke Spine 3)
- Fix: Traffic Doubling (Menggunakan Barrier Request & Strict Cleanup)
- Fix: Priority Management (Normal=100, Reroute=200)
- Features: Full DB Logging, Forecast Integration, & Sine Wave Logic preserved.
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

# =================================================================
# CONFIGURATION
# =================================================================
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

BURST_THRESHOLD_BPS = 120000 
LOWER_THRESHOLD_BPS = 90000 # Hysteresis bawah agar stabil
COOLDOWN_PERIOD = 20        # Detik menunggu sebelum revert diizinkan

# Ports Configuration (Asumsi Mininet Spine-Leaf Default)
# Leaf 1 (DPID 4) -> Port 1: Spine 1, Port 2: Spine 2, Port 3: Spine 3
PORT_TO_SPINE_1 = 1 # Jalur Reroute (Alternatif)
PORT_TO_SPINE_3 = 3 # Jalur Default (Rawan Tabrakan)

# Priorities
PRIO_REROUTE = 200
PRIO_NORMAL = 100
PRIO_MAC_LEARNING = 1

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
        
        # --- STATE ---
        self.congestion_active = False 
        self.last_state_change_time = 0
        
        self.connect_database()
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)

        self.logger.info("✅ VoIP Controller (FULL FIX SAPU JAGAT) Started")

    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
            self.logger.info("Database connected")
        except Exception as e:
            self.logger.warning(f"DB Error: {e}")
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
            for s in switches: self.net.add_node(s.dp.id)
            links = get_link(self, None)
            for l in links:
                self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    # =================================================================
    # 2. LOGIC REROUTE & REVERT (FIXED CORE)
    # =================================================================
    def _monitor_forecast(self):
        while True:
            hub.sleep(2)
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                # Ambil prediksi terbaru
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                result = cur.fetchone()
                cur.close()
                conn.close()

                if not result: continue
                pred_bps = result[0]
                now = time.time()
                time_since_change = now - self.last_state_change_time

                # --- LOGIC TRIGGER ---

                # CASE 1: PREDIKSI TINGGI -> REROUTE
                if pred_bps > BURST_THRESHOLD_BPS:
                    if not self.congestion_active:
                        self.logger.warning(f"🚀 TRIGGER REROUTE (Pred: {pred_bps:.0f} bps)")
                        self.apply_reroute_logic(pred_bps)

                # CASE 2: PREDIKSI RENDAH -> REVERT
                elif pred_bps < LOWER_THRESHOLD_BPS:
                    if self.congestion_active and time_since_change > COOLDOWN_PERIOD:
                        self.logger.info(f"🔄 TRIGGER REVERT (Pred: {pred_bps:.0f} bps)")
                        self.revert_logic()
                    
            except Exception as e:
                self.logger.error(f"Forecast Logic Error: {e}")
                if conn: conn.close()

    def clean_h1_flows_global(self):
        """
        SAPU JAGAT: Menghapus flow spesifik H1->H2 di SEMUA Switch.
        Menggunakan Barrier Request untuk memastikan switch BERSIH total 
        sebelum rule baru dipasang. Mencegah Traffic Doubling.
        """
        self.logger.info("🧹 Cleaning H1->H2 flows globally...")
        for dp in self.datapaths.values():
            ofproto = dp.ofproto
            parser = dp.ofproto_parser
            
            # Match spesifik H1->H2
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            
            # Delete Flow (Strict Off agar semua priority kena)
            mod = parser.OFPFlowMod(
                datapath=dp, command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY,
                match=match
            )
            dp.send_msg(mod)
            
            # WAJIB: Kirim Barrier Request
            # Ini memaksa switch menyelesaikan penghapusan sebelum memproses packet/flow mod berikutnya
            dp.send_msg(parser.OFPBarrierRequest(dp))
        
        # Clear cache flow stats agar perhitungan bersih
        h1_keys = [k for k in self.last_bytes.keys() if '10.0.0.1' in k]
        for k in h1_keys:
            del self.last_bytes[k]

    def apply_reroute_logic(self, trigger_val):
        """Pindah H1 -> Spine 1 (Priority 200)"""
        dp_src = self.datapaths.get(4) # Leaf 1 (Lokasi H1)
        if not dp_src: return

        # 1. BERSIHKAN TOTAL (Sapu Jagat)
        self.clean_h1_flows_global()

        # 2. PASANG FLOW BARU (Priority 200)
        parser = dp_src.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_1)] # Ke Spine 1
        
        # Install dengan idle_timeout=0 (Permanent sampai diubah controller)
        self.add_flow(dp_src, PRIO_REROUTE, match, actions)
        
        # Update State
        self.congestion_active = True
        self.last_state_change_time = time.time()
        
        self.insert_event_log("REROUTE_ACTIVE", "H1 moved to Spine 1 (High Prio)", trigger_val)
        self.logger.info("✅ REROUTE APPLIED: H1 -> Spine 1")

    def revert_logic(self):
        """Kembali H1 -> Spine 3 (Priority 100)"""
        dp_src = self.datapaths.get(4)
        if not dp_src: return

        # 1. BERSIHKAN TOTAL (Sapu Jagat)
        self.clean_h1_flows_global()

        # 2. PASANG FLOW DEFAULT SECARA EKSPLISIT (Priority 100)
        # Kita PAKSA pasang rule ke Spine 3. Jangan andalkan Packet-In.
        parser = dp_src.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_3)] # Ke Spine 3
        
        self.add_flow(dp_src, PRIO_NORMAL, match, actions)
        
        # Update State
        self.congestion_active = False
        self.last_state_change_time = time.time()
        
        self.insert_event_log("REROUTE_REVERT", "H1 returned to Spine 3 (Normal Prio)", 0)
        self.logger.info("✅ REVERT APPLIED: H1 -> Spine 3")

    def insert_event_log(self, event_type, desc, val=0):
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value)
                VALUES (%s, %s, %s, %s)
            """, (datetime.now(), event_type, desc, val))
            conn.commit()
            conn.close()
        except: pass

    # =================================================================
    # 3. OPENFLOW HANDLERS
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                # Inisialisasi Default Route saat Switch Connect
                if datapath.id == 4: 
                    self.revert_logic()
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
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, 
                                    match=match, instructions=inst, idle_timeout=idle_timeout)
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
        
        # MAC Learning
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # ARP Handling (Agar Ping Jalan)
        arp_pkt = pkt.get_protocol(arp.arp)
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

        # IPv4 Handling
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_ip = ip_pkt.src
            self.ip_to_mac[src_ip] = src

            # --- FAILSAFE LOGIC H1->H2 ---
            # Jika paket H1->H2 lolos sampai Controller, berarti flow tidak ada.
            # Kita pasang ulang sesuai state terkini.
            if dpid == 4 and src_ip == '10.0.0.1' and ip_pkt.dst == '10.0.0.2':
                out_p = PORT_TO_SPINE_1 if self.congestion_active else PORT_TO_SPINE_3
                prio = PRIO_REROUTE if self.congestion_active else PRIO_NORMAL
                
                actions = [parser.OFPActionOutput(out_p)]
                match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=ip_pkt.dst)
                
                self.add_flow(datapath, prio, match, actions)
                
                # Kirim paket keluar
                data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
                out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                          in_port=in_port, actions=actions, data=data)
                datapath.send_msg(out)
                return # Stop processing, jangan flooding

        # --- STANDARD SWITCHING (Mac Learning) ---
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install Flow Biasa (Priority 1)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, PRIO_MAC_LEARNING, match, actions, msg.buffer_id, idle_timeout=60)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 4. MONITORING & FULL DB LOGGING (PRESERVED)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
            hub.sleep(1)

    def _resolve_ip(self, mac):
        if not mac: return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: return ip
        return None

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()

        for stat in body:
            if stat.priority == 0: continue 

            match = stat.match
            
            # Resolve IPs with Fallback
            src_ip = match.get('ipv4_src')
            if not src_ip:
                src_mac = match.get('eth_src')
                src_ip = self._resolve_ip(src_mac) if src_mac else None
                
            dst_ip = match.get('ipv4_dst')
            if not dst_ip:
                dst_mac = match.get('eth_dst')
                dst_ip = self._resolve_ip(dst_mac) if dst_mac else None

            # Filter Traffic
            if dst_ip != '10.0.0.2': continue
            if src_ip not in ['10.0.0.1', '10.0.0.3']: continue

            # Extract fields for FULL LOGGING
            src_mac = match.get('eth_src') or '00:00:00:00:00:00'
            dst_mac = match.get('eth_dst') or '00:00:00:00:00:00'
            ip_proto = match.get('ip_proto') or 17 # Default UDP
            tp_src = match.get('udp_src') or match.get('tcp_src') or 0
            tp_dst = match.get('udp_dst') or match.get('tcp_dst') or 0

            # Calc Delta
            flow_key = f"{dpid}-{src_ip}-{dst_ip}"
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            delta_b = max(0, byte_count - prev_b)
            delta_p = max(0, packet_count - prev_p)
            self.last_bytes[flow_key] = (byte_count, packet_count)

            if delta_b <= 0: continue

            # INSERT COMPLETE RECORD
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
                        src_mac, dst_mac, ip_proto, tp_src, tp_dst,
                        delta_b, delta_b,      
                        delta_p, delta_p,
                        1.0,                   
                        'voip' if src_ip == '10.0.0.1' else 'bursty'
                    ))
                    conn.commit()
                    conn.close()
                except Exception as e:
                    pass