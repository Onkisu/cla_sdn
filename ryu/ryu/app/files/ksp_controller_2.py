#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting + Traffic Monitoring (Final Polish)
- Intelligent K-Shortest Path Rerouting
- Postgres Integration for Forecasting
- Anti-Flapping Mechanism
"""

import time
import json
import random
import math
import networkx as nx
import psycopg2
from datetime import datetime
from operator import attrgetter

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp
from ryu.topology import event, api
from ryu.topology.api import get_switch, get_link

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

# Threshold sesuai forecast_2.py (120kbps)
BURST_THRESHOLD_BPS = 120000
LOWER_THRESHOLD_BPS = 80000  # Hysteresis: Traffic harus turun ke angka ini baru balik
COOLDOWN_PERIOD = 20         # Detik (Waktu tunggu minimal sebelum revert)

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        
        # --- TOPOLOGY & STATE ---
        self.net = nx.DiGraph()       # Graph Topologi
        self.mac_to_port = {}         # Mac Learning Table
        self.ip_to_mac = {}           # ARP Cache Table
        self.datapaths = {}           # Active Switches
        
        # --- TRAFFIC STATS ---
        self.last_bytes = {}          # Untuk menghitung delta throughput
        
        # --- CONGESTION STATE MACHINE ---
        self.congestion_active = False
        self.last_reroute_time = 0
        self.current_path_str = "DEFAULT (Shared)"
        
        # --- THREADS ---
        # 1. Topology Discovery (Untuk NetworkX)
        self.topology_thread = hub.spawn(self._discover_topology_loop)
        # 2. Traffic Monitor (Kirim data ke DB untuk Forecaster)
        self.monitor_thread = hub.spawn(self._monitor_traffic_loop)
        # 3. Forecast Watchdog (Baca DB -> Trigger Reroute)
        self.forecast_thread = hub.spawn(self._forecast_watchdog_loop)

        self.logger.info("🚀 SDN ORCHESTRATOR STARTED: Ready to handle traffic.")

    # =================================================================
    # DATABASE UTILS
    # =================================================================
    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception as e:
            self.logger.error(f"❌ DB Connection Error: {e}")
            return None

    def log_system_event(self, event_type, description, val=0):
        """Mencatat event penting ke tabel system_events"""
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (timestamp, event_type, description, trigger_value)
                VALUES (NOW(), %s, %s, %s)
            """, (event_type, description, val))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"Failed to log event: {e}")

    # =================================================================
    # 1. TOPOLOGY DISCOVERY (NETWORKX)
    # =================================================================
    def _discover_topology_loop(self):
        """Looping untuk update graph networkx secara real-time"""
        while True:
            try:
                # Update Nodes (Switches)
                switches = get_switch(self, None)
                nodes_before = self.net.number_of_nodes()
                self.net.clear()
                
                for s in switches:
                    self.net.add_node(s.dp.id)
                
                # Update Links (Edges)
                links = get_link(self, None)
                for l in links:
                    # Simpan port info di edge attributes agar bisa diambil saat routing
                    self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                    self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

                if self.net.number_of_nodes() > nodes_before:
                    self.logger.info(f"🌐 Topology Updated: {self.net.number_of_nodes()} switches, {self.net.number_of_edges()} links")
            except Exception as e:
                self.logger.error(f"Topology Discovery Error: {e}")
            
            hub.sleep(3)

    # =================================================================
    # 2. FORECAST WATCHDOG & REROUTING LOGIC
    # =================================================================
    def _forecast_watchdog_loop(self):
        """Membaca tabel forecast_1h dan mengambil keputusan routing"""
        self.logger.info("👁️  Forecast Watchdog Active...")
        
        while True:
            hub.sleep(1.0) # Cek setiap 1 detik
            
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                # Ambil prediksi terbaru (paling update)
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                row = cur.fetchone()
                cur.close()
                conn.close()
                
                if not row: continue
                
                predicted_bps = float(row[0])
                now = time.time()
                
                # --- LOGIC REROUTE OTOMATIS ---
                
                # KONDISI 1: DETEKSI BURST (Trigger Reroute)
                if predicted_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️  PREDICTION ALERT: {predicted_bps:,.0f} bps > Threshold. Initiating Reroute...")
                    self._perform_reroute(predicted_bps)
                    
                # KONDISI 2: TRAFFIC NORMAL (Trigger Restore/Revert)
                elif predicted_bps < LOWER_THRESHOLD_BPS and self.congestion_active:
                    # Cek Cooldown (Hysteresis)
                    if (now - self.last_reroute_time) > COOLDOWN_PERIOD:
                        self.logger.info(f"✅ TRAFFIC STABLE: {predicted_bps:,.0f} bps. Restoring Default Path.")
                        self._perform_restore(predicted_bps)
                    else:
                        remaining = int(COOLDOWN_PERIOD - (now - self.last_reroute_time))
                        # Debug log (opsional, bisa dikomentari agar tidak spam)
                        # self.logger.debug(f"⏳ Cooling down... {remaining}s remaining.")

            except Exception as e:
                self.logger.error(f"Watchdog Error: {e}")
                if conn and not conn.closed: conn.close()

    def get_ksp_path(self, src, dst, k=3, avoid_node=None):
        """Mencari K-Shortest Paths menggunakan NetworkX"""
        try:
            all_paths = list(nx.shortest_simple_paths(self.net, src, dst))[:k]
            if not all_paths: return None
            
            # Jika ada node yang harus dihindari (misal Spine 1 macet)
            if avoid_node:
                for p in all_paths:
                    if avoid_node not in p:
                        return p
            
            # Jika tidak ada preferensi, kembalikan path ke-2 (alternatif)
            if len(all_paths) > 1:
                return all_paths[1]
            return all_paths[0]
            
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def _perform_reroute(self, trigger_val):
        """Memindahkan H1->H2 ke jalur alternatif"""
        # H1 ada di Leaf 1 (dpid 4), H2 ada di Leaf 2 (dpid 5)
        src_sw, dst_sw = 4, 5
        
        # Cari jalur yang TIDAK melewati Spine 1 (dpid 1) - Asumsi Spine 1 macet oleh H3
        new_path = self.get_ksp_path(src_sw, dst_sw, k=3, avoid_node=1)
        
        if not new_path:
            self.logger.error("❌ No alternative path found!")
            return

        self.logger.info(f"🔄 REROUTING H1 (VoIP) via: {new_path}")
        
        # Pasang Flow Priority Tinggi (30000) untuk H1->H2
        self._install_path_flow(new_path, "10.0.0.1", "10.0.0.2", priority=30000)
        
        # Update State
        self.congestion_active = True
        self.last_reroute_time = time.time()
        self.current_path_str = str(new_path)
        
        self.log_system_event("REROUTE_ACTIVE", f"Path changed to {new_path}", trigger_val)

    def _perform_restore(self, trigger_val):
        """Mengembalikan H1->H2 ke jalur default"""
        # Kita cukup menghapus flow priority tinggi. 
        # Traffic akan jatuh kembali ke flow priority rendah (default) atau Packet-In.
        
        self.logger.info("🔙 RESTORING H1 to Default Path.")
        
        # Hapus flow spesifik H1->H2 di semua switch
        for dpid in self.datapaths:
            self._delete_flow(self.datapaths[dpid], "10.0.0.1", "10.0.0.2")
            
        # Update State
        self.congestion_active = False
        self.last_reroute_time = time.time()
        self.current_path_str = "DEFAULT (Shared)"
        
        self.log_system_event("PATH_RESTORED", "Traffic normalized", trigger_val)

    def _install_path_flow(self, path, src_ip, dst_ip, priority):
        """Helper untuk install flow di sepanjang jalur path"""
        # Path contoh: [4, 2, 5] (Leaf1 -> Spine2 -> Leaf2)
        
        for i in range(len(path) - 1):
            curr_dpid = path[i]
            next_dpid = path[i+1]
            
            dp = self.datapaths.get(curr_dpid)
            if not dp: continue
            
            # Cari port output dari curr ke next menggunakan graph networkx
            if self.net.has_edge(curr_dpid, next_dpid):
                out_port = self.net[curr_dpid][next_dpid]['port']
                
                parser = dp.ofproto_parser
                match = parser.OFPMatch(
                    eth_type=0x0800, # IPv4
                    ipv4_src=src_ip,
                    ipv4_dst=dst_ip
                )
                actions = [parser.OFPActionOutput(out_port)]
                
                # Install Flow
                self.add_flow(dp, priority, match, actions, idle_timeout=0)

    def _delete_flow(self, datapath, src_ip, dst_ip):
        """Helper untuk menghapus flow VoIP Priority Tinggi"""
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=src_ip,
            ipv4_dst=dst_ip
        )
        
        mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE, # Delete matching flows
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            priority=30000, # Hapus yang priority reroute saja
            match=match
        )
        datapath.send_msg(mod)

    # =================================================================
    # 3. PACKET PROCESSING (CORE SWITCHING)
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.datapaths[datapath.id] = datapath
        
        # Table-miss flow (Send to controller)
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout)
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
        if eth.ethertype == 34525: return # IPv6 Ignore

        dst = eth.dst
        src = eth.src
        
        # --- ARP PROXY (Penting untuk Ping Stabil) ---
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            self._handle_arp(datapath, in_port, pkt)
            return

        # --- IP PACKET HANDLING ---
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # DEFAULT ROUTING LOGIC (Sebelum Reroute Aktif)
            # Kita paksa lewat Spine 1 (Port 1 di Leaf) untuk menciptakan tabrakan
            # Asumsi topologi: Port 1 -> Spine 1, Port 2 -> Spine 2, Port 3 -> Host
            
            # Jika paket dari H1 atau H3, dan tujuan H2, dan belum ada Reroute
            if dpid in [4, 6] and dst_ip == "10.0.0.2":
                # Cek apakah flow reroute sudah handle? Kalau belum, pasang default
                if not self.congestion_active:
                    # Lewatkan Spine 1 (Port 1)
                    # NOTE: Sesuaikan port ini dengan wiring Mininet Anda
                    # Biasanya Leaf Link 1 -> Spine 1
                    actions = [parser.OFPActionOutput(1)] 
                    match = parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
                    self.add_flow(datapath, 10, match, actions, msg.buffer_id, idle_timeout=10)
                    
                    # Kirim paket
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                            in_port=in_port, actions=actions, data=msg.data)
                    datapath.send_msg(out)
                    return

        # --- FALLBACK L2 LEARNING (Jika tidak match logic di atas) ---
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]
        
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _handle_arp(self, datapath, port, pkt):
        """Menangani ARP Request/Reply untuk mengurangi broadcast storm"""
        pkt_arp = pkt.get_protocol(arp.arp)
        src_ip = pkt_arp.src_ip
        src_mac = pkt_arp.src_mac
        dst_ip = pkt_arp.dst_ip

        # Simpan mapping IP->MAC
        self.ip_to_mac[src_ip] = src_mac
        
        # Jika ARP Request
        if pkt_arp.opcode == arp.ARP_REQUEST:
            if dst_ip in self.ip_to_mac:
                # Kita tahu jawabannya! Kirim ARP Reply langsung (Proxy ARP)
                dst_mac = self.ip_to_mac[dst_ip]
                self._send_arp_reply(datapath, port, src_mac, src_ip, dst_mac, dst_ip)
            else:
                # Flood jika tidak tahu
                out = datapath.ofproto_parser.OFPPacketOut(
                    datapath=datapath, buffer_id=datapath.ofproto.OFP_NO_BUFFER,
                    in_port=port, actions=[datapath.ofproto_parser.OFPActionOutput(datapath.ofproto.OFPP_FLOOD)],
                    data=pkt.data)
                datapath.send_msg(out)

    def _send_arp_reply(self, datapath, port, target_mac, target_ip, sender_mac, sender_ip):
        e = ethernet.ethernet(dst=target_mac, src=sender_mac, ethertype=ether_types.ETH_TYPE_ARP)
        a = arp.arp(opcode=arp.ARP_REPLY, src_mac=sender_mac, src_ip=sender_ip,
                    dst_mac=target_mac, dst_ip=target_ip)
        p = packet.Packet()
        p.add_protocol(e)
        p.add_protocol(a)
        p.serialize()
        
        actions = [datapath.ofproto_parser.OFPActionOutput(port)]
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=datapath.ofproto.OFP_NO_BUFFER,
            in_port=datapath.ofproto.OFPP_CONTROLLER, actions=actions, data=p.data)
        datapath.send_msg(out)

    # =================================================================
    # 4. MONITORING LOOP (WRITE TO DB FOR FORECASTER)
    # =================================================================
    def _monitor_traffic_loop(self):
        while True:
            hub.sleep(1)
            for dp in self.datapaths.values():
                self._request_stats(dp)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Menerima statistik flow dan simpan ke DB traffic.flow_stats_"""
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()
        
        conn = self.get_db_conn()
        if not conn: return
        
        try:
            cur = conn.cursor()
            for stat in body:
                if stat.priority == 0: continue # Skip table-miss
                
                # Logic sederhana untuk ambil delta bytes
                # (Sederhana: Kita insert totalnya saja, forecast script akan handle delta nya via SQL window function)
                
                match = stat.match
                src_ip = match.get('ipv4_src')
                dst_ip = match.get('ipv4_dst')
                
                if not src_ip or not dst_ip: continue
                
                # Hanya simpan trafik host-to-host (filter noise)
                if not src_ip.startswith('10.0.0.'): continue

                cur.execute("""
                    INSERT INTO traffic.flow_stats_ 
                    (timestamp, dpid, src_ip, dst_ip, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp, dpid, src_ip, dst_ip,
                    stat.byte_count, stat.byte_count, 
                    stat.packet_count, stat.packet_count, 
                    stat.duration_sec
                ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            # Silent fail agar log tidak penuh error jika DB locking
            if conn: conn.close()

if __name__ == '__main__':
    # Ryu main entry point (tidak dieksekusi langsung sebagai script python biasa)
    pass