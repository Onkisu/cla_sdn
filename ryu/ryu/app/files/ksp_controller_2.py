#!/usr/bin/env python3
"""
Ryu SDN Controller: VoIP KSP Rerouting (RESTORATION VERSION)
- Connectivity: FULL (ARP, ICMP/Ping, L2 Switching restored from original)
- Reroute: FIXED (Priority 30000, Anti-Loop logic)
- Logging: REAL DATA (No Synthetic/Fake insertions)
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, arp, icmp
from ryu.topology import event, api
from ryu.topology.api import get_switch, get_link
import networkx as nx
import psycopg2
from datetime import datetime
import random
import time
import json

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

BURST_THRESHOLD_BPS = 120000 

class VoIPSmartController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPSmartController, self).__init__(*args, **kwargs)
        
        # --- TOPOLOGY & STATE ---
        self.mac_to_port = {}
        self.ip_to_mac = {}
        self.datapaths = {}
        self.net = nx.DiGraph()
        
        # --- TRAFFIC STATS ---
        self.last_bytes = {}
        self.flow_stats = {}
        
        # --- REROUTE STATE ---
        self.congestion_active = False
        self.last_congestion_time = 0
        self.cooldown_period = 30
        self.LOWER_THRESHOLD_BPS = 90000
        
        # --- THREADS ---
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)
        self.topology_thread = hub.spawn(self._discover_topology)
        
        self.logger.info("✅ RESTORED CONTROLLER: Ping & ARP Logic is BACK.")

    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except Exception as e:
            self.logger.error(f"DB Connection Error: {e}")
            return None

    # =================================================================
    # 1. TOPOLOGY DISCOVERY (KEEP ALIVE)
    # =================================================================
    def _discover_topology(self):
        while True:
            hub.sleep(5)
            # Membersihkan graph agar tidak stale
            self.net.clear()
            
            # Add Switches
            switches = get_switch(self, None)
            for s in switches:
                self.net.add_node(s.dp.id)
            
            # Add Links
            links = get_link(self, None)
            for l in links:
                self.net.add_edge(l.src.dpid, l.dst.dpid, port=l.src.port_no)
                self.net.add_edge(l.dst.dpid, l.src.dpid, port=l.dst.port_no)

    def get_k_shortest_paths(self, src, dst, k=3):
        try:
            return list(nx.shortest_simple_paths(self.net, src, dst))[0:k]
        except nx.NetworkXNoPath:
            return []

    # =================================================================
    # 2. PACKET HANDLING (RESTORED FROM KSP_CONTROLLER_2.PY)
    #    Ini yang bikin PING JALAN!
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        self.datapaths[datapath.id] = datapath

        # Install Table-miss flow entry (Default Flood)
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        self.logger.info(f"Switch {datapath.id} Connected.")

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
        
        # Ignore LLDP
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        dst = eth.dst
        src = eth.src

        # Learn MAC
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # --- ARP HANDLING (PENTING UNTUK PING) ---
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self.ip_to_mac[arp_pkt.src_ip] = arp_pkt.src_mac
            # Jika kita tahu tujuannya, kita bisa proxy ARP reply (Opsional)
            # Tapi untuk aman, biarkan flood/switch logic bekerja

        # --- IP SNOOPING ---
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            self.ip_to_mac[ip_pkt.src] = src

        # --- L2 SWITCHING LOGIC ---
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install Flow jika bukan flood (agar switch pintar)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            # Priority 1 (L2 Normal)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id, idle_timeout=10)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # 3. TRAFFIC MONITORING & REAL DB INSERT
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                dp.send_msg(req)
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = format(ev.msg.datapath.id, '016x')
        timestamp = datetime.now()
        conn = self.get_db_conn()

        for stat in body:
            # Skip low priority (miss flows)
            if stat.priority == 0: continue

            # Extract fields
            match = stat.match
            src_ip = match.get('ipv4_src')
            dst_ip = match.get('ipv4_dst')
            
            # PENTING: Jika flow L2 (tidak ada match IP), coba resolve dari MAC
            if not src_ip:
                src_ip = self._resolve_ip(match.get('eth_src'))
            if not dst_ip:
                dst_ip = self._resolve_ip(match.get('eth_dst'))

            # Jika masih tidak ada IP, skip (karena DB butuh IP)
            if not src_ip or not dst_ip: continue

            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            # Calculate Delta
            flow_key = f"{dpid}-{src_ip}-{dst_ip}-{stat.priority}"
            if flow_key in self.last_bytes:
                last_b, last_p = self.last_bytes[flow_key]
                delta_bytes = max(0, byte_count - last_b)
                delta_pkts = max(0, packet_count - last_p)
            else:
                delta_bytes = byte_count
                delta_pkts = packet_count
            
            self.last_bytes[flow_key] = (byte_count, packet_count)

            # Hanya insert jika ada traffic traffic signifikan
            if delta_bytes > 0 and conn:
                try:
                    cur = conn.cursor()
                    # LOGIC INSERT REAL (BUKAN SYNTHETIC)
                    cur.execute("""
                        INSERT INTO traffic.flow_stats_
                        (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                        ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx,
                        pkts_tx, pkts_rx, duration_sec, traffic_label)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        timestamp, dpid, src_ip, dst_ip,
                        match.get('eth_src'), match.get('eth_dst'),
                        match.get('ip_proto', 0), 0, 0, # Port 0 jika tidak match L4
                        delta_bytes, delta_bytes,
                        delta_pkts, delta_pkts,
                        1.0,
                        'voip' if src_ip == '10.0.0.1' else 'bursty'
                    ))
                    conn.commit()
                    cur.close()
                except Exception as e:
                    # self.logger.error(f"DB Insert Error: {e}")
                    pass
        
        if conn: conn.close()

    def _resolve_ip(self, mac):
        if not mac: return None
        for ip, m in self.ip_to_mac.items():
            if m == mac: return ip
        return None

    # =================================================================
    # 4. FORECAST MONITOR & REROUTE (FIXED LOGIC)
    # =================================================================
    def _monitor_forecast(self):
        """Membaca hasil prediksi dari tabel forecast_1h"""
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
                current_time = time.time()

                # LOGIC TRIGGER
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    self.logger.warning(f"⚠️  PREDICTION ALERT: {pred_bps:.0f} bps -> Triggering REROUTE!")
                    self.apply_ksp_reroute(k=3, trigger_val=pred_bps)
                    self.congestion_active = True
                    self.last_congestion_time = current_time

                elif self.congestion_active:
                    time_since = current_time - self.last_congestion_time
                    if pred_bps < self.LOWER_THRESHOLD_BPS and time_since > self.cooldown_period:
                        self.logger.info(f"✅ TRAFFIC STABLE: {pred_bps:.0f} bps -> REVERTING to Normal.")
                        self.revert_routing()
                        self.congestion_active = False
            except Exception as e:
                self.logger.error(f"Forecast Monitor Error: {e}")

    # --- FIX: REROUTE DENGAN ANTI-LOOP & PRIORITY TINGGI ---
    def apply_ksp_reroute(self, k=3, trigger_val=0):
        src_sw = 4  # Leaf 1 (Source VoIP)
        dst_sw = 5  # Leaf 2 (Dest VoIP)
        src_ip, dst_ip = '10.0.0.1', '10.0.0.2'
        
        # Cari jalur alternatif (path ke-3)
        paths = self.get_k_shortest_paths(src_sw, dst_sw, k=k)
        if len(paths) < k:
            self.logger.error("No alternative path found!")
            return

        new_route = paths[k-1] # Ambil path ke-3
        self.logger.info(f" >>> APPLING REROUTE: {new_route}")

        PRIO_REROUTE = 30000 # Priority tertinggi agar override normal L2

        # 1. Install Rule di Sepanjang Jalur Baru
        for i in range(len(new_route) - 1):
            curr_dpid = new_route[i]
            next_dpid = new_route[i+1]
            
            if self.net.has_edge(curr_dpid, next_dpid):
                out_port = self.net[curr_dpid][next_dpid]['port']
                dp = self.datapaths.get(curr_dpid)
                if dp:
                    # Match spesifik VoIP (UDP src -> dst)
                    self.add_flow_explicit(dp, PRIO_REROUTE, src_ip, dst_ip, out_port)

        # 2. ANTI-LOOP DI TUJUAN (DPID 5)
        # Pastikan saat sampai di switch terakhir, dia ke HOST, bukan balik ke Spine
        dp_dst = self.datapaths.get(dst_sw)
        if dp_dst:
            # Hardcode atau cari port ke H2 (biasanya port 1 di topologi Anda)
            # Atau lookup mac table
            h2_mac = self.ip_to_mac.get(dst_ip)
            out_port_host = self.mac_to_port.get(dst_sw, {}).get(h2_mac, 1) # Default port 1
            
            self.add_flow_explicit(dp_dst, PRIO_REROUTE, src_ip, dst_ip, out_port_host)
            self.logger.info(f" >>> LOCKED DPID {dst_sw} to Port {out_port_host}")

        self.insert_event_log("REROUTE_ACTIVE", json.dumps(new_route), trigger_val)

    def revert_routing(self):
        """Menghapus flow Reroute (Priority 30000) agar kembali ke L2 Switching Normal"""
        self.logger.info("🔄 REVERTING ROUTE...")
        src_ip, dst_ip = '10.0.0.1', '10.0.0.2'
        PRIO_REROUTE = 30000
        
        # Hapus flow prioritas tinggi di semua switch
        for dpid, dp in self.datapaths.items():
            self.del_flow(dp, PRIO_REROUTE, src_ip, dst_ip)
            
        self.insert_event_log("REROUTE_REVERT", "Back to Normal L2", 0)

    # --- HELPER FLOW OPS ---
    def add_flow_explicit(self, datapath, priority, src_ip, dst_ip, out_port):
        parser = datapath.ofproto_parser
        # Match UDP traffic VoIP
        match = parser.OFPMatch(eth_type=0x0800, ip_proto=17, ipv4_src=src_ip, ipv4_dst=dst_ip)
        actions = [parser.OFPActionOutput(out_port)]
        self.add_flow(datapath, priority, match, actions, idle_timeout=0)

    def del_flow(self, datapath, priority, src_ip, dst_ip):
        ofp = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(eth_type=0x0800, ip_proto=17, ipv4_src=src_ip, ipv4_dst=dst_ip)
        mod = parser.OFPFlowMod(
            datapath=datapath, command=ofp.OFPFC_DELETE_STRICT,
            out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
            priority=priority, match=match
        )
        datapath.send_msg(mod)

    def insert_event_log(self, event_type, description, trigger_value=0):
        conn = self.get_db_conn()
        if not conn: return
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
        except: pass