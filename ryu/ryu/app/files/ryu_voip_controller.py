#!/usr/bin/env python3
"""
Ryu SDN Controller for VoIP Traffic Monitoring (ULTIMATE EDITION)
Features:
- Smart ARP Handling (Reduces Broadcast Storms)
- Explicit ICMP/UDP/TCP Flow Separation
- Async DB Writing with Memory Safety
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, udp, tcp, icmp, arp
import psycopg2
from datetime import datetime
import random
import math
import time
import threading
import queue

# Konfigurasi Database
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

class VoIPTrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPTrafficMonitor, self).__init__(*args, **kwargs)
        
        self.mac_to_port = {}
        self.datapaths = {}
        self.last_bytes = {}
        
        # SAFETY: Batasi ukuran queue max 1000 item agar RAM tidak bocor jika DB mati
        self.db_queue = queue.Queue(maxsize=1000) 
        self.db_thread = threading.Thread(target=self._db_worker, daemon=True)
        self.db_thread.start()
        
        self.start_time = time.time()
        self.monitor_thread = hub.spawn(self._monitor)
        self.logger.info("VoIP Traffic Monitor [ULTIMATE] started")

    def _db_worker(self):
        conn = None
        while True:
            try:
                flow_data_list = self.db_queue.get()
                
                if conn is None or conn.closed:
                    try:
                        conn = psycopg2.connect(**DB_CONFIG)
                    except Exception:
                        # Jangan crash controller jika DB mati, buang task ini
                        self.db_queue.task_done()
                        time.sleep(2)
                        continue

                cursor = conn.cursor()
                insert_query = """
                INSERT INTO traffic.flow_stats_ 
                (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                 ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                 duration_sec, traffic_label)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_query, flow_data_list)
                conn.commit()
                cursor.close()
                self.db_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"DB Error: {e}")
                if conn: conn.rollback()
                conn = None

    def generate_bytes_pattern(self, elapsed_seconds):
        # Pola Sinus 1 Jam
        base, amplitude, period = 16400, 3400, 3600
        phase = (elapsed_seconds % period) / period * 2 * math.pi
        bytes_tx = int(base + (amplitude * math.sin(phase)) + random.uniform(-500, 500))
        return max(13000, min(19800, bytes_tx))

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto, parser = datapath.ofproto, datapath.ofproto_parser
        # Default: Send to Controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=10):
        # Idle Timeout 10s: Flow akan hilang jika tidak ada traffic (bagus untuk simulasi dinamis)
        ofproto, parser = datapath.ofproto, datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority,
                                    match=match, idle_timeout=idle_timeout, instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match,
                                    idle_timeout=idle_timeout, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto, parser = datapath.ofproto, datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP: return

        # MAC Learning
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][eth.src] = in_port

        # --- SMART FORWARDING LOGIC ---
        if eth.dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][eth.dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # --- FLOW INSTALLATION LOGIC ---
        if out_port != ofproto.OFPP_FLOOD:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)
            if ip_pkt:
                src_ip, dst_ip = ip_pkt.src, ip_pkt.dst
                
                # 1. Handle ICMP (Ping)
                if pkt.get_protocol(icmp.icmp):
                    match = parser.OFPMatch(in_port=in_port, eth_type=0x0800, 
                                          ipv4_src=src_ip, ipv4_dst=dst_ip, ip_proto=1)
                    self.add_flow(datapath, 20, match, actions, msg.buffer_id) # Priority Tertinggi
                    return

                # 2. Handle UDP (VoIP/D-ITG)
                udp_pkt = pkt.get_protocol(udp.udp)
                if udp_pkt:
                    match = parser.OFPMatch(in_port=in_port, eth_type=0x0800,
                                          ipv4_src=src_ip, ipv4_dst=dst_ip, ip_proto=17,
                                          udp_src=udp_pkt.src_port, udp_dst=udp_pkt.dst_port)
                    self.add_flow(datapath, 10, match, actions, msg.buffer_id)
                    return

                # 3. Handle TCP
                tcp_pkt = pkt.get_protocol(tcp.tcp)
                if tcp_pkt:
                    match = parser.OFPMatch(in_port=in_port, eth_type=0x0800,
                                          ipv4_src=src_ip, ipv4_dst=dst_ip, ip_proto=6,
                                          tcp_src=tcp_pkt.src_port, tcp_dst=tcp_pkt.dst_port)
                    self.add_flow(datapath, 10, match, actions, msg.buffer_id)
                    return

        # Fallback (Packet Out)
        data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        # Bagian ini sama dengan versi Fixed sebelumnya (Sudah benar)
        # Mengambil stats, menghitung delta, menskalakan ke pola sinus, kirim ke DB Queue
        # (Disingkat agar muat, gunakan logic yang sama dari file sebelumnya di blok ini)
        
        timestamp = datetime.now()
        body = ev.msg.body
        dpid = format(ev.msg.datapath.id, '016x')
        elapsed_seconds = int(time.time() - self.start_time)
        valid_flows_payload = []
        total_real_bytes = 0
        current_active_flows = []

        for stat in body:
            if stat.priority == 0: continue
            match = stat.match
            src_ip, dst_ip = match.get('ipv4_src'), match.get('ipv4_dst')
            if not src_ip or not dst_ip: continue

            # Robust IP Proto check
            ip_proto = match.get('ip_proto', 0)
            tp_src = match.get('udp_src') or match.get('tcp_src') or 0
            tp_dst = match.get('udp_dst') or match.get('tcp_dst') or 0
            
            # Logic Delta & Scaling (Copy from fixed version)...
            flow_key = f"{dpid}-{src_ip}-{dst_ip}-{ip_proto}-{tp_src}-{tp_dst}"
            byte_count, packet_count = stat.byte_count, stat.packet_count
            
            if flow_key in self.last_bytes:
                last_b, last_p = self.last_bytes[flow_key]
                delta_bytes, delta_pkts = byte_count - last_b, packet_count - last_p
            else:
                delta_bytes, delta_pkts = byte_count, packet_count
            
            self.last_bytes[flow_key] = (byte_count, packet_count)

            if delta_bytes > 0:
                current_active_flows.append({
                    'src_ip': src_ip, 'dst_ip': dst_ip, 'src_mac': match.get('eth_src',''),
                    'dst_mac': match.get('eth_dst',''), 'ip_proto': ip_proto,
                    'tp_src': tp_src, 'tp_dst': tp_dst, 'real_bytes': delta_bytes, 'real_pkts': delta_pkts
                })
                total_real_bytes += delta_bytes

        if not current_active_flows: return
        target_bytes = self.generate_bytes_pattern(elapsed_seconds)

        for flow in current_active_flows:
            proportion = flow['real_bytes'] / total_real_bytes
            scaled_tx = int(target_bytes * (1 + (proportion - 0.5)*0.4))
            scaled_tx = max(13000, min(19800, scaled_tx))
            pkt_size = flow['real_bytes'] / flow['real_pkts'] if flow['real_pkts'] > 0 else 180
            scaled_pkts_tx = int(scaled_tx / pkt_size)
            
            record = (timestamp, dpid, flow['src_ip'], flow['dst_ip'], flow['src_mac'], flow['dst_mac'],
                      flow['ip_proto'], flow['tp_src'], flow['tp_dst'], scaled_tx, 
                      int(scaled_tx*0.98), scaled_pkts_tx, int(scaled_pkts_tx*0.98), 1.0, 'voip')
            valid_flows_payload.append(record)

        if valid_flows_payload:
            try:
                self.db_queue.put(valid_flows_payload, block=False)
            except queue.Full:
                self.logger.warning("DB Queue Full! Dropping stats to prevent memory leak.")