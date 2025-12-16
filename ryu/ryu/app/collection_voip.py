from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, tcp, udp
from ryu.lib import hub
import psycopg2
from datetime import datetime
import time

# --- KONFIGURASI DATABASE ---
DB_CONFIG = {
    'dbname': 'development', 
    'user': 'dev_one', 
    'password': 'hijack332.', 
    'host': '103.181.142.165'
}

class IntegratedController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(IntegratedController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)
        self.prev_stats = {} 

    # --- 1. MONITORING ENGINE ---
    def _monitor(self):
        self.logger.info(">>> MONITORING AKTIF. Menunggu Switch...")
        while True:
            try:
                for dp in self.datapaths.values():
                    self._request_stats(dp)
            except Exception as e:
                self.logger.error(f"Monitoring Error (Retrying): {e}")
            hub.sleep(1) # Interval 1 detik

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    # --- 2. STATS PROCESSING (INTI LOGIKA) ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        try:
            body = ev.msg.body
            dpid = f"{ev.msg.datapath.id:016d}"
            timestamp = datetime.now()
            batch_data = []

            for stat in body:
                # Filter hanya L3 Traffic (IPv4)
                if 'ipv4_src' not in stat.match or 'ipv4_dst' not in stat.match:
                    continue

                # --- AMBIL SEMUA PARAMETER ---
                src_ip = stat.match['ipv4_src']
                dst_ip = stat.match['ipv4_dst']
                src_mac = stat.match.get('eth_src', '00:00:00:00:00:00')
                dst_mac = stat.match.get('eth_dst', '00:00:00:00:00:00')
                ip_proto = stat.match.get('ip_proto', 0)
                
                # Deteksi Port (L4)
                tp_src = 0
                tp_dst = 0
                label = "Other"

                if ip_proto == 17: # UDP
                    tp_src = stat.match.get('udp_src', 0)
                    tp_dst = stat.match.get('udp_dst', 0)
                    if tp_dst == 5060: label = "VoIP"
                    else: label = "UDP_General"
                
                elif ip_proto == 6: # TCP
                    tp_src = stat.match.get('tcp_src', 0)
                    tp_dst = stat.match.get('tcp_dst', 0)
                    if tp_dst in [5001, 5201]: label = "Background"
                    else: label = "TCP_General"

                # --- HITUNG THROUGHPUT (DELTA) ---
                # Key unik: DPID + 5-Tuple
                flow_key = (dpid, src_ip, dst_ip, ip_proto, tp_dst)
                
                curr_bytes = stat.byte_count
                curr_pkts = stat.packet_count
                duration_sec = stat.duration_sec
                
                throughput_mbps = 0.0
                bytes_diff = 0
                pkts_diff = 0

                if flow_key in self.prev_stats:
                    prev = self.prev_stats[flow_key]
                    time_diff = time.time() - prev['sys_time']
                    
                    if time_diff > 0.1: # Hindari pembagian 0
                        bytes_diff = curr_bytes - prev['bytes']
                        pkts_diff = curr_pkts - prev['pkts']
                        throughput_mbps = (bytes_diff * 8) / 1000000.0 / time_diff
                        throughput_mbps = max(0.0, throughput_mbps)

                # Update state
                self.prev_stats[flow_key] = {
                    'bytes': curr_bytes,
                    'pkts': curr_pkts,
                    'sys_time': time.time()
                }

                # --- FILTER SIMPAN KE DB ---
                # Hanya simpan jika itu trafik VoIP/Background ATAU ada throughput aktif
                if label in ["VoIP", "Background"] or throughput_mbps > 0:
                    self.logger.info(f"[{label:^10}] {src_ip}->{dst_ip} | {throughput_mbps:.4f} Mbps | {bytes_diff} Bytes")
                    
                    batch_data.append((
                        timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac,
                        ip_proto, tp_src, tp_dst,
                        bytes_diff, bytes_diff, # Asumsi TX=RX di single switch
                        pkts_diff, pkts_diff,
                        1.0, throughput_mbps, label
                    ))

            if batch_data:
                self._save_to_db(batch_data)

        except Exception as e:
            self.logger.error(f"Stats Error: {e}")

    def _save_to_db(self, data):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            sql = """INSERT INTO traffic.flow_stats_
                     (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                      ip_proto, tp_src, tp_dst, 
                      bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
                      duration_sec, throughput_mbps, traffic_label) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cur.executemany(sql, data)
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"DB Insert Error: {e}")

    # --- 3. SWITCH HANDLER (ROUTING) ---
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

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        # FIX: TIMEOUT=0 (Permanen)
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst,
                                idle_timeout=0, hard_timeout=0)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP: return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match_dict = {'in_port': in_port, 'eth_dst': dst, 'eth_src': src}
            
            # PARSING PACKET UNTUK MATCHING LENGKAP (IP, UDP, TCP)
            if eth.ethertype == ether_types.ETH_TYPE_IP:
                ip_pkt = pkt.get_protocol(ipv4.ipv4)
                match_dict['eth_type'] = ether_types.ETH_TYPE_IP
                match_dict['ipv4_src'] = ip_pkt.src
                match_dict['ipv4_dst'] = ip_pkt.dst
                match_dict['ip_proto'] = ip_pkt.proto
                
                if ip_pkt.proto == 17: # UDP
                    u = pkt.get_protocol(udp.udp)
                    if u: 
                        match_dict['udp_src'] = u.src_port
                        match_dict['udp_dst'] = u.dst_port
                elif ip_pkt.proto == 6: # TCP
                    t = pkt.get_protocol(tcp.tcp)
                    if t: 
                        match_dict['tcp_src'] = t.src_port
                        match_dict['tcp_dst'] = t.dst_port
            
            match = parser.OFPMatch(**match_dict)
            self.add_flow(datapath, 10, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)