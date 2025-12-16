from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, arp, tcp, udp
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

class SimpleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        # Thread untuk monitoring
        self.monitor_thread = hub.spawn(self._monitor)
        # Simpan statistik sebelumnya untuk hitung throughput (Delta)
        self.prev_stats = {} 

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.info('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.info('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    def _monitor(self):
        """Thread ini berjalan terus menerus tiap 1 detik"""
        self.logger.info("MONITORING THREAD STARTED...")
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1) # Interval 1 detik

    def _request_stats(self, datapath):
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """Menerima balasan statistik dari Switch"""
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()
        
        batch_data = []

        # Filter Flow yang relevan (Hanya trafik Host-to-Host)
        for stat in body:
            # Kita butuh flow yang punya Match IP (IPv4)
            if 'ipv4_src' not in stat.match or 'ipv4_dst' not in stat.match:
                continue

            src_ip = stat.match['ipv4_src']
            dst_ip = stat.match['ipv4_dst']
            ip_proto = stat.match.get('ip_proto', 0)
            
            # Tentukan Label & Port
            label = "Other"
            tp_dst = 0
            
            # Deteksi VoIP (UDP 5060)
            if ip_proto == 17: # UDP
                tp_dst = stat.match.get('udp_dst', 0)
                if tp_dst == 5060:
                    label = "VoIP"
                else:
                    label = "UDP_Traffic"
            
            # Deteksi Background (TCP 5001 - Iperf)
            elif ip_proto == 6: # TCP
                tp_dst = stat.match.get('tcp_dst', 0)
                if tp_dst == 5001:
                    label = "Background"
                else:
                    label = "TCP_Traffic"
            
            # KUNCI: Hitung Throughput (Mbps)
            # Kita butuh Key unik untuk tracking flow ini
            flow_key = (dpid, src_ip, dst_ip, ip_proto, tp_dst)
            
            curr_bytes = stat.byte_count
            curr_pkts = stat.packet_count
            duration_sec = stat.duration_sec + (stat.duration_nsec / 1e9)
            
            throughput_mbps = 0.0
            bytes_diff = 0
            pkts_diff = 0
            
            if flow_key in self.prev_stats:
                prev = self.prev_stats[flow_key]
                time_diff = time.time() - prev['sys_time']
                
                if time_diff > 0:
                    bytes_diff = curr_bytes - prev['bytes']
                    pkts_diff = curr_pkts - prev['pkts']
                    # Rumus Mbps: (Bytes * 8) / 1juta / detik
                    throughput_mbps = (bytes_diff * 8) / 1000000.0 / time_diff
                    throughput_mbps = max(0.0, throughput_mbps)

            # Update prev stats
            self.prev_stats[flow_key] = {
                'bytes': curr_bytes,
                'pkts': curr_pkts,
                'sys_time': time.time()
            }

            # Hanya simpan jika ada throughput atau ini flow penting
            if throughput_mbps > 0.001 or label == "VoIP":
                # Print log di terminal Controller agar terlihat
                self.logger.info(f"[{label}] {src_ip}->{dst_ip} | {throughput_mbps:.2f} Mbps")

                # Data untuk DB
                batch_data.append((
                    timestamp, str(dpid), src_ip, dst_ip, 
                    "00:00:00:00:00:00", "00:00:00:00:00:00", # MAC (opsional di controller layer stat)
                    ip_proto, 0, tp_dst, 
                    bytes_diff, bytes_diff, # Asumsi TX=RX di stat switch
                    pkts_diff, pkts_diff, 
                    1.0, throughput_mbps, label
                ))

        # Insert ke DB
        if batch_data:
            self._save_to_db(batch_data)

    def _save_to_db(self, data):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            # Pastikan nama tabel benar: traffic.flow_stats_
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
            self.logger.error(f"DB Error: {e}")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
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
        # ... (Kode Packet In / Routing standar Anda, ARP handling, dll) ...
        # Saya singkat bagian ini agar fokus ke monitoring, 
        # TAPI pastikan logika L2/L3 forwarding Anda tetap ada di sini
        # agar pingall berhasil.
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

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

        # Install Flow jika bukan Flood
        if out_port != ofproto.OFPP_FLOOD:
            # Match lengkap (IP/TCP/UDP) agar statistik akurat
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            
            # Cek IP Packet untuk detail flow
            if eth.ethertype == ether_types.ETH_TYPE_IP:
                ip_pkt = pkt.get_protocol(ipv4.ipv4)
                if ip_pkt:
                    match_dict = {
                        'eth_type': ether_types.ETH_TYPE_IP, 
                        'ipv4_src': ip_pkt.src, 
                        'ipv4_dst': ip_pkt.dst,
                        'ip_proto': ip_pkt.proto
                    }
                    # Cek UDP/TCP Port
                    if ip_pkt.proto == 17: # UDP
                        u = pkt.get_protocol(udp.udp)
                        if u: match_dict['udp_dst'] = u.dst_port
                    elif ip_pkt.proto == 6: # TCP
                        t = pkt.get_protocol(tcp.tcp)
                        if t: match_dict['tcp_dst'] = t.dst_port
                    
                    match = parser.OFPMatch(**match_dict)
            
            self.add_flow(datapath, 10, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)