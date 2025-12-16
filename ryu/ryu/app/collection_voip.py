import time
import os
import psycopg2
from datetime import datetime
from ryu.app import simple_switch_stp_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub

class VoIPCollectorDB(simple_switch_stp_13.SimpleSwitch13):
    def __init__(self, *args, **kwargs):
        super(VoIPCollectorDB, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)
        
        # Cache untuk perhitungan delta throughput
        # Key: (dpid, src_ip, dst_ip, ip_proto, tp_src, tp_dst)
        self.flow_stats_cache = {}
        
        # Konfigurasi Database
        self.db_config = {
            "host": "103.181.142.165",
            "database": "development",
            "user": "dev_one",
            "password": "hijack332."
        }
        
        # Inisialisasi Koneksi
        self.conn = None
        self.cur = None
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            print("[*] Berhasil terhubung ke Database PostgreSQL.")
        except Exception as e:
            print(f"[!] Gagal terhubung ke Database: {e}")
            self.conn = None

    def insert_to_db(self, data):
        """
        Melakukan INSERT ke tabel traffic.traffic_flow_
        Data tuple urutan: (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
                            ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, 
                            pkts_tx, pkts_rx, duration_sec, throughput_mbps, traffic_label)
        """
        # Cek koneksi, reconnect jika putus
        if not self.conn or self.conn.closed:
            print("[!] Koneksi DB terputus, mencoba reconnect...")
            self.connect_db()
        
        if not self.conn:
            return # Skip jika masih gagal connect

        sql = """
            INSERT INTO traffic.traffic_flow_ 
            (timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
             ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, 
             pkts_tx, pkts_rx, duration_sec, throughput_mbps, traffic_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            self.cur.execute(sql, data)
            self.conn.commit()
            # print(f"[+] Data inserted: {data[2]} -> {data[3]} ({data[15]})") 
        except Exception as e:
            print(f"[!] Error Insert DB: {e}")
            self.conn.rollback()

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)  # Polling interval 1 detik

    def _request_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid_raw = ev.msg.datapath.id
        dpid_str = f"s{dpid_raw}" # Format "s1", "s2", dst
        
        timestamp_curr = datetime.now() # Format Timestamp PostgreSQL
        current_time = time.time()

        # Filter flow yang relevan (IPv4)
        # Urutkan agar pemrosesan konsisten
        sorted_flows = sorted([flow for flow in body if flow.priority > 0],
                              key=lambda flow: (flow.match.get('in_port', 0), flow.match.get('ipv4_dst', '0')))
        
        for stat in sorted_flows:
            match = stat.match
            
            src_ip = match.get('ipv4_src')
            dst_ip = match.get('ipv4_dst')
            
            if not src_ip or not dst_ip:
                continue
                
            src_mac = match.get('eth_src', '')
            dst_mac = match.get('eth_dst', '')
            ip_proto = match.get('ip_proto', 0)
            tp_src = match.get('udp_src', match.get('tcp_src', 0))
            tp_dst = match.get('udp_dst', match.get('tcp_dst', 0))

            # Key unik untuk flow stats cache
            flow_key = (dpid_raw, src_ip, dst_ip, ip_proto, tp_src, tp_dst)
            
            bytes_now = stat.byte_count
            throughput_mbps = 0.0
            
            # Hitung Throughput
            if flow_key in self.flow_stats_cache:
                prev_bytes, prev_time = self.flow_stats_cache[flow_key]
                delta_bytes = bytes_now - prev_bytes
                delta_time = current_time - prev_time
                
                if delta_time > 0 and delta_bytes >= 0:
                    throughput_mbps = (delta_bytes * 8) / (delta_time * 1000000)
            
            self.flow_stats_cache[flow_key] = (bytes_now, current_time)

            # Labeling Logic (Burst vs Normal)
            if throughput_mbps > 0.5:
                traffic_label = "VoIP-Burst"
            elif throughput_mbps > 0.001:
                traffic_label = "VoIP-Normal"
            else:
                traffic_label = "Idle"

            # Hanya insert jika ada throughput atau traffic aktif (opsional)
            if throughput_mbps >= 0:
                # Siapkan data tuple (tanpa ID, biarkan DB auto-increment)
                row_data = (
                    timestamp_curr,
                    dpid_str,
                    src_ip,
                    dst_ip,
                    src_mac,
                    dst_mac,
                    ip_proto,
                    tp_src,
                    tp_dst,
                    stat.byte_count, # bytes_tx
                    stat.byte_count, # bytes_rx (asumsi sama di switch entry)
                    stat.packet_count, # pkts_tx
                    stat.packet_count, # pkts_rx
                    float(stat.duration_sec),
                    float(f"{throughput_mbps:.4f}"),
                    traffic_label
                )
                
                self.insert_to_db(row_data)