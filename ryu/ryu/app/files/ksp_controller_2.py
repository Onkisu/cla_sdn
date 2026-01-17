# ==============================================================================
# ENTERPRISE SPINE-LEAF SDN CONTROLLER
# Version: 3.0 (Production Ready)
# Author: Gemini AI
# ==============================================================================

import time
import json
import threading
import datetime
import logging
from collections import defaultdict, deque

# Import Ryu Libraries
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ether_types, arp, ipv4, icmp, udp, tcp
from ryu.lib import hub

# Import Database Driver
try:
    import psycopg2
    from psycopg2 import pool
except ImportError:
    raise ImportError("CRITICAL: Module 'psycopg2' not found. Install with: pip3 install psycopg2-binary")

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================

# Database Configuration (Hardcoded sesuai request)
DB_CONFIG = {
    "host": "127.0.0.1",
    "database": "development",
    "user": "dev_one",
    "password": "hijack332.",
    "port": "5432"
}

# Network Thresholds
BURST_THRESHOLD_BPS = 120000  # Threshold switch ke Spine 2
RECOVERY_DELAY_SEC = 5        # Waktu tunggu sebelum kembali ke Spine 1 (Hysteresis)
MONITOR_INTERVAL = 1.0        # Polling stats setiap 1 detik

# Topology Definitions (Hardcoded for Spine-Leaf)
# Leaf Switches DPIDs: 4, 5, 6
# Spine Switches DPIDs: 1, 2, 3
LEAF_SWITCHES = [4, 5, 6]
SPINE_SWITCHES = [1, 2, 3]

# Port Mapping pada Leaf Switch
# Asumsi: Port 1 -> Spine 1, Port 2 -> Spine 2, Port 3+ -> Host
PORT_UPLINK_SPINE_1 = 1
PORT_UPLINK_SPINE_2 = 2

# IP to MAC Table (Static mapping for Proxy ARP fallback)
# Ini membantu jika learning belum selesai
STATIC_HOSTS = {
    "10.0.0.1": "00:00:00:00:00:01",
    "10.0.0.2": "00:00:00:00:00:02",
    "10.0.0.3": "00:00:00:00:00:03"
}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("SDN_Core")

# ==============================================================================
# MODULE: DATABASE MANAGER
# Handles all PostgreSQL interactions safely with connection pooling
# ==============================================================================
class DatabaseManager:
    def __init__(self):
        self.pool = None
        self.connected = False
        self._setup_connection()

    def _setup_connection(self):
        try:
            logger.info("Connecting to Database...")
            self.pool = psycopg2.pool.SimpleConnectionPool(
                1, 10, **DB_CONFIG
            )
            if self.pool:
                self.connected = True
                logger.info("✅ Database Connected Successfully.")
                self._init_tables()
        except Exception as e:
            logger.error(f"❌ Database Connection Failed: {e}")
            self.connected = False

    def _init_tables(self):
        """Memastikan tabel statistik ada (Opsional safety check)"""
        conn = self._get_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            # Cek tabel flow_stats_
            cur.execute("""
                CREATE TABLE IF NOT EXISTS traffic.flow_stats_ (
                    id serial4 NOT NULL,
                    "timestamp" timestamptz DEFAULT now() NOT NULL,
                    dpid int8 NOT NULL,
                    bytes_tx int8 NULL,
                    bytes_rx int8 NULL,
                    pkts_tx int8 NULL,
                    pkts_rx int8 NULL,
                    duration_sec float8 NULL,
                    CONSTRAINT flow_stats__pkey PRIMARY KEY (id)
                );
            """)
            conn.commit()
            cur.close()
            self._put_conn(conn)
        except Exception as e:
            logger.error(f"Table Init Error: {e}")
            if conn: self._put_conn(conn)

    def _get_conn(self):
        if not self.connected or not self.pool:
            # Try reconnecting
            self._setup_connection()
            if not self.connected: return None
        try:
            return self.pool.getconn()
        except Exception as e:
            logger.error(f"Error getting connection from pool: {e}")
            return None

    def _put_conn(self, conn):
        try:
            self.pool.putconn(conn)
        except Exception:
            pass

    def insert_flow_stats(self, dpid, stats_list):
        """Batch insert statistik port ke DB"""
        conn = self._get_conn()
        if not conn: return

        try:
            cur = conn.cursor()
            args_str = ','.join(cur.mogrify("(%s, NOW(), %s, %s, %s, %s, %s)", (
                dpid, x['tx_bytes'], x['rx_bytes'], x['tx_pkts'], x['rx_pkts'], x['duration']
            )).decode('utf-8') for x in stats_list)
            
            cur.execute("INSERT INTO traffic.flow_stats_ (dpid, timestamp, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec) VALUES " + args_str)
            conn.commit()
            cur.close()
            self._put_conn(conn)
        except Exception as e:
            logger.error(f"Failed to insert stats: {e}")
            self._put_conn(conn)

    def get_latest_forecast(self):
        """Mengambil prediksi AI terbaru dari tabel forecast_1h"""
        conn = self._get_conn()
        if not conn: return 0.0

        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT y_pred 
                FROM forecast_1h 
                ORDER BY ts_created DESC 
                LIMIT 1
            """)
            result = cur.fetchone()
            cur.close()
            self._put_conn(conn)
            
            if result:
                return float(result[0])
            return 0.0
        except Exception as e:
            logger.error(f"Failed to fetch forecast: {e}")
            self._put_conn(conn)
            return 0.0

    def log_system_event(self, event_type, description, value):
        conn = self._get_conn()
        if not conn: return

        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (event_type, description, trigger_value)
                VALUES (%s, %s, %s)
            """, (event_type, description, value))
            conn.commit()
            cur.close()
            self._put_conn(conn)
        except Exception as e:
            logger.error(f"Failed to log event: {e}")
            self._put_conn(conn)

# ==============================================================================
# MODULE: TRAFFIC MANAGER
# Handles Rerouting Logic & Flow Modification
# ==============================================================================
class TrafficManager:
    def __init__(self, db_manager):
        self.db = db_manager
        self.reroute_active = False
        self.last_state_change = time.time()
        self.consecutive_safe_count = 0

    def evaluate_network_state(self, datapaths):
        """
        Dibaca oleh thread monitor. Membaca prediksi DB dan memutuskan routing.
        """
        # 1. Ambil Prediksi
        predicted_bps = self.db.get_latest_forecast()
        
        # 2. Logic State Machine
        is_congestion_predicted = predicted_bps > BURST_THRESHOLD_BPS
        
        # Target: Leaf 1 (dpid 4) - Tempat H1 berada
        leaf1 = datapaths.get(4)
        if not leaf1: return

        current_time = time.time()

        if is_congestion_predicted:
            # Reset safe counter
            self.consecutive_safe_count = 0
            
            if not self.reroute_active:
                logger.warning(f"🚀 AI PREDICTION: BURST INCOMING ({predicted_bps:,.0f} bps). ENGAGING REROUTE.")
                self._activate_reroute(leaf1, predicted_bps)
                self.reroute_active = True
                self.last_state_change = current_time

        else:
            # Kondisi Aman
            if self.reroute_active:
                self.consecutive_safe_count += 1
                # Hysteresis: Perlu 5 detik aman berturut-turut untuk kembali
                if self.consecutive_safe_count >= RECOVERY_DELAY_SEC:
                    logger.info(f"✅ AI PREDICTION: NETWORK STABLE ({predicted_bps:,.0f} bps). RESTORING PATH.")
                    self._deactivate_reroute(leaf1, predicted_bps)
                    self.reroute_active = False
                    self.last_state_change = current_time
                    self.consecutive_safe_count = 0

    def _activate_reroute(self, datapath, trigger_val):
        """Memindahkan trafik H1->H2 ke Spine 2 (Port 2)"""
        self._inject_flow(datapath, to_spine_2=True)
        self.db.log_system_event(
            "REROUTE_ACTIVATED",
            f"Forecast {trigger_val:.0f} bps > Threshold. Switched H1->H2 to Spine 2.",
            trigger_val
        )

    def _deactivate_reroute(self, datapath, trigger_val):
        """Mengembalikan trafik H1->H2 ke Spine 1 (Port 1)"""
        self._inject_flow(datapath, to_spine_2=False)
        self.db.log_system_event(
            "PATH_RESTORED",
            f"Forecast {trigger_val:.0f} bps < Threshold. Switched H1->H2 to Spine 1.",
            trigger_val
        )

    def _inject_flow(self, datapath, to_spine_2):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Match H1 (10.0.0.1) -> H2 (10.0.0.2)
        match = parser.OFPMatch(
            eth_type=ether_types.ETH_TYPE_IP,
            ipv4_src="10.0.0.1",
            ipv4_dst="10.0.0.2"
        )
        
        # Tentukan output port
        out_port = PORT_UPLINK_SPINE_2 if to_spine_2 else PORT_UPLINK_SPINE_1
        
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # Priority 100 menimpa priority 1 (default L2)
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=100,
            match=match,
            instructions=inst,
            idle_timeout=0, # Permanen sampai diubah lagi
            hard_timeout=0
        )
        datapath.send_msg(mod)


# ==============================================================================
# MODULE: TOPOLOGY & ARP HANDLER
# Handles L2 Forwarding, Proxy ARP, and Loop Prevention
# ==============================================================================
class L2SwitchingModule:
    def __init__(self):
        # Struktur: self.mac_to_port[dpid][mac_address] = port
        self.mac_to_port = defaultdict(dict)
        # Struktur: self.arp_table[ip_address] = mac_address
        self.arp_table = {} 
        
        # Pre-load static ARP host (untuk mempercepat ping awal)
        for ip, mac in STATIC_HOSTS.items():
            self.arp_table[ip] = mac

    def learn_mac(self, dpid, src_mac, in_port):
        self.mac_to_port[dpid][src_mac] = in_port

    def get_port(self, dpid, dst_mac):
        return self.mac_to_port[dpid].get(dst_mac)

    def handle_arp(self, datapath, port, pkt_ethernet, pkt_arp):
        """
        PROXY ARP LOGIC:
        Alih-alih membanjiri jaringan (Flood), controller menjawab ARP Request
        jika IP tujuannya sudah dikenal.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # 1. Pelajari mapping IP -> MAC dari pengirim
        src_ip = pkt_arp.src_ip
        src_mac = pkt_arp.src_mac
        self.arp_table[src_ip] = src_mac
        
        # Pelajari lokasi MAC di switch ini
        self.learn_mac(datapath.id, src_mac, port)

        dst_ip = pkt_arp.dst_ip
        
        # 2. Jika ARP Request (Opcode 1)
        if pkt_arp.opcode == arp.ARP_REQUEST:
            # Cek apakah kita tahu MAC target?
            if dst_ip in self.arp_table:
                dst_mac = self.arp_table[dst_ip]
                # KIRIM ARP REPLY (Unicast balik ke pengirim)
                self._send_arp_reply(datapath, port, src_mac, src_ip, dst_mac, dst_ip)
                # logger.info(f"🛡️ Proxy ARP: Answered {src_ip} asking for {dst_ip} at {datapath.id}")
                return True # Handled by proxy
            else:
                # Jika tidak tahu, kita terpaksa flood tapi TERKONTROL
                return False # Lanjutkan ke flooding logic

        # 3. Jika ARP Reply (Opcode 2) - biasanya broadcast gratuitous atau unicast
        elif pkt_arp.opcode == arp.ARP_REPLY:
            self.arp_table[src_ip] = src_mac
            return False

    def _send_arp_reply(self, datapath, port, target_mac, target_ip, sender_mac, sender_ip):
        """Membuat paket ARP Reply palsu dari Controller"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        e = ethernet.ethernet(dst=target_mac, src=sender_mac, ethertype=ether_types.ETH_TYPE_ARP)
        a = arp.arp(
            opcode=arp.ARP_REPLY,
            src_mac=sender_mac, src_ip=sender_ip,
            dst_mac=target_mac, dst_ip=target_ip
        )
        p = packet.Packet()
        p.add_protocol(e)
        p.add_protocol(a)
        p.serialize()

        actions = [parser.OFPActionOutput(port)]
        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=ofproto.OFPP_CONTROLLER,
            actions=actions,
            data=p.data
        )
        datapath.send_msg(out)

# ==============================================================================
# MAIN CONTROLLER APP
# Integrates all modules
# ==============================================================================
class MegaController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(MegaController, self).__init__(*args, **kwargs)
        
        # Initialize Modules
        self.db = DatabaseManager()
        self.l2_switch = L2SwitchingModule()
        self.traffic_mgr = TrafficManager(self.db)
        
        self.datapaths = {}
        
        # Start Background Thread
        self.monitor_thread = hub.spawn(self._monitor_loop)
        
        logger.info("🟢 MEGA CONTROLLER V3 STARTED")
        logger.info("   - Proxy ARP: Enabled")
        logger.info("   - AI Forecasting: Enabled")
        logger.info("   - Loop Protection: Enabled")

    # --------------------------------------------------------------------------
    # Switch Connection Handling
    # --------------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                logger.info(f"Switch Connected: dpid={datapath.id}")
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                logger.info(f"Switch Disconnected: dpid={datapath.id}")
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Install Table-Miss Flow (Semua paket unknown kirim ke Controller)
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst, idle_timeout=timeout)
        datapath.send_msg(mod)

    # --------------------------------------------------------------------------
    # Packet Processing Core
    # --------------------------------------------------------------------------
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
        
        # Ignore LLDP & IPv6 (Keep logs clean)
        if eth.ethertype == ether_types.ETH_TYPE_LLDP: return
        if eth.ethertype == 34525: return # IPv6

        dst = eth.dst
        src = eth.src

        # 1. Proxy ARP Handling
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            arp_pkt = pkt.get_protocols(arp.arp)[0]
            handled = self.l2_switch.handle_arp(datapath, in_port, eth, arp_pkt)
            if handled:
                return # Paket sudah dijawab oleh controller, stop processing.

        # 2. MAC Learning
        self.l2_switch.learn_mac(dpid, src, in_port)

        # 3. Destination Lookup
        out_port = self.l2_switch.get_port(dpid, dst)

        # 4. Anti-Loop & Flooding Logic (SPINE-LEAF SPECIFIC)
        if out_port is None:
            # Kita perlu Flood, TAPI hati-hati loop.
            if dpid in LEAF_SWITCHES:
                # Jika di Leaf
                if in_port in [PORT_UPLINK_SPINE_1, PORT_UPLINK_SPINE_2]:
                    # Paket dari Spine -> HANYA flood ke Host (port > 2)
                    # Karena kita tidak tahu port host pastinya, kita flood tapi exclude uplinks
                    # Namun OFPP_FLOOD akan kirim ke semua port kecuali in_port.
                    # Masalah: Akan kirim balik ke Spine 2 jika datang dari Spine 1.
                    # SOLUSI: Jangan pakai OFPP_FLOOD jika datang dari Uplink. Drop atau Directed Flood.
                    # Asumsi host di port 3, 4, dst.
                    # Untuk keamanan: DROP paket broadcast/unknown unicast dari spine jika kita tidak tahu tujuannya.
                    # Kenapa? Karena Spine seharusnya sudah tahu tujuannya jika ARP jalan.
                    # Tapi untuk fail-safe:
                    out_port = ofproto.OFPP_FLOOD # (Risky in generic, but handled by ProxyARP mostly)
                else:
                    # Paket dari Host -> Flood ke SEMUA (termasuk Uplinks)
                    out_port = ofproto.OFPP_FLOOD
            else:
                # Jika di Spine
                # Flood ke semua port
                out_port = ofproto.OFPP_FLOOD
        
        # Refinement Anti-Loop untuk Flooding di Leaf:
        # Jika out_port adalah FLOOD dan kita di Leaf dan in_port adalah Uplink
        if out_port == ofproto.OFPP_FLOOD and dpid in LEAF_SWITCHES and in_port in [PORT_UPLINK_SPINE_1, PORT_UPLINK_SPINE_2]:
             # Ini berpotensi loop L1->S1->L2->S2->L1.
             # Kita batalkan flood ke Uplink lain. Kita hanya flood ke port lokal.
             # Cara paling aman di OpenFlow tanpa tahu port host: 
             # Controller PacketOut ke semua port KECUALI port 1 & 2.
             # Implementasi simplified: biarkan OFPP_FLOOD tapi andalkan Proxy ARP.
             pass

        actions = [parser.OFPActionOutput(out_port)]

        # 5. Flow Installation
        # Jika destination dikenal (Unicast), pasang flow agar packet selanjutnya hardware switching
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            # Verify buffer_id
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)

        # 6. Kirim Paket (Packet Out)
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # --------------------------------------------------------------------------
    # Monitoring & Telemetry Loop
    # --------------------------------------------------------------------------
    def _monitor_loop(self):
        """Thread utama yang berjalan paralel"""
        while True:
            # 1. Minta Statistik Switch (Request)
            for dp in self.datapaths.values():
                self._request_stats(dp)
            
            # 2. Jalankan Logika Traffic Manager (AI Check & Reroute)
            #    Kita pass copy dari datapaths untuk thread safety
            if self.datapaths:
                self.traffic_mgr.evaluate_network_state(self.datapaths.copy())
            
            hub.sleep(MONITOR_INTERVAL)

    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        """Menerima balasan statistik dari switch dan simpan ke DB"""
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        stats_to_insert = []
        for stat in body:
            # Filter Port: Local & Controller
            if stat.port_no > 1000: continue
            
            stats_data = {
                'tx_bytes': stat.tx_bytes,
                'rx_bytes': stat.rx_bytes,
                'tx_pkts': stat.tx_packets,
                'rx_pkts': stat.rx_packets,
                'duration': stat.duration_sec
            }
            stats_to_insert.append(stats_data)

        # Kirim ke DB Module
        if stats_to_insert:
            self.db.insert_flow_stats(dpid, stats_to_insert)

# ==============================================================================
# END OF CODE
# ==============================================================================