#!/usr/bin/env python3
"""
Ryu SDN Controller: Stable Priority-Based Rerouting
- H1 (VoIP): Dynamic (Spine 2 -> Spine 1 saat macet -> Spine 2 saat aman)
- H3 (Video): Static (Selalu Spine 2)
- Mekanisme: High Priority Overlay (Mencegah chaos saat transisi)
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types, ipv4, arp
from ryu.topology.api import get_switch, get_link
import psycopg2
from datetime import datetime
import time

# --- KONFIGURASI DATABASE ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# --- KONFIGURASI THRESHOLD ---
BURST_THRESHOLD_BPS = 120000  # Batas atas (Trigger Reroute)
LOWER_THRESHOLD_BPS = 80000   # Batas bawah (Trigger Revert) - Harus lebih rendah!

# --- KONFIGURASI PORT (SESUAIKAN DENGAN MININET ANDA) ---
# Asumsi Topologi:
# Leaf 1 (DPID 4) -> Port 1 ke Spine 1, Port 3 ke Spine 2
# Spine 1 (DPID 1) -> Port 1 ke Leaf Tujuan
# Spine 2 (DPID 2) -> Port 1 ke Leaf Tujuan
PORT_TO_SPINE_1 = 1
PORT_TO_SPINE_2 = 3

class VoIPPriorityController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VoIPPriorityController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.last_bytes = {}
        
        # State
        self.congestion_active = False 
        self.last_reroute_ts = 0 
        self.cooldown_period = 20 # Detik (Mencegah pindah-pindah terlalu cepat)

        # Priority Levels
        self.PRIO_HIGH = 100  # Untuk Reroute (Spine 1)
        self.PRIO_LOW = 10    # Untuk Default (Spine 2)
        
        # Threads
        self.monitor_thread = hub.spawn(self._monitor_traffic)
        self.forecast_thread = hub.spawn(self._monitor_forecast)

        self.logger.info("✅ Controller Started: Priority Based Rerouting")

    def get_db_conn(self):
        try:
            return psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        except:
            return None

    # =================================================================
    # LOGIKA INTI: MONITORING & SWITCHING
    # =================================================================
    def _monitor_forecast(self):
        """Memonitor prediksi trafik dan trigger reroute/revert"""
        while True:
            hub.sleep(2)
            conn = self.get_db_conn()
            if not conn: continue
            
            try:
                cur = conn.cursor()
                cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
                res = cur.fetchone()
                cur.close()
                conn.close()

                if not res: continue
                pred_bps = res[0]
                now = time.time()

                # --- 1. DETEKSI MACET (Trigger Reroute) ---
                if pred_bps > BURST_THRESHOLD_BPS and not self.congestion_active:
                    if (now - self.last_reroute_ts) > 5: # Safety delay
                        self.logger.warning(f"⚠️ CONGESTION: {pred_bps:.0f} bps. Moving H1 to Spine 1.")
                        self.activate_reroute_h1()

                # --- 2. DETEKSI AMAN (Trigger Revert) ---
                elif self.congestion_active:
                    # Syarat: Trafik turun DAN sudah lewat masa cooldown
                    if pred_bps < LOWER_THRESHOLD_BPS and (now - self.last_reroute_ts > self.cooldown_period):
                        self.logger.info(f"✅ NORMAL: {pred_bps:.0f} bps. Returning H1 to Spine 2.")
                        self.deactivate_reroute_h1()

            except Exception as e:
                self.logger.error(f"DB Error: {e}")

    def activate_reroute_h1(self):
        """
        Hanya memindahkan H1. H3 tidak disentuh.
        Caranya: Pasang Flow Priority TINGGI (100) untuk H1 ke Spine 1.
        """
        self.congestion_active = True
        self.last_reroute_ts = time.time()
        
        # Clear stats cache agar tidak ada lonjakan data palsu
        self.clear_stats_cache('10.0.0.1')

        # Rule: H1(10.0.0.1) -> H2(10.0.0.2) Lewat PORT 1 (Spine 1)
        # Priority: 100 (Mengalahkan default priority 10)
        dp_leaf1 = self.datapaths.get(4) # Leaf yang connect ke H1
        if dp_leaf1:
            parser = dp_leaf1.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(PORT_TO_SPINE_1)] 
            self.add_flow(dp_leaf1, self.PRIO_HIGH, match, actions)
            self.logger.info("   -> Applied High Prio Rule: H1 via Spine 1")

        # Pastikan Spine 1 tahu jalan ke tujuan (agar tidak drop)
        dp_spine1 = self.datapaths.get(1)
        if dp_spine1:
            parser = dp_spine1.ofproto_parser
            match = parser.OFPMatch(eth_type=0x0800, ipv4_dst='10.0.0.2')
            actions = [parser.OFPActionOutput(1)] # Asumsi Port 1 ke Leaf tujuan
            self.add_flow(dp_spine1, self.PRIO_HIGH, match, actions)

    def deactivate_reroute_h1(self):
        """
        Kembalikan H1 ke jalur normal.
        Caranya: CUKUP HAPUS Flow Priority TINGGI (100).
        Otomatis paket akan jatuh ke Flow Priority RENDAH (10) yang mengarah ke Spine 2.
        """
        self.congestion_active = False
        self.last_reroute_ts = time.time()
        
        self.clear_stats_cache('10.0.0.1')

        dp_leaf1 = self.datapaths.get(4)
        if dp_leaf1:
            parser = dp_leaf1.ofproto_parser
            ofproto = dp_leaf1.ofproto
            
            # Hapus HANYA rule prioritas tinggi milik H1
            match = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
            mod = parser.OFPFlowMod(
                datapath=dp_leaf1,
                command=ofproto.OFPFC_DELETE_STRICT, # Strict = Hapus yg persis match
                priority=self.PRIO_HIGH,             # Hapus yg priority 100 saja
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=match
            )
            dp_leaf1.send_msg(mod)
            self.logger.info("   -> Removed High Prio Rule. H1 falls back to Spine 2.")

    def clear_stats_cache(self, target_ip):
        """Hapus cache statistik agar perhitungan bandwidth reset"""
        keys_to_del = [k for k in self.last_bytes if target_ip in k]
        for k in keys_to_del:
            del self.last_bytes[k]

    # =================================================================
    # STANDARD HANDLERS
    # =================================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                # Saat switch connect, pasang default rule (Spine 2)
                if datapath.id == 4: # Leaf 1
                    self.install_default_path(datapath)
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    def install_default_path(self, datapath):
        """
        Pasang jalur 'Default' (Spine 2) dengan Priority RENDAH (10).
        Ini berlaku untuk H1 DAN H3.
        """
        parser = datapath.ofproto_parser
        # Default H1 -> H2 lewat Spine 2
        match_h1 = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.1', ipv4_dst='10.0.0.2')
        actions = [parser.OFPActionOutput(PORT_TO_SPINE_2)]
        self.add_flow(datapath, self.PRIO_LOW, match_h1, actions)

        # Default H3 -> H2 lewat Spine 2 (Ini tidak akan pernah ditimpa oleh priority 100)
        match_h3 = parser.OFPMatch(eth_type=0x0800, ipv4_src='10.0.0.3', ipv4_dst='10.0.0.2')
        self.add_flow(datapath, self.PRIO_LOW, match_h3, actions)
        
        self.logger.info("   -> Default Rules Installed (Prio 10) for H1 & H3 via Spine 2")

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
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority, 
                                    match=match, instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, 
                                    match=match, instructions=inst)
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

        # Learning MAC
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][eth.src] = in_port

        # --- LOGIKA PACKET-IN SEDERHANA ---
        # Kita mengandalkan Proactive Flow yang dipasang di awal / saat reroute.
        # Packet-In hanya menangani ARP atau broadcast pertama kali.
        
        dst = eth.dst
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]
        
        # Install flow L2 sederhana (agar tidak flood terus)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=eth.src)
            # Prio 1 agar tidak mengganggu routing policy Prio 10/100
            self.add_flow(datapath, 1, match, actions) 

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =================================================================
    # STATS MONITORING (Sama seperti sebelumnya)
    # =================================================================
    def _monitor_traffic(self):
        while True:
            for dp in list(self.datapaths.values()):
                if dp.id:
                    try:
                        req = dp.ofproto_parser.OFPFlowStatsRequest(dp)
                        dp.send_msg(req)
                    except: pass
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        timestamp = datetime.now()

        # === PATCH: Ignore Stale Stats after Reroute ===
        # Beri jeda 3 detik setelah reroute sebelum menerima stats lagi
        # agar counter yang belum ter-reset di switch tidak terbaca sebagai lonjakan.
        if time.time() - self.last_reroute_ts < 3.0:
            return 
        
        for stat in body:
            if stat.priority == 0: continue
            
            match = stat.match
            src_ip = match.get('ipv4_src') or self._resolve_ip(match.get('eth_src'))
            dst_ip = match.get('ipv4_dst') or self._resolve_ip(match.get('eth_dst'))
            
            if dst_ip != '10.0.0.2': continue
            if src_ip not in ['10.0.0.1', '10.0.0.3']: continue
                
            flow_key = f"{dpid}-{src_ip}-{dst_ip}"
            byte_count = stat.byte_count
            packet_count = stat.packet_count
            
            # Hitung Delta
            prev_b, prev_p = self.last_bytes.get(flow_key, (0, 0))
            
            # Jika counter reset (karena flow baru diinstall), anggap delta = current
            if byte_count < prev_b:
                delta_b = byte_count
                delta_p = packet_count
            else:
                delta_b = byte_count - prev_b
                delta_p = packet_count - prev_p
            
            # Update Cache
            self.last_bytes[flow_key] = (byte_count, packet_count)
            
            if delta_b <= 0: continue
            
            ip_proto = match.get('ip_proto') or 17
            tp_src = match.get('tcp_src') or match.get('udp_src') or 0
            tp_dst = match.get('tcp_dst') or match.get('udp_dst') or 0
            
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
                    """, (timestamp, dpid, src_ip, dst_ip, 
                          match.get('eth_src',''), match.get('eth_dst',''),
                          ip_proto, tp_src, tp_dst, delta_b, delta_b, 
                          delta_p, delta_p, 1.0, 
                          'voip' if src_ip == '10.0.0.1' else 'bursty'))
                    conn.commit()
                    conn.close()
                except Exception as e: 
                    pass