import p_key
import time
import json
import psycopg2
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ether_types, arp, ipv4
from ryu.lib import hub

# ==========================================
# KONFIGURASI SESUAI FILE ANDA
# ==========================================
DB_HOST = "127.0.0.1"
DB_NAME = "development"
DB_USER = "dev_one"
DB_PASS = "hijack332."

# Threshold harus match dengan forecast_2.py (120kbps)
# Kita pakai sedikit buffer agar lebih responsif
BURST_THRESHOLD = 120000 

# Port Mapping pada Leaf Switch (Berdasarkan urutan addLink di mininet script)
# Link 1: Leaf -> Spine 1
# Link 2: Leaf -> Spine 2
PORT_SPINE_1 = 1
PORT_SPINE_2 = 2

class ProactiveController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ProactiveController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.is_rerouted = False
        
        # Thread untuk monitoring & polling DB forecast
        self.monitor_thread = hub.spawn(self._monitor_loop)

    def get_db_conn(self):
        try:
            return psycopg2.connect(
                host=DB_HOST, database=DB_NAME, 
                user=DB_USER, password=DB_PASS
            )
        except Exception as e:
            self.logger.error(f"Database Error: {e}")
            return None

    # =========================================================================
    # 1. INITIAL SETUP
    # =========================================================================
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
        datapath = ev.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Install Table-Miss Flow (Kirim ke Controller)
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id if buffer_id else ofproto.OFP_NO_BUFFER,
                                priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    # =========================================================================
    # 2. PACKET HANDLING (L2 SWITCHING + ANTI LOOP)
    # =========================================================================
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
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

        # Logic Switching Dasar
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            # FLOODING LOGIC (PENTING AGAR TIDAK LOOP DI SPINE-LEAF)
            # Jika paket datang dari Spine (Port 1/2), JANGAN balikkan ke Spine lain.
            # Hanya flood ke Host (Port > 2)
            if in_port in [PORT_SPINE_1, PORT_SPINE_2]:
                out_port = ofproto.OFPP_FLOOD # Di Spine-Leaf real, ini harus directed flood ke host port saja
            else:
                out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install Flow Standar (Priority 1)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            self.add_flow(datapath, 1, match, actions, msg.buffer_id)
        
        # Kirim PacketOut
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =========================================================================
    # 3. STATS COLLECTION (OUTPUT KE DB TRAFFIC.FLOW_STATS_)
    # =========================================================================
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        conn = self.get_db_conn()
        if not conn: return
        
        try:
            cur = conn.cursor()
            for stat in body:
                # Filter port controller/local
                if stat.port_no > 1000: continue
                
                # INSERT realtime stats agar forecast_2.py punya data
                cur.execute("""
                    INSERT INTO traffic.flow_stats_ (
                        dpid, timestamp, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                """, (dpid, stat.tx_bytes, stat.rx_bytes, stat.tx_packets, stat.rx_packets, stat.duration_sec))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error saving stats: {e}")

    # =========================================================================
    # 4. PROACTIVE LOOP (BACA DB FORECAST & REROUTE)
    # =========================================================================
    def _monitor_loop(self):
        while True:
            # A. Request Stats dari Switch (Setiap 1 detik)
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY)
                dp.send_msg(req)

            # B. Cek Hasil Prediksi AI
            self._check_ai_prediction()
            
            hub.sleep(1)

    def _check_ai_prediction(self):
        conn = self.get_db_conn()
        if not conn: return

        try:
            cur = conn.cursor()
            # Ambil prediksi terbaru dari script forecast_2.py
            cur.execute("""
                SELECT y_pred 
                FROM forecast_1h 
                ORDER BY ts_created DESC 
                LIMIT 1
            """)
            res = cur.fetchone()
            cur.close()
            conn.close()

            if res:
                y_pred = float(res[0])
                self._handle_reroute_logic(y_pred)
                
        except Exception as e:
            self.logger.error(f"Forecast check error: {e}")

    def _handle_reroute_logic(self, prediction_value):
        # Target Switch: Leaf 1 (dpid 4) dimana H1 berada
        # Target Traffic: H1 (10.0.0.1) -> H2 (10.0.0.2)
        leaf1 = self.datapaths.get(4)
        if not leaf1: return

        is_burst = prediction_value > BURST_THRESHOLD

        if is_burst and not self.is_rerouted:
            self.logger.warning(f"⚠️  BURST PREDICTED ({prediction_value:.0f} bps). REROUTING H1->H2 to Spine 2.")
            
            # Action: Pindahkan ke Spine 2
            self._apply_policy(leaf1, use_spine_2=True)
            self._log_system_event("REROUTE_TRIGGERED", f"AI predicted {prediction_value} bps. Switching to Spine 2.", prediction_value)
            self.is_rerouted = True

        elif not is_burst and self.is_rerouted:
            self.logger.info(f"✅ TRAFFIC NORMAL ({prediction_value:.0f} bps). Restoring H1->H2 to Spine 1.")
            
            # Action: Kembalikan ke Spine 1
            self._apply_policy(leaf1, use_spine_2=False)
            self._log_system_event("REROUTE_RESTORED", f"Traffic normalized. Switching back to Spine 1.", prediction_value)
            self.is_rerouted = False

    def _apply_policy(self, datapath, use_spine_2):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Flow Match: HANYA H1 -> H2
        match = parser.OFPMatch(
            eth_type=ether_types.ETH_TYPE_IP,
            ipv4_src="10.0.0.1",
            ipv4_dst="10.0.0.2"
        )

        # Tentukan Output Port
        out_port = PORT_SPINE_2 if use_spine_2 else PORT_SPINE_1
        actions = [parser.OFPActionOutput(out_port)]

        # Priority 100 (Lebih tinggi dari switching biasa yg cuma 1)
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # Kirim FlowMod
        mod = parser.OFPFlowMod(datapath=datapath, priority=100,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def _log_system_event(self, event_type, desc, val):
        conn = self.get_db_conn()
        if not conn: return
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (event_type, description, trigger_value)
                VALUES (%s, %s, %s)
            """, (event_type, desc, val))
            conn.commit()
            conn.close()
        except Exception:
            pass