import time
import threading
import psycopg2
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ether_types, ipv4
from ryu.lib import hub

# ==========================================
# CONFIGURATION
# ==========================================
DB_HOST = "127.0.0.1"
DB_NAME = "development"
DB_USER = "dev_one"
DB_PASS = "hijack332."

# Threshold harus sinkron dengan yang ada di forecast_2.py
# Di script forecast Anda: BURST_THRESHOLD_BPS = 120000 
PREDICTION_THRESHOLD_BPS = 120000 

# Topology Mapping
PORT_TO_S1 = 1
PORT_TO_S2 = 2

class ProactiveAIController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ProactiveAIController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        
        # State tracking
        self.is_rerouted = False
        self.last_prediction_val = 0
        
        # Mulai Thread Monitoring
        self.monitor_thread = hub.spawn(self._monitor_loop)

    def get_db_conn(self):
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, 
            user=DB_USER, password=DB_PASS
        )

    # =========================================================================
    # OPENFLOW BASIC SETUP
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
        
        # Default flow: send to controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match, instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    # =========================================================================
    # L2 SWITCHING (PING & CONNECTIVITY)
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

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install flow agar tidak membebani controller terus menerus
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # =========================================================================
    # STATISTICS COLLECTOR (Feed Data to DB for Forecasting)
    # =========================================================================
    # Kita TETAP butuh ini. Kalau controller tidak kirim stats ke DB,
    # script forecast_2.py tidak punya data untuk memprediksi!
    
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        try:
            conn = self.get_db_conn()
            cur = conn.cursor()
            for stat in body:
                if stat.port_no > 1000: continue 
                cur.execute("""
                    INSERT INTO traffic.flow_stats_ (
                        dpid, bytes_tx, bytes_rx, pkts_tx, pkts_rx, duration_sec
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (dpid, stat.tx_bytes, stat.rx_bytes, stat.tx_packets, stat.rx_packets, stat.duration_sec))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"DB Write Error: {e}")

    # =========================================================================
    # PROACTIVE INTELLIGENCE LOOP
    # =========================================================================
    def _monitor_loop(self):
        while True:
            # 1. Request Stats (Agar DB terisi data real-time untuk forecasting)
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY)
                dp.send_msg(req)

            # 2. Cek Prediksi dari DB
            self._check_forecast_and_act()
            
            hub.sleep(1)

    def _check_forecast_and_act(self):
        # Kita hanya peduli mengamankan trafik H1->H2 (di Leaf 1 / dpid 4)
        l1 = self.datapaths.get(4)
        if not l1: return

        try:
            conn = self.get_db_conn()
            cur = conn.cursor()
            
            # Ambil prediksi TERBARU
            # forecast_2.py menyimpan hasil di tabel forecast_1h
            cur.execute("""
                SELECT y_pred, ts_created 
                FROM forecast_1h 
                ORDER BY ts_created DESC 
                LIMIT 1
            """)
            result = cur.fetchone()
            cur.close()
            conn.close()

            if not result:
                return

            y_pred = float(result[0])
            # ts_created = result[1] # Bisa dipakai untuk validasi expired data jika perlu

            # Logika Proaktif
            if y_pred > PREDICTION_THRESHOLD_BPS:
                if not self.is_rerouted:
                    self.logger.warning(f"🔮 AI PREDICTION: Burst Incoming ({y_pred:.0f} bps). PRE-EMPTIVE REROUTE to Spine 2!")
                    self.apply_reroute(l1, to_spine_2=True)
                    self.log_event("AI_REROUTE_ACTIVE", f"Predicted burst {y_pred:.0f} bps > threshold.", y_pred)
                    self.is_rerouted = True
            else:
                if self.is_rerouted:
                    # Kita bisa langsung balik, atau pakai sedikit hysteresis
                    # Karena ini prediksi, kita asumsikan jika prediksi turun, aman.
                    self.logger.info(f"🔮 AI PREDICTION: Traffic Safe ({y_pred:.0f} bps). Restoring to Spine 1.")
                    self.apply_reroute(l1, to_spine_2=False)
                    self.log_event("AI_PATH_RESTORED", f"Predicted traffic normal.", y_pred)
                    self.is_rerouted = False

        except Exception as e:
            self.logger.error(f"Forecast Check Failed: {e}")

    def apply_reroute(self, datapath, to_spine_2=True):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Trafik H1 (10.0.0.1) -> H2 (10.0.0.2)
        match = parser.OFPMatch(
            eth_type=ether_types.ETH_TYPE_IP,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )

        out_port = PORT_TO_S2 if to_spine_2 else PORT_TO_S1
        actions = [parser.OFPActionOutput(out_port)]
        
        # Priority 100 agar menimpa flow normal (priority 1)
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=100,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def log_event(self, event_type, desc, value):
        try:
            conn = self.get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.system_events (event_type, description, trigger_value)
                VALUES (%s, %s, %s)
            """, (event_type, desc, value))
            conn.commit()
            conn.close()
        except Exception:
            pass