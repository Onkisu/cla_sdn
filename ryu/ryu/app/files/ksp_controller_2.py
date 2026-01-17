import time
import threading
import psycopg2
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ether_types, ipv4, arp
from ryu.lib import hub

# ==========================================
# CONFIGURATION
# ==========================================
DB_HOST = "127.0.0.1"
DB_NAME = "development"
DB_USER = "dev_one"
DB_PASS = "hijack332."

PREDICTION_THRESHOLD_BPS = 120000 

# Port Mapping pada Leaf Switch (dpid 4, 5, 6)
PORT_TO_S1 = 1  # Uplink ke Spine 1
PORT_TO_S2 = 2  # Uplink ke Spine 2
PORT_HOST  = 4  # Downlink ke Host

class ProactiveAIController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ProactiveAIController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.is_rerouted = False
        
        # Mulai Thread Monitoring (Non-blocking)
        self.monitor_thread = hub.spawn(self._monitor_loop)

    def get_db_conn(self):
        try:
            return psycopg2.connect(
                host=DB_HOST, database=DB_NAME, 
                user=DB_USER, password=DB_PASS, connect_timeout=3
            )
        except:
            return None

    # =========================================================================
    # OPENFLOW SETUP
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
        
        # Table-miss flow: Send to Controller
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
    # INTELLIGENT PACKET HANDLING (ANTI-LOOP)
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

        # Ignore LLDP/IPv6 agar log bersih
        if eth.ethertype == ether_types.ETH_TYPE_LLDP or eth.ethertype == 34525:
            return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # --- LOGIC FORWARDING ---
        if dst in self.mac_to_port[dpid]:
            # Jika tujuan sudah dikenal, kirim Unicast
            out_port = self.mac_to_port[dpid][dst]
        else:
            # Jika tujuan belum dikenal (Broadcasting/Flooding)
            # KITA HARUS MENCEGAH LOOP DISINI
            if dpid >= 4:  # Jika ini LEAF Switch (dpid 4,5,6)
                if in_port == PORT_TO_S1 or in_port == PORT_TO_S2:
                    # Paket datang dari Spine (Uplink), JANGAN kembalikan ke Spine lain!
                    # Hanya kirim ke Host (Downlink)
                    out_port = PORT_HOST
                else:
                    # Paket datang dari Host, kirim ke semua Spine (Uplink)
                    out_port = ofproto.OFPP_FLOOD 
            else:
                # Jika Spine Switch, Flood saja (karena Spine tidak terhubung ke Spine lain)
                out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # --- INSTALL FLOW (Agar paket selanjutnya tidak tanya controller lagi) ---
        if out_port != ofproto.OFPP_FLOOD:
            # Match hanya Destination MAC agar lebih fleksibel terhadap multipath
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            
            # Verify buffer_id
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
    # STATS COLLECTOR & AI ACTION
    # =========================================================================
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        # Gunakan thread terpisah/async untuk DB agar tidak memblokir Packet processing
        hub.spawn(self._save_stats_to_db, dpid, body)

    def _save_stats_to_db(self, dpid, body):
        conn = self.get_db_conn()
        if not conn: return
        try:
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
        except Exception:
            pass

    def _monitor_loop(self):
        while True:
            # Request Stats
            for dp in self.datapaths.values():
                req = dp.ofproto_parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY)
                dp.send_msg(req)

            # Cek Prediksi
            self._check_forecast_and_act()
            hub.sleep(1)

    def _check_forecast_and_act(self):
        # Logika Reroute H1 -> H2
        l1 = self.datapaths.get(4)
        if not l1: return

        try:
            conn = self.get_db_conn()
            if not conn: return
            cur = conn.cursor()
            
            cur.execute("SELECT y_pred FROM forecast_1h ORDER BY ts_created DESC LIMIT 1")
            result = cur.fetchone()
            cur.close()
            conn.close()

            if not result: return
            y_pred = float(result[0])

            if y_pred > PREDICTION_THRESHOLD_BPS:
                if not self.is_rerouted:
                    self.logger.info(f"🔮 AI ALERT: Burst {y_pred:.0f} bps. REROUTING to Spine 2.")
                    self.apply_reroute(l1, to_spine_2=True)
                    self.is_rerouted = True
            else:
                if self.is_rerouted:
                    self.logger.info(f"✅ AI CALM: Traffic {y_pred:.0f} bps. Back to Spine 1.")
                    self.apply_reroute(l1, to_spine_2=False)
                    self.is_rerouted = False
        except Exception:
            pass

    def apply_reroute(self, datapath, to_spine_2=True):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Match spesifik IP Src/Dst untuk H1->H2
        match = parser.OFPMatch(
            eth_type=ether_types.ETH_TYPE_IP,
            ipv4_src='10.0.0.1',
            ipv4_dst='10.0.0.2'
        )

        out_port = PORT_TO_S2 if to_spine_2 else PORT_TO_S1
        actions = [parser.OFPActionOutput(out_port)]
        
        # Priority 100 (High)
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=100,
                                match=match, instructions=inst)
        datapath.send_msg(mod)