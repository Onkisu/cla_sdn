#!/usr/bin/env python3
"""
THE KERNEL BYPASS CONTROLLER (CONFIGURED FOR YOUR PC)
- Throughput: Dari OpenFlow (OVS)
- Drops: BACA LANGSUNG DARI FILE LINUX (/sys/class/net/...)
- Target: Interface l1-eth3 (Victim Link)
"""
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types
import psycopg2
import time
import os

# CONFIG DATABASE
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

# --- MAPPING REAL DARI IP LINK ANDA ---
INTERFACE_MAP = {
    # DPID 2 = Switch l1
    2: { 
        2: 'l1-eth2',  # Port 2 -> Ke h1 (VoIP User)
        3: 'l1-eth3'   # Port 3 -> Ke h2 (VICTIM) -> SUMBER DROP
    },
    # DPID 3 = Switch l2
    3: {
        2: 'l2-eth2'   # Port 2 -> Ke h3 (Attacker)
    }
}

class KernelMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(KernelMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.prev_stats = {}      
        self.prev_kernel_drops = {} 
        self.monitor_thread = hub.spawn(self._monitor)
        self.conn = None
        self.cur = None
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            self.logger.info("✅ Database Connected")
        except Exception as e:
            self.logger.error(f"❌ DB Error: {e}")

    # --- FUNGSI BACA KERNEL (THE MAGIC) ---
    def get_linux_drops(self, interface_name):
        """Baca langsung file system Linux."""
        try:
            path = f"/sys/class/net/{interface_name}/statistics/tx_dropped"
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return int(f.read().strip())
        except Exception:
            return 0
        return 0

    # --- STANDARD OPENFLOW SETUP ---
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
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
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
        
        # Simple L2 Switch
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER: data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # --- MONITOR LOOP ---
    def _monitor(self):
        while True:
            for dp in list(self.datapaths.values()):
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
            self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    # --- MAIN LOGIC ---
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        now = time.time()

        total_bytes = 0
        total_pkts = 0
        
        for stat in body:
            if stat.priority == 0: continue
            total_bytes += stat.byte_count
            total_pkts += stat.packet_count

        key = dpid
        if key in self.prev_stats:
            last_bytes, last_pkts, last_time = self.prev_stats[key]
            time_delta = now - last_time
            
            if time_delta >= 0.5: 
                delta_bytes = total_bytes - last_bytes
                delta_pkts = total_pkts - last_pkts
                
                # Hitung Throughput
                throughput_bps = int((delta_bytes * 8) / time_delta)
                pps = int(delta_pkts / time_delta)
                if throughput_bps > 2000000000: throughput_bps = 2000000000

                # === KERNEL DROP CHECKER ===
                real_drops = 0
                
                if dpid in INTERFACE_MAP:
                    for port_no, iface_name in INTERFACE_MAP[dpid].items():
                        
                        # 1. BACA DROP DARI LINUX
                        current_kernel_drop = self.get_linux_drops(iface_name)
                        
                        # 2. HITUNG DELTA
                        drop_key = (dpid, iface_name)
                        if drop_key in self.prev_kernel_drops:
                            last_k_drop = self.prev_kernel_drops[drop_key]
                            diff = current_kernel_drop - last_k_drop
                            
                            if diff > 0:
                                real_drops += diff
                                self.logger.info(f"🔥 DROP on {iface_name}: {diff} pkts (Load: {throughput_bps/1e6:.1f} Mbps)")
                        
                        self.prev_kernel_drops[drop_key] = current_kernel_drop
                
                # INSERT KE DB
                if throughput_bps > 1000 or real_drops > 0:
                    self.insert_stats(dpid, throughput_bps, pps, delta_bytes, delta_pkts, drops=real_drops)
                
                self.prev_stats[key] = (total_bytes, total_pkts, now)
        else:
            self.prev_stats[key] = (total_bytes, total_pkts, now)
            # Init Kernel Drops juga saat pertama kali
            if dpid in INTERFACE_MAP:
                for port_no, iface_name in INTERFACE_MAP[dpid].items():
                    drop_key = (dpid, iface_name)
                    self.prev_kernel_drops[drop_key] = self.get_linux_drops(iface_name)

    def insert_stats(self, dpid, bps, pps, bytes_delta, pkts_delta, drops=0):
        if not self.conn or self.conn.closed:
            try: self.connect_db()
            except: return
        try:
            drops = max(0, drops)
            query = """
            INSERT INTO traffic.flow_stats_real
            (timestamp, dpid, throughput_bps, packet_rate_pps, byte_count, packet_count, dropped_count)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s)
            """
            self.cur.execute(query, (str(dpid), bps, pps, bytes_delta, pkts_delta, drops))
            self.conn.commit()
        except Exception:
            if self.conn: self.conn.rollback()