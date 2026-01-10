from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib import hub
import time

class SimpleMonitor13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleMonitor13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        
        # Dictionary untuk menyimpan data throughput sebelumnya
        # Format: { (dpid, port_no): {'bytes': 12345, 'time': 123456789.0} }
        self.prev_stats = {} 
        
        # Thread untuk request statistik tiap 1 detik
        self.monitor_thread = hub.spawn(self._monitor)

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
            hub.sleep(1) # Interval request (1 detik)

    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        # Waktu sekarang (untuk hitung delta time)
        now = time.time()

        print(f"\n--- Switch {dpid} Real-time Bandwidth (Delta) ---")
        
        for stat in body:
            # Kita hanya monitor port fisik (bukan port LOCAL/internal switch)
            if stat.port_no > 4000000000: 
                continue

            # Key unik untuk setiap Port di setiap Switch
            key = (dpid, stat.port_no)
            
            # Ambil data bytes saat ini (bisa tx_bytes atau rx_bytes, disini saya pakai tx+rx)
            current_bytes = stat.rx_bytes + stat.tx_bytes
            
            # --- LOGIC SAFE DELTA ---
            if key in self.prev_stats:
                # 1. Ambil data sebelumnya
                prev_bytes = self.prev_stats[key]['bytes']
                prev_time = self.prev_stats[key]['time']
                
                # 2. Hitung selisih
                delta_bytes = current_bytes - prev_bytes
                delta_time = now - prev_time
                
                # 3. PENGAMAN UTAMA (ANTI CRASH)
                # Jika waktu terlalu cepat (< 0.001 detik), lewati perhitungan untuk hindari pembagian 0
                if delta_time < 0.001:
                    continue 

                # 4. Hitung Bandwidth (Bits per second)
                # Rumus: (Bytes * 8) / Waktu
                speed_bps = (delta_bytes * 8) / delta_time
                speed_mbps = speed_bps / 1000000.0
                
                # Tampilkan hanya jika ada traffic (> 0.01 Mbps) agar terminal bersih
                if speed_mbps > 0.01:
                    print(f"Port {stat.port_no}: {speed_mbps:.4f} Mbps")
            
            # Update data 'prev' untuk putaran berikutnya
            self.prev_stats[key] = {
                'bytes': current_bytes,
                'time': now
            }

    # --- Bagian Switching Standar (Agar Ping Bisa Jalan) ---
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
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
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
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