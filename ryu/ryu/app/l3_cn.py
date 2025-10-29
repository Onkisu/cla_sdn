#!/usr/bin/python3
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ether_types, arp, ipv4
from ryu.lib.packet.ether_types import ETH_TYPE_IP, ETH_TYPE_ARP
from ryu.lib.packet.in_proto import IPPROTO_ICMP, IPPROTO_UDP, IPPROTO_TCP

from collections import defaultdict
import json
from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication, route

class L3SpineLeafController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {
        'wsgi': WSGIApplication
    }

    def __init__(self, *args, **kwargs):
        super(L3SpineLeafController, self).__init__(*args, **kwargs)
        # BUKAN mac_to_port, tapi ip_to_port (L3)
        # { dpid -> { ip -> port } }
        self.ip_to_port = defaultdict(dict)
        # Simpan objek datapath (switch)
        self.datapaths = {}

        # REST API (Sama seperti file Anda sebelumnya, tapi pakai L3)
        wsgi = kwargs['wsgi']
        wsgi.register(RESTController, {'controller_app': self})

    # --- Helper untuk REST API ---
    def get_ip_to_port_map(self):
        return self.ip_to_port

    def get_datapaths(self):
        # Mengembalikan DPID dalam format string
        return {str(dpid): str(dp) for dpid, dp in self.datapaths.items()}


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id
        
        # Simpan objek datapath
        self.datapaths[dpid] = datapath
        self.logger.info("Switch terhubung: DPID=%s", dpid)

        # Default flow: kirim paket ARP dan IP ke controller
        # 1. ARP -> Controller
        match_arp = parser.OFPMatch(eth_type=ETH_TYPE_ARP)
        actions_arp = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                              ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 10, match_arp, actions_arp, "ARP->Controller")

        # 2. IP -> Controller
        match_ip = parser.OFPMatch(eth_type=ETH_TYPE_IP)
        actions_ip = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                             ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 1, match_ip, actions_ip, "IP->Controller") # Prioritas IP lebih rendah

    def add_flow(self, datapath, priority, match, actions, log_msg=""):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        # self.logger.info(f"Adding flow ke {datapath.id}: {log_msg}")
        datapath.send_msg(mod)

    def _send_packet_out(self, datapath, buffer_id, in_port, out_port, data):
        """Helper untuk mengirim PacketOut."""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(out_port)]
        
        msg = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )
        datapath.send_msg(msg)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return  # abaikan LLDP

        # ----------------------------------------------------
        # Bagian 1: Logika ARP (Belajar IP dan Membalas ARP)
        # ----------------------------------------------------
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            src_ip = arp_pkt.src_ip
            src_mac = arp_pkt.src_mac
            dst_ip = arp_pkt.dst_ip

            # BELAJAR: Simpan IP -> Port mapping
            if src_ip != '0.0.0.0':
                self.ip_to_port[dpid][src_ip] = in_port
                self.logger.info(f"Belajar: DPID={dpid} | IP={src_ip} | Port={in_port}")
            
            # RESPON: Cek apakah kita tahu di mana tujuan ARP (dst_ip)
            # Ini adalah logika L3 routing sederhana
            out_port = None
            for dp_id, ip_map in self.ip_to_port.items():
                if dst_ip in ip_map:
                    # Kita tahu IP itu ada di switch 'dp_id'
                    # Untuk topologi Spine-Leaf, kita harus merutekannya
                    # Tapi untuk controller L2/L3 hybrid sederhana, kita flood saja
                    # *Kontroler L3 sejati akan menghitung jalur*
                    # Untuk sekarang, kita flood ARP agar pingAll berhasil
                    out_port = ofproto.OFPP_FLOOD
                    break
            
            if out_port is None:
                # Jika kita tidak tahu sama sekali, flood
                out_port = ofproto.OFPP_FLOOD

            self._send_packet_out(datapath, msg.buffer_id, in_port, out_port, msg.data)
            return

        # ----------------------------------------------------
        # Bagian 2: Logika IP (Routing)
        # ----------------------------------------------------
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # BELAJAR: (Belajar dari IP juga, untuk jaga-jaga)
            self.ip_to_port[dpid][src_ip] = in_port
            
            # ROUTING: Cari port untuk IP tujuan
            out_port = None
            
            # 1. Apakah IP tujuan ada di switch ini?
            if dst_ip in self.ip_to_port[dpid]:
                out_port = self.ip_to_port[dpid][dst_ip]
                self.logger.debug(f"IP {dst_ip} ditemukan di switch {dpid} port {out_port}")
            
            # 2. (TODO) Jika tidak, ini adalah TUGAS TESIS Anda:
            #    - Cari di switch mana (Leaf) IP itu berada
            #    - Hitung jalur (Path) via Spine
            #    - Pasang flow di sL1, sS1, sL2
            
            # 3. Untuk sekarang (agar pingAll berhasil), kita flood
            if out_port is None:
                out_port = ofproto.OFPP_FLOOD

            actions = [parser.OFPActionOutput(out_port)]

            # Pasang flow rule agar paket berikutnya tidak ke controller
            match = parser.OFPMatch(eth_type=ETH_TYPE_IP, ipv4_dst=dst_ip)
            self.add_flow(datapath, 10, match, actions, f"Route {src_ip}->{dst_ip}")

            # Kirim paket pertama ini
            self._send_packet_out(datapath, msg.buffer_id, in_port, out_port, msg.data)
            return

# --- Kelas REST API (Sama seperti punya Anda, tapi memanggil fungsi baru) ---
class RESTController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(RESTController, self).__init__(req, link, data, **config)
        self.controller_app = data['controller_app']

    @route('mappings', '/ip_to_port_map', methods=['GET'])
    def get_ip_to_port_map(self, req, **kwargs):
        # Gunakan defaultdict-safe serialization
        data_safe = {str(k): v for k, v in self.controller_app.get_ip_to_port_map().items()}
        body = json.dumps(data_safe)
        return Response(content_type='application/json', body=body.encode('utf-8'))

    @route('mappings', '/datapaths', methods=['GET'])
    def get_datapaths(self, req, **kwargs):
        body = json.dumps(self.controller_app.get_datapaths())
        return Response(content_type='application/json', body=body.encode('utf-8'))