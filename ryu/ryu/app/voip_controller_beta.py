from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp, ipv4
from ryu.lib import hub
from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link
import networkx as nx
import itertools

class KSPController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(KSPController, self).__init__(*args, **kwargs)
        self.topology_api_app = self
        self.net = nx.DiGraph()
        self.mac_to_port = {}
        self.datapaths = {}
        self.discovery_thread = hub.spawn(self._monitor_topology)
        
        # --- KONFIGURASI KSP ---
        self.K_PATHS = 3  # Kita cari 3 jalur terbaik (karena ada 3 Spine)

    # ===============================================================
    # 1. SWITCH CONNECTION & DEFAULT RULES
    # ===============================================================
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.info(f"Switch {datapath.id} Connected.")
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        # Default Flow: Send to Controller
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

    # ===============================================================
    # 2. TOPOLOGY DISCOVERY
    # ===============================================================
    def _monitor_topology(self):
        while True:
            self._update_topology()
            hub.sleep(2)

    def _update_topology(self):
        self.net.clear()
        switch_list = get_switch(self.topology_api_app, None)
        switches = [switch.dp.id for switch in switch_list]
        self.net.add_nodes_from(switches)
        
        link_list = get_link(self.topology_api_app, None)
        for link in link_list:
            self.net.add_edge(link.src.dpid, link.dst.dpid, port=link.src.port_no)
            self.net.add_edge(link.dst.dpid, link.src.dpid, port=link.dst.port_no)

    # ===============================================================
    # 3. K-SHORTEST PATH LOGIC (INTI PERUBAHAN)
    # ===============================================================
    def _get_ksp_out_port(self, src_dpid, dst_dpid, src_ip=None, dst_ip=None):
        """
        Menghitung K-Jalur terpendek dan memilih salah satu.
        """
        try:
            # Menggunakan shortest_simple_paths (implementasi Yen's Algorithm atau BFS varian)
            # Generator ini menghasilkan path dari terpendek ke terpanjang
            paths_generator = nx.shortest_simple_paths(self.net, src_dpid, dst_dpid)
            
            # Ambil K jalur teratas
            k_paths = list(itertools.islice(paths_generator, self.K_PATHS))
            
            if not k_paths:
                return None, None

            # -------------------------------------------------------------
            # [AREA LOGIKA REROUTING ANDA]
            # Di sinilah Anda nanti menyisipkan logika deteksi bursty.
            # Misalnya: if burst_detected(src_ip): selected_index = 1 else: 0
            # -------------------------------------------------------------
            
            selected_path_index = 0  # DEFAULT: Pakai jalur terbaik (index 0)
            
            # Contoh simulasi sederhana (bisa dihapus/diganti nanti):
            # Jika ada flag 'burst' (misal dikirim global), ganti ke index 1 atau 2
            # if self.GLOBAL_BURST_FLAG: 
            #    selected_path_index = random.randint(1, len(k_paths)-1)
            
            # Pastikan index valid (jika path yang ditemukan kurang dari K)
            if selected_path_index >= len(k_paths):
                selected_path_index = 0

            chosen_path = k_paths[selected_path_index]
            
            # self.logger.info(f"KSP Paths found: {len(k_paths)}. Selected Index: {selected_path_index}. Path: {chosen_path}")

            # Cari Port keluar untuk hop berikutnya
            next_hop = chosen_path[1]
            out_port = self.net[src_dpid][next_hop]['port']
            return out_port, chosen_path

        except (nx.NetworkXNoPath, IndexError):
            return None, None
        except Exception as e:
            # self.logger.error(f"KSP Error: {e}")
            return None, None

    # ===============================================================
    # 4. PACKET HANDLING
    # ===============================================================
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        # Ignore LLDP/IPv6 multicast spam
        if eth.ethertype == 35020 or eth.ethertype == 34525: return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # --- Extract IP Info (untuk potensi logic rerouting nanti) ---
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        src_ip = ip_pkt.src if ip_pkt else None
        dst_ip = ip_pkt.dst if ip_pkt else None

        if dst in self.mac_to_port[dpid]:
            # Local switching
            out_port = self.mac_to_port[dpid][dst]
            self._install_forwarding(datapath, src, dst, in_port, out_port, msg)
        else:
            # Routing antar Switch
            dst_dpid = self._find_host_dpid(dst)
            if dst_dpid:
                # Panggil Fungsi KSP
                out_port, path = self._get_ksp_out_port(dpid, dst_dpid, src_ip, dst_ip)
                if out_port:
                    self._install_forwarding(datapath, src, dst, in_port, out_port, msg)
                else:
                    return # Path not found
            else:
                self._flood_smart(msg, dpid, in_port)

    def _find_host_dpid(self, mac):
        for dpid in self.mac_to_port:
            if mac in self.mac_to_port[dpid]:
                return dpid
        return None

    def _install_forwarding(self, datapath, src, dst, in_port, out_port, msg):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        actions = [parser.OFPActionOutput(out_port)]
        match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
        
        # Install Flow (Priority 1)
        # Note: Berikan idle_timeout agar jika jalur berubah (rerouting), flow lama hilang
        if msg.buffer_id != ofproto.OFP_NO_BUFFER:
            self.add_flow(datapath, 1, match, actions, msg.buffer_id)
        else:
            self.add_flow(datapath, 1, match, actions)
            
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def _flood_smart(self, msg, dpid, in_port):
        """Flood hanya ke Edge Ports (Host), bukan ke Backbone"""
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        inter_switch_ports = set()
        if dpid in self.net:
            for nbr in self.net[dpid]:
                inter_switch_ports.add(self.net[dpid][nbr]['port'])

        actions = []
        for port_no in datapath.ports:
            if port_no != in_port and port_no not in inter_switch_ports and port_no < ofproto.OFPP_MAX:
                actions.append(parser.OFPActionOutput(port_no))

        if actions:
            data = None
            if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                data = msg.data
            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                      in_port=in_port, actions=actions, data=data)
            datapath.send_msg(out)