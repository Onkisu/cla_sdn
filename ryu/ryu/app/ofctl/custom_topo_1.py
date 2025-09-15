from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ether_types, ipv4, arp, tcp, udp
import json
from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication, route

# Create a simple REST API for the collector to get mappings
class MappingController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(MappingController, self).__init__(req, link, data, **config)
        self.controller_app = data['controller_app']

    @route('mappings', '/ip_mac_map', methods=['GET'])
    def get_ip_mac_map(self, req, **kwargs):
        body = json.dumps(self.controller_app.get_ip_mac_map())
        return Response(content_type='application/json', body=body)

    @route('mappings', '/mac_ip_map', methods=['GET'])
    def get_mac_ip_map(self, req, **kwargs):
        body = json.dumps(self.controller_app.get_mac_ip_map())
        return Response(content_type='application/json', body=body)

class CollectorFriendlyController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    _CONTEXTS = {
        'wsgi': WSGIApplication
    }

    def __init__(self, *args, **kwargs):
        super(CollectorFriendlyController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_mac_map = {}  # Store IP to MAC mapping
        self.mac_ip_map = {}  # Store MAC to IP mapping
        
        # Setup REST API
        wsgi = kwargs['wsgi']
        wsgi.register(MappingController, {'controller_app': self})

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Default flow: send unmatched packets to controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
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

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return  # ignore LLDP

        dst = eth.dst
        src = eth.src
        dpid = format(datapath.id, "d").zfill(16)
        self.mac_to_port.setdefault(dpid, {})

        # Learn MAC address to port mapping
        self.mac_to_port[dpid][src] = in_port

        # Determine output port
        out_port = self.mac_to_port[dpid].get(dst, ofproto.OFPP_FLOOD)

        actions = [parser.OFPActionOutput(out_port)]

        # Process ARP packets - LEARN IP-MAC MAPPING
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            # LEARN IP-MAC MAPPING FROM ARP PACKETS
            self.ip_mac_map[arp_pkt.src_ip] = src
            self.mac_ip_map[src] = arp_pkt.src_ip
            if arp_pkt.opcode == arp.ARP_REPLY and arp_pkt.dst_ip in self.ip_mac_map:
                self.mac_ip_map[dst] = arp_pkt.dst_ip
            
            # Install ARP flow
            match = parser.OFPMatch(
                eth_type=ether_types.ETH_TYPE_ARP,
                arp_spa=arp_pkt.src_ip,
                arp_tpa=arp_pkt.dst_ip
            )
            self.add_flow(datapath, 1, match, actions)
        else:
            # Process IPv4 packets
            ip_pkt = pkt.get_protocol(ipv4.ipv4)
            if ip_pkt:
                # LEARN IP-MAC MAPPING FROM IP PACKETS
                if ip_pkt.src not in self.ip_mac_map:
                    self.ip_mac_map[ip_pkt.src] = src
                if ip_pkt.dst in self.ip_mac_map and dst not in self.mac_ip_map:
                    self.mac_ip_map[dst] = ip_pkt.dst

                # Extract transport layer information (TCP/UDP)
                tcp_pkt = pkt.get_protocol(tcp.tcp)
                udp_pkt = pkt.get_protocol(udp.udp)
                
                # Build match for IPv4 flow
                match_dict = {
                    'eth_type': ether_types.ETH_TYPE_IP,
                    'ipv4_src': ip_pkt.src,
                    'ipv4_dst': ip_pkt.dst,
                    'ip_proto': ip_pkt.proto
                }
                
                # Add transport layer ports if available
                if tcp_pkt:
                    match_dict['tcp_src'] = tcp_pkt.src_port
                    match_dict['tcp_dst'] = tcp_pkt.dst_port
                elif udp_pkt:
                    match_dict['udp_src'] = udp_pkt.src_port
                    match_dict['udp_dst'] = udp_pkt.dst_port
                
                match = parser.OFPMatch(**match_dict)
                self.add_flow(datapath, 10, match, actions)
            else:
                # Non-IP, fallback to L2 only
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_src=src,
                    eth_dst=dst
                )
                self.add_flow(datapath, 1, match, actions)

        # Send packet out
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )
        datapath.send_msg(out)

    # Method to get IP-MAC mapping for collector
    def get_ip_mac_map(self):
        return self.ip_mac_map

    # Method to get MAC-IP mapping for collector  
    def get_mac_ip_map(self):
        return self.mac_ip_map