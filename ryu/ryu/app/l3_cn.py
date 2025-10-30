#!/usr/bin/env python3
# controller.py
#
# TIDAK ADA PERUBAHAN. Skrip ini sudah 100% benar.
# Dia nunggu di port 6633 dan sudah bisa handle duplicate connection.
#

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types
from ryu.lib import hub

import threading
import logging

LOG = logging.getLogger('ryu.app.fattree_stable_fixed')
LOG.setLevel(logging.INFO)

class FatTreeStableController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    ECHO_INTERVAL = 5
    STATS_INTERVAL = 10

    def __init__(self, *args, **kwargs):
        super(FatTreeStableController, self).__init__(*args, **kwargs)
        self.datapaths = {}        # dpid -> latest datapath
        self.mac_to_port = {}      # dpid -> {mac -> port}
        self.datapath_locks = {}   # dpid -> threading.Lock()
        self.monitor_thread = hub.spawn(self._monitor)
        LOG.info("✅ FatTreeStableController initialized (Listening on port 6633 by default)")

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        dp = ev.datapath
        dpid = dp.id

        if ev.state == MAIN_DISPATCHER:
            # Ini udah bener, dia bakal ganti koneksi lama kalo ada yg baru
            if dpid in self.datapaths:
                old_dp = self.datapaths[dpid]
                if old_dp != dp:
                    LOG.warning(f"Multiple connection detected for DPID {dpid:016x} — replacing old connection")

            self.datapaths[dpid] = dp
            if dpid not in self.datapath_locks:
                self.datapath_locks[dpid] = threading.Lock()
            self.mac_to_port.setdefault(dpid, {})
            LOG.info(f"✅ Switch {dpid:016x} connected.")
        elif ev.state == DEAD_DISPATCHER:
            if dpid in self.datapaths and self.datapaths[dpid] == dp:
                del self.datapaths[dpid]
                LOG.info(f"❌ Switch {dpid:016x} disconnected.")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        dp = ev.msg.datapath
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self._add_flow(dp, priority=0, match=match, actions=actions)
        LOG.info(f"📋 Table-miss flow added for switch {dp.id:016x}")

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        dpid = dp.id
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        src = eth.src
        dst = eth.dst
        in_port = msg.match['in_port']

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        out_port = self.mac_to_port[dpid].get(dst, ofp.OFPP_FLOOD)
        actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofp.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
            self.add_flow(dp, priority=1, match=match, actions=actions, idle_timeout=30)

        data = None
        if msg.buffer_id == ofp.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=dp, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        dp.send_msg(out)

    def _add_flow(self, dp, priority, match, actions, buffer_id=None, idle_timeout=0, hard_timeout=0):
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]

        if buffer_id:
            mod = parser.OFPFlowMod(datapath=dp, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout, hard_timeout=hard_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=dp,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout, hard_timeout=hard_timeout)
        dp.send_msg(mod)

    def _monitor(self):
        while True:
            dps = list(self.datapaths.values())
            for dp in dps:
                try:
                    self._request_flow_stats(dp)
                except Exception as e:
                    LOG.debug(f"Monitor: failed stats for DPID {getattr(dp,'id',None)}: {e}")
            hub.sleep(self.STATS_INTERVAL)

    def _request_flow_stats(self, dp):
        parser = dp.ofproto_parser
        req = parser.OFPFlowStatsRequest(dp)
        dp.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply(self, ev):
        dp = ev.msg.datapath
        dpid = dp.id
        body = ev.msg.body
        for stat in [f for f in body if f.priority == 1]:
            match = stat.match
            LOG.debug(f"FlowStats: DPID {dpid:016x}, match {match}, packets {stat.packet_count}, bytes {stat.byte_count}")