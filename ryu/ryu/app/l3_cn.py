#!/usr/bin/env python3
# controller_fattree_stable.py
#
# Ryu controller designed for Fat-Tree Mininet experiments.
# - Handles multiple reconnects by closing old datapath connections.
# - Sends periodic EchoRequests (keepalive).
# - Simple learning switch logic + flow install.
# - Periodic flow stats request for telemetry.

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types

import logging

LOG = logging.getLogger('ryu.app.controller_fattree_stable')
LOG.setLevel(logging.INFO)

class FatTreeStableController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    # keepalive interval (seconds)
    ECHO_INTERVAL = 5
    # stats poll interval (seconds)
    STATS_INTERVAL = 10

    def __init__(self, *args, **kwargs):
        super(FatTreeStableController, self).__init__(*args, **kwargs)
        self.datapaths = {}        # dpid -> datapath object (latest)
        self.datapath_lock = {}    # dpid -> hub.Lock(), prevents races when replacing datapaths
        self.mac_to_port = {}      # dpid -> { mac -> port }
        self.monitor_thread = hub.spawn(self._monitor)
        self.echo_thread = hub.spawn(self._echo_loop)
        LOG.info("✅ FatTreeStableController initialized")

    # -------------------------
    # State change handler
    # -------------------------
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        dp = ev.datapath
        dpid = dp.id

        if ev.state == MAIN_DISPATCHER:
            if dpid in self.datapaths:
                old_dp = self.datapaths[dpid]
                if old_dp is not dp:
                    LOG.warning(f"Multiple connection detected for DPID {dpid:016x} — replacing old connection with new one")
                    try:
                        if hasattr(old_dp, 'close'):
                            old_dp.close()
                            LOG.info(f"Closed previous datapath object for {dpid:016x}")
                    except Exception as e:
                        LOG.exception(f"Failed to close old datapath for {dpid:016x}: {e}")

            self.datapaths[dpid] = dp
            if dpid not in self.datapath_lock:
                self.datapath_lock[dpid] = hub.Lock()
            self.mac_to_port.setdefault(dpid, {})
            LOG.info(f"✅ Switch {dpid:016x} connected.")

        elif ev.state == DEAD_DISPATCHER:
            if dpid in self.datapaths and self.datapaths[dpid] is dp:
                del self.datapaths[dpid]
                LOG.info(f"❌ Switch {dpid:016x} disconnected.")
            self.datapath_lock.pop(dpid, None)

    # -------------------------
    # Switch feature (table-miss)
    # -------------------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self._add_flow(dp, priority=0, match=match, actions=actions)
        LOG.info(f"📋 Table-miss flow added for switch {dp.id:016x}")

    # -------------------------
    # PacketIn: learning switch
    # -------------------------
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
        in_port = msg.match.get('in_port')

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        out_port = self.mac_to_port[dpid].get(dst, ofp.OFPP_FLOOD)
        actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofp.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
            self._add_flow(dp, priority=1, match=match, actions=actions, idle_timeout=30)

        data = None
        if msg.buffer_id == ofp.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=dp, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        dp.send_msg(out)

    # -------------------------
    # Helper: add flow
    # -------------------------
    def _add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0, hard_timeout=0):
        ofp = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout, hard_timeout=hard_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath,
                                    priority=priority, match=match,
                                    instructions=inst, idle_timeout=idle_timeout, hard_timeout=hard_timeout)
        datapath.send_msg(mod)

    # -------------------------
    # Periodic monitor: request stats
    # -------------------------
    def _monitor(self):
        while True:
            try:
                dps = list(self.datapaths.values())
                for dp in dps:
                    try:
                        self._request_flow_stats(dp)
                    except Exception as e:
                        LOG.debug(f"Monitor: failed request stats for {getattr(dp,'id',None)}: {e}")
                hub.sleep(self.STATS_INTERVAL)
            except Exception as e:
                LOG.exception(f"Monitor thread exception: {e}")
                hub.sleep(5)

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
            LOG.debug(f"DP {dpid:016x} Flow {match} packets={stat.packet_count} bytes={stat.byte_count}")

    # -------------------------
    # Keepalive: echo requests
    # -------------------------
    def _echo_loop(self):
        while True:
            try:
                for dp in list(self.datapaths.values()):
                    try:
                        parser = dp.ofproto_parser
                        echo = parser.OFPEchoRequest(dp)
                        dp.send_msg(echo)
                    except Exception as e:
                        LOG.debug(f"Echo failed for {getattr(dp,'id',None)}: {e}")
                hub.sleep(self.ECHO_INTERVAL)
            except Exception as e:
                LOG.exception(f"Echo loop exception: {e}")
                hub.sleep(5)
