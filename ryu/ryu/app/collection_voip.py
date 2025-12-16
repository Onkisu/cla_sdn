from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from datetime import datetime
import psycopg2

# ================= DATABASE =================
DB_CONFIG = {
    'dbname': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'host': '103.181.142.165'
}

# ================= FLOW DEFINITIONS =================
FLOW_PROFILES = [
    {
        "label": "VoIP",
        "ip_proto": 17,      # UDP
        "tp_dst": 5060
    },
    {
        "label": "Background",
        "ip_proto": 6,       # TCP
        "tp_dst": None       # any
    }
]

class FlowStatsCollector(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}
        self.prev_stats = {}
        self.monitor_thread = hub.spawn(self._monitor)

    # ================= SWITCH REGISTER =================
    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, CONFIG_DISPATCHER])
    def _state_change_handler(self, ev):
        dp = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[dp.id] = dp
        elif dp.id in self.datapaths:
            del self.datapaths[dp.id]

    # ================= PERIODIC REQUEST =================
    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(1)

    def _request_stats(self, dp):
        ofp = dp.ofproto
        parser = dp.ofproto_parser
        req = parser.OFPFlowStatsRequest(dp)
        dp.send_msg(req)

    # ================= FLOW STATS HANDLER =================
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply(self, ev):
        now = datetime.now()
        dp = ev.msg.datapath
        dpid = f"{dp.id:016x}"

        for stat in ev.msg.body:
            if stat.priority == 0:
                continue

            match = stat.match
            ip_proto = match.get('ip_proto')
            tp_dst = match.get('udp_dst') or match.get('tcp_dst')

            label = self._classify_flow(ip_proto, tp_dst)
            if not label:
                continue

            key = (dpid, ip_proto, tp_dst)
            prev = self.prev_stats.get(key)

            if prev:
                time_diff = (now - prev['ts']).total_seconds()
                if time_diff > 0:
                    byte_diff = stat.byte_count - prev['bytes']
                    throughput = (byte_diff * 8) / 1_000_000 / time_diff

                    self._insert_db(
                        now, dpid, ip_proto, tp_dst,
                        byte_diff, stat.packet_count - prev['pkts'],
                        time_diff, throughput, label
                    )

                    self.logger.info(
                        "[%s] proto=%s dst=%s | %.2f Mbps",
                        label, ip_proto, tp_dst, throughput
                    )

            self.prev_stats[key] = {
                'bytes': stat.byte_count,
                'pkts': stat.packet_count,
                'ts': now
            }

    # ================= CLASSIFIER =================
    def _classify_flow(self, proto, tp_dst):
        for f in FLOW_PROFILES:
            if proto == f['ip_proto']:
                if f['tp_dst'] is None or f['tp_dst'] == tp_dst:
                    return f['label']
        return None

    # ================= DATABASE =================
    def _insert_db(self, ts, dpid, proto, tp_dst,
                   bytes_diff, pkts_diff,
                   duration, throughput, label):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traffic.flow_stats_
                (timestamp, dpid, ip_proto, tp_dst,
                 bytes_rx, pkts_rx,
                 duration_sec, throughput_mbps, traffic_label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                ts, dpid, proto, tp_dst,
                bytes_diff, pkts_diff,
                duration, throughput, label
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error("DB Error: %s", e)
