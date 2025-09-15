#!/usr/bin/python3
import os, re, json, socket, subprocess, yaml, requests, time 

# --------------------------
# CONFIG
# --------------------------
RYU_REST = "http://127.0.0.1:8080"
RYU_FLOW_ADD = f"{RYU_REST}/stats/flowentry/add"
DPID = 1
DEFAULT_PRIORITY = 200

APP_DB_FILE = "apps.yaml"
with open(APP_DB_FILE, "r") as f:
    APP_DB = yaml.safe_load(f)

LOCAL_MAP = {
    "youtube": "10.0.0.1",
    "netflix": "10.0.0.2",
    "twitch": "10.0.0.3"
}

INTENT_LABELS = ["block", "allow", "prioritize", "throttle"]
DEFAULT_UPLINK = os.environ.get("UPLINK_PORT", None)
DNS_CACHE_TTL = 300  # 5 minutes
_dns_cache = {}  # domain -> (timestamp, [ips])

# --------------------------
# UTIL
# --------------------------
def parse_bandwidth(prompt: str):
    m = re.search(r'(\d+)\s*(kbps|mbps|gbps|bps)', prompt.lower())
    if not m: 
        return None
    val = int(m.group(1))
    unit = m.group(2)
    factor = {"bps": 1, "kbps": 1000, "mbps": 1000000, "gbps": 1000000000}
    return val * factor[unit]

def resolve_domain(domain: str):
    now = time.time()
    if domain in _dns_cache:
        ts, ips = _dns_cache[domain]
        if now - ts < DNS_CACHE_TTL:
            return ips
    # existing resolution
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, 'A')
        ips = [a.to_text() for a in answers]
    except Exception:
        try:
            ips = socket.gethostbyname_ex(domain)[2]
        except Exception:
            ips = ["10.0.0.1"]
    _dns_cache[domain] = (now, ips)
    return ips
    
# def resolve_domain(domain: str):
#     if domain in LOCAL_MAP:
#         return [LOCAL_MAP[domain]]
#     try:
#         import dns.resolver
#         answers = dns.resolver.resolve(domain, 'A')
#         return [a.to_text() for a in answers]
#     except:
#         try:
#             return socket.gethostbyname_ex(domain)[2]
#         except:
#             return ["10.0.0.1"]

def expand_targets(word: str):
    """Return list of apps/cidrs from a target word (app or category)."""
    apps = []
    if word in APP_DB.get("apps", {}):
        apps.append(word)
    elif word in APP_DB.get("categories", {}):
        apps.extend(APP_DB["categories"][word].get("apps", []))
    return apps or [word]

# --------------------------
# PARSER
# --------------------------
def parse_prompt(prompt: str):
    p = prompt.lower()
    # intent
    if any(w in p for w in ["blokir", "block", "deny"]): 
        intent = "block"
    elif any(w in p for w in ["prioritas", "prioritaskan", "utamakan", "priority"]): 
        intent = "prioritize"
    elif any(w in p for w in ["batasi", "limit", "throttle"]): 
        intent = "throttle"
    else: 
        intent = "allow"

    # proto
    proto = "any"
    if "tcp" in p: proto = "tcp"
    if "udp" in p: proto = "udp"

    # targets: apps or categories
    targets = []
    for key in list(APP_DB.get("apps", {})) + list(APP_DB.get("categories", {})):
        if key in p and key not in targets:
            targets.append(key)

    # fallback: domain regex
    domains = re.findall(r'([a-z0-9\-]+\.(?:com|net|org|io|tv|id))', p)
    targets.extend(domains)

    # expand categories into apps
    expanded_targets = []
    for t in targets or ["general"]:
        expanded_targets.extend(expand_targets(t))

    bw = parse_bandwidth(p)
    return {
        "intent": intent,
        "targets": expanded_targets,
        "proto": proto,
        "bandwidth": bw,
        "priority": None
    }

# --------------------------
# QoS
# --------------------------
def ensure_qos(port: str, queue_id: int, rate: int = None):
    """Ensure OVS queue exists with given rate (bps)."""
    rate = rate or 1_000_000_000
    cmd_ovs = f"""
    ovs-vsctl -- --id=@newqos create qos type=linux-htb other-config:max-rate=1000000000 queues={queue_id}=@q{queue_id} \
    -- --id=@q{queue_id} create queue other-config:max-rate={rate} \
    -- set port {port} qos=@newqos
    """
    subprocess.run(cmd_ovs, shell=True)
    # TC TBF as backup
    if rate:
        rate_mbit = rate / 1_000_000
        cmd_tc = f"""
        tc qdisc del dev {port} root 2>/dev/null
        tc qdisc add dev {port} root tbf rate {rate_mbit}mbit burst 4kb latency 50ms
        """
        subprocess.run(cmd_tc, shell=True)

# --------------------------
# BUILD FLOW
# --------------------------
def build_flows(parsed):
    flows = []
    intent = parsed.get("intent", "allow")
    proto = parsed.get("proto", "any")

    for t in parsed.get("targets", []):
        cidrs = []
        if t in APP_DB.get("apps", {}):
            for d in APP_DB["apps"][t].get("cidrs", []):
                for ip in resolve_domain(d):
                    cidrs.append(ip + "/32")
        else:
            for ip in resolve_domain(t):
                cidrs.append(ip + "/32")

        for c in cidrs:
            match = {"eth_type": 2048, "ipv4_dst": c}
            if proto == "tcp": match["ip_proto"] = 6
            if proto == "udp": match["ip_proto"] = 17

            if intent == "block":
                actions = []
            elif intent == "prioritize":
                actions = [{"type": "SET_QUEUE", "queue_id": 1}, {"type": "OUTPUT", "port": "NORMAL"}]
            elif intent == "throttle":
                actions = [{"type": "SET_QUEUE", "queue_id": 2}, {"type": "OUTPUT", "port": "NORMAL"}]
            else:
                actions = [{"type": "OUTPUT", "port": "NORMAL"}]

            flows.append({
                "dpid": DPID,
                "priority": DEFAULT_PRIORITY,
                "match": match,
                "actions": actions
            })
    return flows

# --------------------------
# PUSH FLOW
# --------------------------
def push_flows(flows):
    res = []
    for f in flows:
        try:
            r = requests.post(RYU_FLOW_ADD, json=f, timeout=5)
            res.append({"flow": f, "status": r.status_code})
        except Exception as e:
            res.append({"flow": f, "error": str(e)})
    return res

# --------------------------
# MAIN API
# --------------------------
def translate_and_apply(prompt, uplink_override=None):
    parsed = parse_prompt(prompt)
    uplink = uplink_override or DEFAULT_UPLINK or "s1-eth1"

    # QoS setup if throttle with bandwidth
    if parsed.get("bandwidth") and parsed["intent"] == "throttle":
        ensure_qos(uplink, 2, parsed["bandwidth"])

    flows = build_flows(parsed)
    push_result = push_flows(flows)
    return {"parsed": parsed, "flows": flows, "push": push_result, "uplink": uplink}

# --------------------------
# CLI
# --------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python translator_auto.py \"<prompt>\"")
        exit(1)
    prompt = " ".join(sys.argv[1:])
    out = translate_and_apply(prompt)
    print(json.dumps(out, indent=2))
