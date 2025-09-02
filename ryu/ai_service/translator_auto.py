#!/usr/bin/python3
import os, re, json, socket, subprocess, yaml, requests

# --------------------------
# CONFIG
# --------------------------
RYU_REST = "http://127.0.0.1:8080"
RYU_FLOW_ADD = f"{RYU_REST}/stats/flowentry/add"
DPID = 1
DEFAULT_PRIORITY = 200

# Load apps db
APP_DB_FILE = "apps.yaml"
with open(APP_DB_FILE,"r") as f:
    APP_DB = yaml.safe_load(f)

LOCAL_MAP = {
    "youtube.com":"10.0.0.1",
    "netflix.com":"10.0.0.2",
    "twitch.tv":"10.0.0.3"
}

INTENT_LABELS=["block","allow","prioritize","throttle"]

# --------------------------
# UTIL
# --------------------------
def parse_bandwidth(prompt:str):
    m=re.search(r'(\d+)\s*(kbps|mbps|gbps|bps)', prompt.lower())
    if not m: return None
    val=int(m.group(1))
    unit=m.group(2)
    factor={"bps":1,"kbps":1000,"mbps":1000000,"gbps":1000000000}
    return val*factor[unit]

def resolve_domain(domain:str):
    if domain in LOCAL_MAP: return [LOCAL_MAP[domain]]
    try:
        import dns.resolver
        answers=dns.resolver.resolve(domain,'A')
        return [a.to_text() for a in answers]
    except:
        try:
            return socket.gethostbyname_ex(domain)[2]
        except:
            return ["10.0.0.1"]

def ip_to_cidr(ip:str): return [ip+"/32"]

def app_to_cidrs(app:str):
    if app in LOCAL_MAP: return [LOCAL_MAP[app]+"/32"]
    return APP_DB.get("apps",{}).get(app,{}).get("cidrs",["0.0.0.0/0"])

# --------------------------
# PARSER
# --------------------------
def parse_prompt(prompt:str):
    p=prompt.lower()
    if any(w in p for w in ["blokir","block","deny"]): intent="block"
    elif any(w in p for w in ["prioritas","prioritaskan","utamakan","priority"]): intent="prioritize"
    elif any(w in p for w in ["batasi","limit","throttle"]): intent="throttle"
    else: intent="allow"

    targets=[]
    proto="any"
    if "tcp" in p: proto="tcp"
    if "udp" in p: proto="udp"
    for app in APP_DB.get("apps",{}):
        if app in p and app not in targets: targets.append(app)
    domains=re.findall(r'([a-z0-9\-]+\.(?:com|net|org|io|tv|id))', p)
    targets.extend(domains)
    bw=parse_bandwidth(p)
    return {"intent":intent,"targets":targets or ["general"],"proto":proto,"bandwidth":bw,"priority":None}

# --------------------------
# QoS
# --------------------------
def ensure_qos(port:str, queue_id:int, rate:int=None):
    rate=rate or 1_000_000_000
    # OVS HTB queue
    cmd_ovs=f"""
    ovs-vsctl -- --id=@newqos create qos type=linux-htb other-config:max-rate=1000000000 queues={queue_id}=@q{queue_id} \
    -- --id=@q{queue_id} create queue other-config:max-rate={rate} \
    -- set port {port} qos=@newqos
    """
    subprocess.run(cmd_ovs,shell=True)
    # TC TBF
    if rate:
        rate_mbit=rate/1_000_000
        cmd_tc=f"""
        tc qdisc del dev {port} root 2>/dev/null
        tc qdisc add dev {port} root tbf rate {rate_mbit}mbit burst 4kb latency 50ms
        """
        subprocess.run(cmd_tc,shell=True)

# --------------------------
# BUILD FLOW
# --------------------------
def build_flows(parsed):
    flows=[]
    intent=parsed.get("intent","allow")
    proto=parsed.get("proto","any")

    for t in parsed.get("targets", []):
        cidrs=[]
        if t in APP_DB.get("apps", {}):
            for d in APP_DB["apps"][t].get("cidrs", []):
                for ip in resolve_domain(d):
                    cidrs.append(ip+"/32")
        else:
            for ip in resolve_domain(t):
                cidrs.append(ip+"/32")

        for c in cidrs:
            match={"eth_type":2048,"ipv4_dst":c}
            if proto=="tcp": match["ip_proto"]=6
            if proto=="udp": match["ip_proto"]=17

            if intent=="block": actions=[]
            elif intent=="prioritize": actions=[{"type":"SET_QUEUE","queue_id":1},{"type":"OUTPUT","port":"NORMAL"}]
            elif intent=="throttle": actions=[{"type":"SET_QUEUE","queue_id":2},{"type":"OUTPUT","port":"NORMAL"}]
            else: actions=[{"type":"OUTPUT","port":"NORMAL"}]

            flows.append({"dpid":DPID,"priority":DEFAULT_PRIORITY,"match":match,"actions":actions})
    return flows

# --------------------------
# PUSH FLOW
# --------------------------
def push_flows(flows):
    res=[]
    for f in flows:
        try:
            r=requests.post(RYU_FLOW_ADD,json=f,timeout=5)
            res.append({"flow":f,"status":r.status_code})
        except Exception as e:
            res.append({"flow":f,"error":str(e)})
    return res

# --------------------------
# MAIN
# --------------------------
def translate_and_apply(prompt):
    parsed=parse_prompt(prompt)
    uplink="s1-eth1"
    if parsed.get("bandwidth"): ensure_qos(uplink,2,parsed["bandwidth"])
    flows=build_flows(parsed)
    push_result=push_flows(flows)
    return {"parsed":parsed,"flows":flows,"push":push_result,"uplink":uplink}

# --------------------------
# CLI
# --------------------------
if __name__=="__main__":
    import sys
    if len(sys.argv)<2:
        print("Usage: python translator_auto.py \"<prompt>\"")
        exit(1)
    prompt=" ".join(sys.argv[1:])
    out=translate_and_apply(prompt)
    print(json.dumps(out,indent=2))
