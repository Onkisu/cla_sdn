# ai_service/translator_auto.py
import os, re, json, socket, requests, yaml
from typing import List, Dict, Any

# config
RYU_REST = os.getenv("RYU_REST", "http://103.181.142.165:8080")
RYU_FLOW_ADD = f"{RYU_REST}/stats/flowentry/add"
DPID = int(os.getenv("RYU_DPID", "1"))
DEFAULT_PRIORITY = int(os.getenv("FLOW_PRIORITY", "200"))
APP_DB_FILE = os.path.join(os.path.dirname(__file__), "apps.yaml")

# load apps db
with open(APP_DB_FILE, "r") as f:
    APP_DB = yaml.safe_load(f)

# try to use local llama (llama-cpp-python) if available (best: no rate limit)
USE_LLAMA = False
try:
    from llama_cpp import Llama
    # set model path via env LLM_PATH or default None -> user must supply model path
    LLM_PATH = os.getenv("LLM_PATH", None)
    if LLM_PATH and os.path.exists(LLM_PATH):
        llama = Llama(model_path=LLM_PATH)
        USE_LLAMA = True
    else:
        llama = None
except Exception:
    llama = None
    USE_LLAMA = False

# fallback transformer-based zero-shot or heuristics
USE_TRANSFORMERS = False
try:
    from transformers import pipeline
    # zero-shot classifier (local HF)
    zclf = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    ner = pipeline("ner", model="dslim/bert-base-NER", aggregation_strategy="simple")
    USE_TRANSFORMERS = True
except Exception:
    zclf = None
    ner = None
    USE_TRANSFORMERS = False

INTENT_LABELS = ["block", "prioritize", "allow", "throttle"]

def parse_with_llama(prompt: str) -> Dict[str,Any]:
    """
    Use local LLM (llama.cpp) to produce strict JSON.
    You should set LLM_PATH env var to a local GGML model file.
    """
    system = "You are a network intent extractor. Output JSON only: {intent, targets(list), proto, time_window(start,end), bandwidth, priority}."
    # craft prompt
    p = f"{system}\nUser: {prompt}\nReturn only JSON."
    out = llama.create(prompt=p, max_tokens=300, temperature=0.0)
    txt = out.get("choices", [{}])[0].get("text","").strip()
    # extract JSON substring
    m = re.search(r'\{.*\}', txt, re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except:
            pass
    # fallback to heuristic
    return parse_heuristic(prompt)

def parse_with_hf(prompt: str) -> Dict[str,Any]:
    # zero-shot classification for intent
    res = zclf(prompt, INTENT_LABELS)
    intent = res["labels"][0]
    # NER for targets
    entities = ner(prompt)
    targets=[]
    for e in entities:
        w = e.get("word","").lower()
        # simple mapping
        if "." in w or any(c.isdigit() for c in w):
            targets.append(w)
    # apps by keyword
    for app in APP_DB.get("apps",{}):
        if app in prompt.lower() and app not in targets:
            targets.append(app)
    # proto/time/bw
    proto = "any"
    if "udp" in prompt.lower(): proto="udp"
    if "tcp" in prompt.lower(): proto="tcp"
    # time window parse basic
    m = re.search(r'(\d{1,2})(?::?(\d{1,2}))?\s*(am|pm|pagi|siang|sore|malam)?\s*(?:-|to|sampai|hingga)\s*(\d{1,2})(?::?(\d{1,2}))?', prompt.lower())
    tw=None
    if m:
        def to24(h,mn,part):
            h=int(h); mn=int(mn or 0)
            if part and part in ["pm","sore","malam"] and h<12: h+=12
            if part and part in ["am","pagi"] and h==12: h=0
            return f"{h:02d}:{mn:02d}"
        tw={"start":to24(m.group(1),m.group(2),m.group(3)),"end":to24(m.group(4),m.group(5),None)}
    return {"intent":intent,"targets":targets or ["general"],"proto":proto,"time_window":tw,"bandwidth":None,"priority":None}

def parse_heuristic(prompt: str) -> Dict[str,Any]:
    p = prompt.lower()
    if any(w in p for w in ["blokir","block","deny","tutup"]): intent="block"
    elif any(w in p for w in ["prioritas","prioritaskan","utamakan","priority"]): intent="prioritize"
    elif any(w in p for w in ["batasi","limit","throttle"]): intent="throttle"
    elif any(w in p for w in ["izinkan","allow","buka"]): intent="allow"
    else: intent="allow"
    proto="any"
    if "udp" in p: proto="udp"
    if "tcp" in p: proto="tcp"
    targets=[]
    # domain-like tokens
    domains = re.findall(r'([a-z0-9\-]+\.(?:com|net|org|io|tv|id))', p)
    targets.extend(domains)
    # known app keywords
    for app in APP_DB.get("apps",{}):
        if app in p and app not in targets:
            targets.append(app)
    if not targets: targets=["general"]
    return {"intent":intent,"targets":targets,"proto":proto,"time_window":None,"bandwidth":None,"priority":None}

def resolve_domain(domain:str)->List[str]:
    ips=[]
    try:
        # use dnspython if installed
        import dns.resolver
        answers = dns.resolver.resolve(domain,'A')
        for a in answers: ips.append(a.to_text())
    except Exception:
        try:
            res=socket.gethostbyname_ex(domain)
            ips=res[2]
        except:
            ips=[]
    return list(set(ips))

def ip_to_cidr(ip:str)->List[str]:
    # try ipwhois if available
    cidrs=[]
    try:
        from ipwhois import IPWhois
        obj = IPWhois(ip)
        res = obj.lookup_rdap(asn_methods=['whois'])
        net = res.get('network',{})
        cid = net.get('cidr')
        if cid:
            cidrs.append(cid)
    except Exception:
        pass
    if not cidrs: cidrs.append(ip+"/32")
    return cidrs

def app_to_cidrs(app:str)->List[str]:
    meta=APP_DB.get("apps",{}).get(app,{})
    return meta.get("cidrs",["0.0.0.0/0"])

def build_flows(parsed:Dict[str,Any])->List[Dict[str,Any]]:
    flows=[]
    intent=parsed.get("intent","allow")
    proto=parsed.get("proto","any")
    for t in parsed.get("targets",[]):
        cidrs=[]
        # if target is 'gaming' or matches app 'gaming'
        if t in ("gaming","game","games"):
            # create flows for gaming port ranges: match UDP/TCP ports, set queue 1
            ports = []
            for v in GAMING_PORTS.values():
                ports.extend(v)
            ports = sorted(set(ports))
            for port in ports:
                # prefer UDP (many games use UDP) but also add TCP match
                # UDP match
                match_udp = {"eth_type":2048,"ip_proto":17,"udp_dst":port}
                actions_udp = []
                if intent=="block":
                    actions_udp = []  # drop
                elif intent=="prioritize":
                    actions_udp=[{"type":"SET_QUEUE","queue_id":1},{"type":"OUTPUT","port":"NORMAL"}]
                elif intent=="throttle":
                    actions_udp=[{"type":"SET_QUEUE","queue_id":2},{"type":"OUTPUT","port":"NORMAL"}]
                else:
                    actions_udp=[{"type":"OUTPUT","port":"NORMAL"}]
                flows.append({"dpid":DPID,"priority":DEFAULT_PRIORITY+10,"match":match_udp,"actions":actions_udp})

                # TCP match
                match_tcp = {"eth_type":2048,"ip_proto":6,"tcp_dst":port}
                actions_tcp = actions_udp.copy()
                flows.append({"dpid":DPID,"priority":DEFAULT_PRIORITY+10,"match":match_tcp,"actions":actions_tcp})
            continue

        # existing logic for IP / domain / app name
        if re.match(r'^\d+\.\d+\.\d+\.\d+$', t):
            cidrs=ip_to_cidr(t)
        elif '.' in t: # domain
            ips=resolve_domain(t)
            for ip in ips: cidrs.extend(ip_to_cidr(ip))
            if not cidrs: cidrs.append("0.0.0.0/0")
        else: # app name fallback
            cidrs=app_to_cidrs(t)

        for c in cidrs:
            match={"eth_type":2048,"ipv4_dst":c}
            if proto=="tcp": match["ip_proto"]=6
            if proto=="udp": match["ip_proto"]=17
            if intent=="block":
                actions=[]  # drop
            elif intent=="prioritize":
                actions=[{"type":"SET_QUEUE","queue_id":1},{"type":"OUTPUT","port":"NORMAL"}]
            elif intent=="throttle":
                actions=[{"type":"SET_QUEUE","queue_id":2},{"type":"OUTPUT","port":"NORMAL"}]
            else:
                actions=[{"type":"OUTPUT","port":"NORMAL"}]
            flows.append({"dpid":DPID,"priority":DEFAULT_PRIORITY,"match":match,"actions":actions})
    return flows

def push_flows(flows:List[Dict[str,Any]]):
    res=[]
    for f in flows:
        try:
            r=requests.post(RYU_FLOW_ADD,json=f,timeout=5)
            res.append({"flow":f,"status":r.status_code,"text":r.text})
        except Exception as e:
            res.append({"flow":f,"error":str(e)})
    return res

def translate_and_apply(prompt:str)->Dict[str,Any]:
    # prefer llama local
    parsed=None
    if USE_LLAMA and llama:
        try:
            parsed=parse_with_llama(prompt)
        except Exception as e:
            parsed=None
    if parsed is None and USE_TRANSFORMERS:
        try:
            parsed=parse_with_hf(prompt)
        except Exception:
            parsed=None
    if parsed is None:
        parsed=parse_heuristic(prompt)
    # ensure targets
    if not parsed.get("targets"):
        parsed["targets"]=["general"]
    flows=build_flows(parsed)
    push_result=push_flows(flows)
    return {"parsed":parsed,"flows":flows,"push":push_result}

# CLI helper
if __name__=="__main__":
    import sys
    if len(sys.argv)<2:
        print("Usage: python translator_auto.py \"<prompt>\"")
        exit(1)
    prompt=" ".join(sys.argv[1:])
    out=translate_and_apply(prompt)
    print(json.dumps(out,indent=2))

    # di atas file: tambahkan GAMING_PORTS constant
GAMING_PORTS = {
    # common game ports (contoh, perlu di-tune sesuai game)
    # key = name (not necessary), values = list of ports
    "common": [27015, 27016, 27017, 27036,    # steam/valve
               5000, 3478, 4379, 4380,        # generic STUN etc
               7000, 7001, 8000,              # some games
               10000, 12000,                  # examples / UDP ranges
               27000, 27005],                 # Dota2/steam common
    # add more ranges if needed, or parse service names
}


