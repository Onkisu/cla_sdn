# decision.py
def decide_action(result):
    # Simple policy: if SLA < 0.8 → reroute
    if result["latency_sla_prob"] < 0.8 or result["jitter_sla_prob"] < 0.8:
        return "reroute_high_priority"
    elif result["anomaly_score"] > 0.7:
        return "throttle_low_priority"
    else:
        return "no_action"
