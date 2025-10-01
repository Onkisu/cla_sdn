# db.py
import psycopg2
import json
from config import DB_URI

def get_conn():
    return psycopg2.connect(DB_URI)

def insert_forecast_result(result):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO forecast_results (
            category, anomaly_score, traffic_trend, seasonality_pattern,
            created_at, latency_sla_prob, jitter_sla_prob, loss_sla_prob,
            category_sla_prob, argument, decision
        )
        VALUES (%s,%s,%s,%s,now(),%s,%s,%s,%s,%s,%s)
    """, (
        result["category"],
        result["anomaly_score"],
        result["traffic_trend"],
        json.dumps(result["seasonality_pattern"]),
        result["latency_sla_prob"],
        result["jitter_sla_prob"],
        result["loss_sla_prob"],
        json.dumps(result["category_sla_prob"]),
        result.get("argument"),
        result.get("decision")
    ))
    conn.commit()
    cur.close()
    conn.close()
