#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
forecast_adaptive.py

Self-tuning adaptive forecasting + decision system.

Features:
- Exact metric calculations (burstiness, anomaly, SLA probs)
- Prophet throughput forecasting (retrained each run)
- Dynamic SLA threshold (historical percentile / mean+std)
- Deterministic adaptive policy: choose action with best historical reward
- Log actions & outcomes to database (learning / feedback loop)
- Optional: push actions to Ryu via REST API
- Optional: get long-form reasoning from Gemini (for humans)
"""

import os
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
import requests

# Prophet import (pip install prophet)
from prophet import Prophet

# Optional Gemini: for long explanations (not required for adaptation)
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except Exception:
    GEMINI_AVAILABLE = False

# -----------------------
# CONFIG
# -----------------------
DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
RYU_REST_URL = os.getenv("RYU_REST_URL", "http://localhost:8080/stats/flowentry/add")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyCGQPVIWDMQ6zvspGsAjkUXshEAYeEFdOc")  # optional
# Safety defaults / SLA baseline
SLA_THROUGHPUT_MBPS_BASE = 0.3
SLA_LATENCY_MS_BASE = 50.0
SLA_JITTER_MS_BASE = 20.0
SLA_LOSS_PCT_BASE = 1.5

CATEGORIES = ["video", "gaming", "voip", "data"]

# Action space (strings)
ACTIONS = ["scale_up", "reroute", "rate_limit", "alert", "none"]

# Learning window: how many past action logs to consider when measuring success
ACTION_HISTORY_WINDOW = 1000

# Observation window: how many minutes to use for metrics/forecast
OBS_WINDOW_MINUTES = 120

# Post-action observation window to evaluate reward (minutes)
POST_ACTION_OBS_MINUTES = 10

# -----------------------
# DB helpers
# -----------------------
def run_sql(query, params=None, fetch=False):
    try:
        conn = psycopg2.connect(DB_DSN)
        cur = conn.cursor()
        cur.execute(query, params or ())
        rows = None
        if fetch:
            rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[DB ERROR] {e}\nQuery: {query}\nParams: {params}")
        return None

def ensure_tables_exist():
    # Table to store action logs and outcomes (for learning)
    run_sql("""
    CREATE TABLE IF NOT EXISTS forecast_actions_log (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMP NOT NULL DEFAULT NOW(),
        action TEXT NOT NULL,
        reason TEXT,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        pre_burstiness DOUBLE PRECISION,
        pre_anomaly DOUBLE PRECISION,
        pre_throughput DOUBLE PRECISION,
        post_burstiness DOUBLE PRECISION,
        post_anomaly DOUBLE PRECISION,
        post_throughput DOUBLE PRECISION,
        reward DOUBLE PRECISION,
        meta JSONB
    );
    """)
    # Keep your forecast summaries
    run_sql("""
    CREATE TABLE IF NOT EXISTS summary_forecast_hybrid (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMP NOT NULL DEFAULT NOW(),
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        burstiness_index DOUBLE PRECISION,
        anomaly_score DOUBLE PRECISION,
        traffic_trend DOUBLE PRECISION,
        latency_sla_prob DOUBLE PRECISION,
        jitter_sla_prob DOUBLE PRECISION,
        loss_sla_prob DOUBLE PRECISION,
        category_sla_prob JSONB,
        forecast_json JSONB
    );
    """)

# -----------------------
# Data loaders (DB)
# -----------------------
def load_recent_traffic_data(minutes=OBS_WINDOW_MINUTES):
    q = f"""
    SELECT 
        timestamp,
        SUM(bytes_tx + bytes_rx) AS total_bytes,
        AVG(latency_ms) AS avg_latency,
        STDDEV(latency_ms) AS avg_jitter,
        CASE WHEN SUM(pkts_tx) = 0 THEN 0
             ELSE 100.0 * (SUM(pkts_tx) - SUM(pkts_rx)) / SUM(pkts_tx)
        END AS avg_loss
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{minutes} minutes'
    GROUP BY timestamp
    ORDER BY timestamp;
    """
    df = None
    try:
        conn = psycopg2.connect(DB_DSN)
        df = pd.read_sql(q, conn)
        conn.close()
    except Exception as e:
        print(f"[LOAD ERROR] {e}")
        df = pd.DataFrame()
    return df

def load_category_traffic_data(minutes=OBS_WINDOW_MINUTES):
    q = f"""
    SELECT 
        timestamp,
        category,
        SUM(bytes_tx + bytes_rx) AS bytes,
        COUNT(*) AS flow_count
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{minutes} minutes'
    GROUP BY timestamp, category
    ORDER BY timestamp, category;
    """
    try:
        conn = psycopg2.connect(DB_DSN)
        df = pd.read_sql(q, conn)
        conn.close()
    except Exception as e:
        print(f"[LOAD ERROR] {e}")
        df = pd.DataFrame()
    return df

# -----------------------
# Exact metrics
# -----------------------
def calculate_burstiness(series):
    if series is None or series.empty or len(series) < 2:
        return 1.0
    values = series.values
    if np.mean(values) == 0:
        return 1.0
    return round(float(np.max(values) / np.mean(values)), 4)

def calculate_anomaly_iqr(series):
    if series is None or series.empty or len(series) < 5:
        return 0.0
    values = series.values
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    if IQR == 0:
        return 0.0
    last_value = values[-1]
    median = np.median(values[:-1])
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    if last_value < lower_bound or last_value > upper_bound:
        score = abs(last_value - median) / IQR
        return round(float(score), 4)
    return 0.0

def calculate_trend_mbps_per_min(series):
    if series is None or series.empty or len(series) < 3:
        return 0.0
    values = series.values
    x = np.arange(len(values))
    try:
        slope = np.polyfit(x, values, 1)[0]
        slope_mbps = (slope * 8) / 1e6
        return round(float(slope_mbps), 6)
    except Exception:
        return 0.0

def calculate_sla_prob(current_value, threshold, direction='gt'):
    if current_value is None:
        return 0.0
    try:
        if direction == 'lt':
            if current_value < threshold:
                violation_ratio = (threshold - current_value) / (threshold if threshold != 0 else 1)
                return round(min(0.99, violation_ratio), 4)
            return 0.0
        else:
            if current_value > threshold:
                violation_ratio = (current_value - threshold) / (threshold if threshold != 0 else 1)
                return round(min(0.99, violation_ratio), 4)
            return 0.0
    except Exception:
        return 0.0

# -----------------------
# Prophet forecast
# -----------------------
def forecast_throughput_prophet(traffic_df, periods=30):
    if traffic_df is None or traffic_df.empty:
        return pd.DataFrame()
    df = traffic_df[['timestamp', 'total_bytes']].copy()
    df['ds'] = pd.to_datetime(df['timestamp'])
    df['y'] = (df['total_bytes'] * 8) / 1e6  # bytes -> Mbps
    df = df[['ds', 'y']].dropna()
    if len(df) < 5:
        return pd.DataFrame()
    m = Prophet(daily_seasonality=False, weekly_seasonality=False, yearly_seasonality=False)
    m.fit(df)
    future = m.make_future_dataframe(periods=periods, freq='min')
    forecast = m.predict(future)
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# -----------------------
# Dynamic SLA calculation
# -----------------------
def dynamic_sla_from_history(traffic_df, method='percentile', percentile=5):
    """
    Compute dynamic SLA threshold for throughput:
    - method='percentile' => returns Xth percentile of historical throughput (Mbps)
    - method='mean_std' => returns mean - k * std (safer)
    """
    if traffic_df is None or traffic_df.empty:
        return SLA_THROUGHPUT_MBPS_BASE
    df = traffic_df.copy()
    df['throughput_mbps'] = (df['total_bytes'] * 8) / 1e6
    if df['throughput_mbps'].empty:
        return SLA_THROUGHPUT_MBPS_BASE
    if method == 'percentile':
        val = np.percentile(df['throughput_mbps'].values, percentile)
        return max(0.0, round(float(val), 6))
    else:
        mean = df['throughput_mbps'].mean()
        std = df['throughput_mbps'].std()
        threshold = max(0.0, mean - 2 * std)
        return round(float(threshold), 6)

# -----------------------
# Adaptive policy (deterministic)
# -----------------------
def get_action_stats(window=ACTION_HISTORY_WINDOW):
    """Return dict {action: {count, avg_reward}} from DB history (most recent window rows)"""
    rows = run_sql("SELECT action, reward FROM forecast_actions_log ORDER BY ts DESC LIMIT %s", (window,), fetch=True)
    stats = {a: {"count": 0, "avg_reward": 0.0} for a in ACTIONS}
    if not rows:
        return stats
    totals = {a: 0.0 for a in ACTIONS}
    counts = {a: 0 for a in ACTIONS}
    for r in rows:
        a, reward = r[0], r[1] if r[1] is not None else 0.0
        if a in totals:
            totals[a] += float(reward)
            counts[a] += 1
    for a in ACTIONS:
        stats[a]["count"] = counts[a]
        stats[a]["avg_reward"] = (totals[a] / counts[a]) if counts[a] > 0 else 0.0
    return stats

def select_action_adaptive(pre_metrics, forecast_df, sla_dynamic):
    """
    Deterministic selection:
    - Compute candidate actions and reasons
    - Use historical avg_reward to pick best action
    - If no history (all avg_reward == 0), pick safe default or 'none'
    """
    # Candidate list with simple heuristic scoring
    candidates = []

    # Example heuristics:
    # If predicted throughput median is below SLA -> scale_up or reroute
    if not forecast_df.empty:
        future_median = float(forecast_df['yhat'].median())
    else:
        future_median = pre_metrics['throughput_mbps']

    # If predicted below SLA or trend negative and small throughput
    if future_median < sla_dynamic * 0.9:
        candidates += [("scale_up", "Predicted throughput below dynamic SLA")]
        candidates += [("reroute", "Predicted localized deficit; reroute heavy flows")]
    # If high burstiness => rate limit
    if pre_metrics['burstiness'] > 1.0:
        candidates += [("rate_limit", "High burstiness detected")]
    # If many anomalies => alert & investigate
    if pre_metrics['anomaly'] > 1.5:
        candidates += [("alert", "Multiple anomalies detected")]
    # Always include 'none' as fallback
    candidates += [("none", "No direct action — monitor")]

    # Get action stats
    stats = get_action_stats()

    # Score candidates by historical avg_reward (higher is better). Deterministically pick max.
    best_action = None
    best_score = -9999.0
    for act, reason in candidates:
        score = stats.get(act, {}).get("avg_reward", 0.0)
        # Tie-breaking by preference order (scale_up > reroute > rate_limit > alert > none)
        pref_order = {"scale_up":5, "reroute":4, "rate_limit":3, "alert":2, "none":1}
        score = score + (pref_order.get(act,0) * 1e-6)  # tiny deterministic tie-breaker
        if score > best_score:
            best_score = score
            best_action = (act, reason)

    # If all avg_reward are zero (no history), pick deterministic safe default:
    all_zero = all(stats[a]["avg_reward"] == 0.0 for a in ACTIONS)
    if all_zero:
        # Choose conservative default based on immediate metrics
        if pre_metrics['anomaly'] > 2:
            return ("alert", "No history; anomalies high -> alert")
        if pre_metrics['burstiness'] > 1.0:
            return ("rate_limit", "No history; high burstiness -> rate_limit")
        if future_median < sla_dynamic * 0.9:
            return ("reroute", "No history; predicted deficit -> reroute")
        return ("none", "No history; default monitor")

    return best_action  # (action, reason)

# -----------------------
# Apply action to Ryu (optional)
# -----------------------
def apply_action_to_ryu(action):
    # This is a placeholder; adapt to your Ryu rule formats
    if not RYU_REST_URL:
        return False, "Ryu URL not configured"
    try:
        if action == "scale_up":
            # Example: install flow to prioritize certain traffic (customize)
            flow_rule = {
                "dpid": 1,
                "priority": 200,
                "match": {"ip_proto":6},  # example
                "actions": [{"type":"OUTPUT","port":3}]
            }
            r = requests.post(RYU_REST_URL, json=flow_rule, timeout=5)
            return r.status_code == 200, f"Ryu status {r.status_code}"
        elif action == "reroute":
            flow_rule = {
                "dpid": 1,
                "priority": 200,
                "match": {"in_port": 2},
                "actions": [{"type":"OUTPUT","port":4}]
            }
            r = requests.post(RYU_REST_URL, json=flow_rule, timeout=5)
            return r.status_code == 200, f"Ryu status {r.status_code}"
        elif action == "rate_limit":
            # Many Ryu setups need external policer; this is placeholder
            return False, "rate_limit requires external policer integration"
        elif action == "alert":
            # Just log / send alert (not integrated here)
            return False, "alert (no Ryu rule)"
        elif action == "none":
            return True, "no-op"
        else:
            return False, "unknown action"
    except Exception as e:
        return False, str(e)

# -----------------------
# Logging & reward computation
# -----------------------
def log_action_and_outcome(action, reason, window_start, window_end, pre_metrics, post_metrics, meta=None):
    # Reward definition (deterministic): positive reward if metrics improved after action:
    # reward = reduction in (burstiness + anomaly_count) normalized by pre values
    pre_score = float(pre_metrics.get('burstiness',0.0) + pre_metrics.get('anomaly',0.0))
    post_score = float(post_metrics.get('burstiness',0.0) + post_metrics.get('anomaly',0.0))
    if pre_score == 0:
        reward = 0.0
    else:
        reward = round(max(-1.0, min(1.0, (pre_score - post_score) / (pre_score))), 6)  # in [-1,1]

    run_sql("""
        INSERT INTO forecast_actions_log
        (action, reason, window_start, window_end,
         pre_burstiness, pre_anomaly, pre_throughput,
         post_burstiness, post_anomaly, post_throughput,
         reward, meta)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        action,
        reason,
        window_start,
        window_end,
        pre_metrics.get('burstiness'),
        pre_metrics.get('anomaly'),
        pre_metrics.get('throughput_mbps'),
        post_metrics.get('burstiness'),
        post_metrics.get('anomaly'),
        post_metrics.get('throughput_mbps'),
        reward,
        json.dumps(meta or {})
    ))
    return reward

# -----------------------
# Gemini explanation (optional, deterministic prompt)
# -----------------------
def ask_gemini_long_reasoning(metrics, forecast_df):
    if not GEMINI_AVAILABLE or GEMINI_API_KEY is None:
        return "Gemini not configured or unavailable."
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")
    summary = (
        "Network metrics summary:\n"
        f"- avg throughput (Mbps): {metrics['throughput_mbps']:.6f}\n"
        f"- burstiness: {metrics['burstiness']:.6f}\n"
        f"- anomaly score: {metrics['anomaly']:.6f}\n"
        f"- latency_prob: {metrics['latency_prob']:.6f}\n"
        f"- jitter_prob: {metrics['jitter_prob']:.6f}\n"
        f"- loss_prob: {metrics['loss_prob']:.6f}\n\n"
        "Recent forecast (last 5 rows):\n" +
        forecast_df[['ds','yhat','yhat_lower','yhat_upper']].tail(5).to_string(index=False) +
        "\n\nPlease provide a long, detailed reasoning about these results, "
        "including potential root causes, recommended operational actions, and expected effects."
    )
    resp = model.generate_content(summary)
    return resp.text

# -----------------------
# Main orchestration
# -----------------------
def compute_current_metrics(traffic_df, category_df):
    traffic_df = traffic_df.copy()
    traffic_df['timestamp'] = pd.to_datetime(traffic_df['timestamp'])
    bytes_series = traffic_df['total_bytes'].dropna() if 'total_bytes' in traffic_df else pd.Series(dtype=float)
    latency_series = traffic_df['avg_latency'].dropna() if 'avg_latency' in traffic_df else pd.Series(dtype=float)
    jitter_series = traffic_df['avg_jitter'].dropna() if 'avg_jitter' in traffic_df else pd.Series(dtype=float)
    loss_series = traffic_df['avg_loss'].dropna() if 'avg_loss' in traffic_df else pd.Series(dtype=float)

    throughput_mbps = float((bytes_series.iloc[-1] * 8) / 1e6) if not bytes_series.empty else 0.0
    burstiness = calculate_burstiness(bytes_series)
    anomaly = calculate_anomaly_iqr(bytes_series)
    trend = calculate_trend_mbps_per_min(bytes_series)
    latency_prob = calculate_sla_prob(latency_series.iloc[-1] if not latency_series.empty else None, SLA_LATENCY_MS_BASE, 'gt')
    jitter_prob = calculate_sla_prob(jitter_series.iloc[-1] if not jitter_series.empty else None, SLA_JITTER_MS_BASE, 'gt')
    loss_prob = calculate_sla_prob(loss_series.iloc[-1] if not loss_series.empty else None, SLA_LOSS_PCT_BASE, 'gt')

    return {
        'throughput_mbps': throughput_mbps,
        'burstiness': burstiness,
        'anomaly': anomaly,
        'trend_mbps_per_min': trend,
        'latency_prob': latency_prob,
        'jitter_prob': jitter_prob,
        'loss_prob': loss_prob
    }

def main_run():
    ensure_tables_exist()
    print(f"[{datetime.now()}] Starting adaptive forecast run...")

    # 1) Load data
    traffic_df = load_recent_traffic_data(minutes=OBS_WINDOW_MINUTES)
    category_df = load_category_traffic_data(minutes=OBS_WINDOW_MINUTES)
    if traffic_df is None or traffic_df.empty:
        print("No traffic data available. Exiting.")
        return

    # 2) Compute current metrics
    pre_metrics = compute_current_metrics(traffic_df, category_df)
    window_start = pd.to_datetime(traffic_df['timestamp'].min())
    window_end = pd.to_datetime(traffic_df['timestamp'].max())

    # 3) Dynamic SLA
    sla_dynamic = dynamic_sla_from_history(traffic_df, method='percentile', percentile=5)
    print(f"Dynamic SLA throughput (5th percentile): {sla_dynamic} Mbps")

    # 4) Forecast
    forecast_df = forecast_throughput_prophet(traffic_df, periods=30)
    if forecast_df.empty:
        print("Forecast unavailable (insufficient data). Proceeding with exact metrics only.")

    # 5) Prepare and save summary (DB)
    run_sql("""
        INSERT INTO summary_forecast_hybrid
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend,
         latency_sla_prob, jitter_sla_prob, loss_sla_prob, category_sla_prob, forecast_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        window_start,
        window_end,
        pre_metrics['burstiness'],
        pre_metrics['anomaly'],
        pre_metrics['trend_mbps_per_min'],
        pre_metrics['latency_prob'],
        pre_metrics['jitter_prob'],
        pre_metrics['loss_prob'],
        json.dumps({}),
        json.dumps(forecast_df.tail(10).to_dict(orient='records') if not forecast_df.empty else [])
    ))

    # 6) Choose action adaptively (deterministic)
    action, reason = select_action_adaptive(pre_metrics, forecast_df, sla_dynamic)
    print(f"Selected action: {action} — Reason: {reason}")

    # 7) Apply action to Ryu (or simulate)
    success, ryu_msg = apply_action_to_ryu(action)
    print(f"Action apply result: success={success}, info={ryu_msg}")

    # 8) Wait (or schedule) to collect post-action metrics for reward computation.
    # In a real deployment you'd wait POST_ACTION_OBS_MINUTES. Here we perform a short deterministic wait
    # to allow data to accumulate. You can increase this or run a separate collector later.
    wait_seconds = 5  # small, deterministic; in production use >= POST_ACTION_OBS_MINUTES * 60
    print(f"Gathering post-action metrics (waiting {wait_seconds}s)...")
    time.sleep(wait_seconds)

    # 9) Load recent data again for post metrics (using a slightly shorter window to focus on after-action)
    post_df = load_recent_traffic_data(minutes=POST_ACTION_OBS_MINUTES)
    if post_df is None or post_df.empty:
        print("No post-action data available; logging with zeroed post metrics.")
        post_metrics = {'throughput_mbps': pre_metrics['throughput_mbps'], 'burstiness': pre_metrics['burstiness'], 'anomaly': pre_metrics['anomaly']}
    else:
        post_metrics = compute_current_metrics(post_df, category_df)

    # 10) Log action outcome and compute reward
    reward = log_action_and_outcome(action, reason, window_start, window_end, pre_metrics, post_metrics, meta={"ryu_msg": ryu_msg, "success": success})
    print(f"Logged action with reward {reward}")

    # 11) Optional: get Gemini long reasoning for human operator
    if GEMINI_AVAILABLE and GEMINI_API_KEY:
        try:
            reasoning = ask_gemini_long_reasoning(pre_metrics, forecast_df if not forecast_df.empty else pd.DataFrame())
            print("\n--- GEMINI LONG REASONING ---\n")
            print(reasoning)
            print("\n--- END GEMINI ---\n")
        except Exception as e:
            print(f"Gemini reasoning failed: {e}")

    print(f"[{datetime.now()}] Adaptive run completed.")

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    main_run()
