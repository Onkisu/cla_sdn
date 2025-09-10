#!/usr/bin/env python3
"""
forecast_full.py
- Extended forecasting for throughput (total + per-app), latency, jitter, packet loss
- Calculates SLA violation probabilities
- Inserts summary into traffic.summary_forecast_v2
- Creates alerts/actions via translator_auto.translate_and_apply when SLA risk high

Requirements:
- prophet, pandas, numpy, psycopg2, translator_auto.py in same folder (with apps.yaml)
- traffic.flow_stats contains columns: timestamp, bytes_tx, bytes_rx, pkts_tx, pkts_rx, app, latency_ms
"""

import os
import json
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from prophet import Prophet

# translator_auto must exist and expose translate_and_apply(prompt)
from translator_auto import translate_and_apply

DB_DSN = os.environ.get("DB_DSN", "dbname=development user=dev_one password=hijack332. host=127.0.0.1")

# SLA thresholds (example values; adjust to your SLAs)
SLA_THROUGHPUT_MBPS = 5.0   # Mbps minimal throughput per-app
SLA_LATENCY_MS = 50.0       # max latency in ms
SLA_JITTER_MS = 30.0        # max jitter in ms
SLA_LOSS_PCT = 1.0         # max packet loss percent

# Decision thresholds for actions
SLA_VIOLATION_PROB_TH = 0.30  # if prob > 30% -> trigger action
ANOMALY_ACTION_TH = 5.0       # anomaly score threshold (MAD)

# apps to check per-app throughput (must match your apps.yaml)
MONITORED_APPS = ["youtube", "netflix", "twitch"]

# helper DB functions
def run_sql(query, params=None, fetch=False):
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

def ensure_summary_columns():
    """Ensure summary table has columns for storing SLA probs & per-app JSONB."""
    q = """
    ALTER TABLE traffic.summary_forecast_v2
    ADD COLUMN IF NOT EXISTS latency_sla_prob NUMERIC,
    ADD COLUMN IF NOT EXISTS jitter_sla_prob NUMERIC,
    ADD COLUMN IF NOT EXISTS loss_sla_prob NUMERIC,
    ADD COLUMN IF NOT EXISTS app_sla_prob JSONB;
    """
    run_sql(q)

# data loaders
def load_time_series_for_kpi(sql_query):
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql(sql_query, conn)
    conn.close()
    if df.empty:
        return df
    # rename timestamp column to ds and value to y if necessary depending on query
    return df

def load_agg_bytes(days=30):
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, SUM(bytes_tx) AS y
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{days} days'
    GROUP BY ds ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_app_bytes(app, days=30):
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, SUM(bytes_tx) AS y
    FROM traffic.flow_stats
    WHERE app = %s AND timestamp >= NOW() - interval '{days} days'
    GROUP BY ds ORDER BY ds;
    """
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql(q, conn, params=(app,))
    conn.close()
    return df

def load_latency_series(days=7):
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, AVG(latency_ms) AS y
    FROM traffic.flow_stats
    WHERE latency_ms IS NOT NULL AND timestamp >= NOW() - interval '{days} days'
    GROUP BY ds ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_jitter_series(days=7):
    # jitter approximated as stddev of latency per minute
    q = f"""
    SELECT ds, stddev(lat) AS y FROM (
      SELECT date_trunc('minute', timestamp) AS ds, latency_ms AS lat
      FROM traffic.flow_stats
      WHERE latency_ms IS NOT NULL AND timestamp >= NOW() - interval '{days} days'
    ) s GROUP BY ds ORDER BY ds;
    """
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql(q, conn)
    conn.close()
    return df

def load_loss_series(days=7):
    # loss% per minute = 100 * (tx - rx) / tx  (guard tx==0)
    q = f"""
    SELECT ds, CASE WHEN sum_tx = 0 THEN 0 ELSE (100.0 * (sum_tx - sum_rx) / sum_tx) END AS y FROM (
      SELECT date_trunc('minute', timestamp) AS ds,
             SUM(COALESCE(pkts_tx,0)) AS sum_tx,
             SUM(COALESCE(pkts_rx,0)) AS sum_rx
      FROM traffic.flow_stats
      WHERE timestamp >= NOW() - interval '{days} days'
      GROUP BY ds
    ) t ORDER BY ds;
    """
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql(q, conn)
    conn.close()
    return df

# metrics
def burstiness_index(series):
    if series.empty or series.mean() == 0:
        return 0.0
    return round(series.max() / series.mean(), 2)

def anomaly_score_mad(series):
    if series.empty:
        return 0.0
    median = series.median()
    mad = np.median(np.abs(series - median))
    if mad == 0:
        if series.std() == 0:
            return 0.0
        z = (series - series.mean()) / series.std()
        return round(abs(z.iloc[-1]), 2)
    score = abs(series.iloc[-1] - median) / mad
    return round(float(score), 2)

def traffic_trend(series):
    if series.empty or len(series) < 2:
        return 0.0
    slope = np.polyfit(range(len(series)), series, 1)[0]
    return float(slope)

def slope_to_mbps(slope_bytes_per_min):
    return (slope_bytes_per_min * 8.0) / 1e6

# forecasting utilities
def prophet_forecast(df, periods=15, freq='min'):
    """
    Input df must have columns ['ds','y'], ds timezone-naive
    Returns forecast dataframe (with columns yhat, yhat_lower, yhat_upper, and seasonal* columns)
    """
    if df.empty or len(df) < 3:
        return None
    # ensure Prophet gets proper ds type
    df = df.copy()
    df['ds'] = pd.to_datetime(df['ds'])
    m = Prophet(daily_seasonality=True, yearly_seasonality=False)
    # short cycles helpful for network-level seasonality
    m.add_seasonality(name='hour_cycle', period=60, fourier_order=4)
    m.add_seasonality(name='quarter_cycle', period=15, fourier_order=3)
    m.fit(df)
    future = m.make_future_dataframe(periods=periods, freq=freq)
    forecast = m.predict(future)
    return forecast

def sla_violation_prob_from_forecast(forecast, threshold, kpi_col='yhat'):
    """
    Simple empirical estimate: fraction of forecast points where predicted KPI < threshold.
    forecast: DataFrame from Prophet.predict
    """
    if forecast is None or forecast.empty:
        return 0.0
    if kpi_col not in forecast.columns:
        kpi_col = 'yhat'
    below = (forecast[kpi_col] < threshold).sum()
    prob = float(below) / float(len(forecast))
    return round(prob, 4)

# DB insert helpers
def save_summary(window_start, window_end, burst, anomaly, trend_mbps,
                 seasonality, latency_prob, jitter_prob, loss_prob, app_probs):
    ensure_summary_columns()
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.summary_forecast_v2
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, seasonality_pattern, latency_sla_prob, jitter_sla_prob, loss_sla_prob, app_sla_prob)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (window_start, window_end, burst, anomaly, trend_mbps, json.dumps(seasonality), latency_prob, jitter_prob, loss_prob, json.dumps(app_probs)))
    conn.commit()
    cur.close()
    conn.close()

def insert_alert(level, msg, anomaly_score=None, dpid=None, src_ip=None, dst_ip=None, flow_id=None):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.alerts(level,msg,anomaly_score,dpid,src_ip,dst_ip,flow_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (level, msg, anomaly_score, dpid, src_ip, dst_ip, flow_id))
    alert_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return alert_id

def insert_action(action, params, outcome, ref_alert_id):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.actions(action,params_json,outcome,reference_alert_id)
        VALUES (%s,%s,%s,%s)
    """, (action, json.dumps(params), outcome, ref_alert_id))
    conn.commit()
    cur.close()
    conn.close()

# decision logic based on SLA probs
def decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly):
    """
    If any SLA probability exceed SLA_VIOLATION_PROB_TH, raise alert and take action:
      - For per-app high prob: throttle that app
      - For latency/jitter/loss high prob: create network-level alert (and optionally reroute)
    """
    actions_taken = []
    # latency/jitter/loss
    if latency_prob > SLA_VIOLATION_PROB_TH:
        msg = f"Latency SLA risk: prob={latency_prob}"
        aid = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
        # For demo, trigger prioritization of interactive apps
        out = translate_and_apply("prioritize gaming")
        insert_action("prioritize", out, "ok", aid)
        actions_taken.append(("prioritize", "gaming", aid))
    if jitter_prob > SLA_VIOLATION_PROB_TH:
        msg = f"Jitter SLA risk: prob={jitter_prob}"
        aid = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
        out = translate_and_apply("prioritize voip")
        insert_action("prioritize", out, "ok", aid)
        actions_taken.append(("prioritize", "voip", aid))
    if loss_prob > SLA_VIOLATION_PROB_TH:
        msg = f"Packet loss SLA risk: prob={loss_prob}"
        aid = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
        # example action: throttle heavy bulk apps
        out = translate_and_apply("throttle netflix 5mbps")
        insert_action("throttle", out, "ok", aid)
        actions_taken.append(("throttle", "netflix", aid))

    # per-app
    for app, prob in (app_probs or {}).items():
        if prob >= SLA_VIOLATION_PROB_TH:
            msg = f"App {app} SLA risk: prob={prob}"
            aid = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
            # choose action: throttle if high prob, otherwise prioritize
            out = translate_and_apply(f"throttle {app} 2mbps")
            insert_action("throttle", out, "ok", aid)
            actions_taken.append(("throttle", app, aid))

    # burst/anomaly overrides
    if anomaly >= ANOMALY_ACTION_TH:
        msg = f"High anomaly detected (score={anomaly})"
        aid = insert_alert("critical", msg, anomaly_score=anomaly, dpid=1)
        # pick top talker and block/throttle; here we throttle youtube as example
        out = translate_and_apply("throttle youtube 1mbps")
        insert_action("throttle", out, "ok", aid)
        actions_taken.append(("throttle", "youtube", aid))

    return actions_taken

# === MAIN execution ===
if __name__ == "__main__":
    # 1) load base series for total throughput (30 days)
    total_df = load_agg_bytes(days=30)
    if total_df.empty:
        print("No throughput data available. Exiting.")
        raise SystemExit(0)
    total_df.rename(columns={'ds': 'ds', 'y': 'y'}, inplace=True)
    total_df['ds'] = pd.to_datetime(total_df['ds']).dt.tz_localize(None)

    # Fit forecast for total (used mainly for seasonality)
    total_forecast = prophet_forecast(total_df, periods=15, freq='min')

    # 2) compute recent 15-min window metrics for throughput
    recent = total_df[total_df['ds'] >= total_df['ds'].max() - timedelta(minutes=15)]
    if recent.empty:
        recent = total_df.tail(15)

    burst = burstiness_index(recent['y'])
    anomaly = anomaly_score_mad(recent['y'])
    slope = traffic_trend(recent['y'])
    trend_mbps = slope_to_mbps(slope)

    # seasonality sample from total_forecast
    seasonality_cols = [c for c in total_forecast.columns if c.startswith('seasonal')] if total_forecast is not None else []
    seasonality_data = total_forecast[['ds'] + seasonality_cols].tail(10).to_dict(orient='records') if seasonality_cols else []

    # 3) latency/jitter/loss forecasts (7 days window)
    latency_df = load_latency_series(days=7)
    jitter_df = load_jitter_series(days=7)
    loss_df = load_loss_series(days=7)

    # prepare and forecast each KPI
    latency_prob = 0.0
    jitter_prob = 0.0
    loss_prob = 0.0

    if not latency_df.empty:
        latency_df['ds'] = pd.to_datetime(latency_df['ds']).dt.tz_localize(None)
        lat_fore = prophet_forecast(latency_df, periods=15, freq='min')
        latency_prob = sla_violation_prob_from_forecast(lat_fore, SLA_LATENCY_MS, kpi_col='yhat')
    if not jitter_df.empty:
        jitter_df['ds'] = pd.to_datetime(jitter_df['ds']).dt.tz_localize(None)
        jit_fore = prophet_forecast(jitter_df, periods=15, freq='min')
        jitter_prob = sla_violation_prob_from_forecast(jit_fore, SLA_JITTER_MS, kpi_col='yhat')
    if not loss_df.empty:
        loss_df['ds'] = pd.to_datetime(loss_df['ds']).dt.tz_localize(None)
        loss_fore = prophet_forecast(loss_df, periods=15, freq='min')
        loss_prob = sla_violation_prob_from_forecast(loss_fore, SLA_LOSS_PCT, kpi_col='yhat')

    # 4) per-app throughput forecasts and SLA probs
    app_probs = {}
    for app in MONITORED_APPS:
        app_df = load_app_bytes(app, days=14)  # 2 weeks for app-level
        if app_df.empty:
            app_probs[app] = 0.0
            continue
        app_df['ds'] = pd.to_datetime(app_df['ds']).dt.tz_localize(None)
        # convert bytes -> Mbps per minute: bytes per minute -> bits /1e6
        # Prophet expects 'y' as numeric; convert to Mbps for easier SLA compare
        app_df = app_df.copy()
        app_df['y'] = (app_df['y'] * 8.0) / 1e6  # now y in Megabits per minute (~Mbps*60)
        # We want roughly instantaneous Mbps — dividing by 60 to approximate Mbps avg
        app_df['y'] = app_df['y'] / 60.0
        app_fore = prophet_forecast(app_df, periods=15, freq='min')
        prob = sla_violation_prob_from_forecast(app_fore, SLA_THROUGHPUT_MBPS, kpi_col='yhat')
        app_probs[app] = prob

    # 5) SLA violation probabilities now collected
    print("Summary:")
    print(f" Burstiness: {burst}, Anomaly(MAD): {anomaly}, Trend: {trend_mbps:.3f} Mbps/min")
    print(f" Latency SLA prob: {latency_prob}, Jitter SLA prob: {jitter_prob}, Loss SLA prob: {loss_prob}")
    print(f" App SLA probs: {app_probs}")

    # 6) save summary to DB
    window_start = recent['ds'].min()
    window_end = recent['ds'].max()
    save_summary(window_start, window_end, burst, anomaly, trend_mbps, seasonality_data, latency_prob, jitter_prob, loss_prob, app_probs)

    # 7) decision & actions
    actions = decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly)
    print("Actions taken:", actions)
