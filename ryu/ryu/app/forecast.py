#!/usr/bin/env python3
"""
forecast_full.py (patched)
- Fixes SLA violation direction logic (throughput = min required -> lt; latency/jitter/loss = max allowed -> gt)
- Robust seasonality extraction
- Better logging and safe handling when Prophet can't forecast
- Keeps same DB schema interactions as before

Requirements:
- prophet, pandas, numpy, psycopg2, translator_auto.py in same folder (with apps.yaml)
- traffic.flow_stats contains columns used by loaders (if not present, related KPI will be skipped)
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
SLA_THROUGHPUT_MBPS = 5.0   # Mbps minimal throughput per-app (violation if predicted < this)
SLA_LATENCY_MS = 50.0       # max latency in ms (violation if predicted > this)
SLA_JITTER_MS = 30.0        # max jitter in ms (violation if predicted > this)
SLA_LOSS_PCT = 1.0          # max packet loss percent (violation if predicted > this)

# Decision thresholds for actions
SLA_VIOLATION_PROB_TH = 0.30  # if prob > 30% -> trigger action
ANOMALY_ACTION_TH = 5.0       # anomaly score threshold (MAD)

# apps to check per-app throughput (must match your apps.yaml)
MONITORED_APPS = ["youtube", "netflix", "twitch"]

# ---------- DB helpers ----------
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

# ---------- data loaders ----------
def load_time_series_for_kpi(sql_query, params=None):
    conn = psycopg2.connect(DB_DSN)
    try:
        df = pd.read_sql(sql_query, conn, params=params)
    finally:
        conn.close()
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
    return load_time_series_for_kpi(q, params=(app,))

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
    return load_time_series_for_kpi(q)

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
    return load_time_series_for_kpi(q)

# ---------- simple metrics ----------
def burstiness_index(series):
    if series is None or series.empty or series.mean() == 0:
        return 0.0
    return round(series.max() / series.mean(), 2)

def anomaly_score_mad(series):
    if series is None or series.empty:
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
    if series is None or series.empty or len(series) < 2:
        return 0.0
    slope = np.polyfit(range(len(series)), series, 1)[0]
    return float(slope)

def slope_to_mbps(slope_bytes_per_min):
    return (slope_bytes_per_min * 8.0) / 1e6

# ---------- forecasting ----------
def prophet_forecast(df, periods=15, freq='min'):
    """
    Input df must have columns ['ds','y'], ds timezone-naive
    Returns forecast dataframe (or None on failure)
    """
    if df is None or df.empty or len(df) < 3:
        return None
    df_local = df.copy()
    df_local['ds'] = pd.to_datetime(df_local['ds']).dt.tz_localize(None)
    # ensure numeric y and drop NaN rows
    df_local = df_local.dropna(subset=['y'])
    if df_local.empty or len(df_local) < 3:
        return None

    try:
        m = Prophet(daily_seasonality=True, yearly_seasonality=False)
        m.add_seasonality(name='hour_cycle', period=60, fourier_order=4)
        m.add_seasonality(name='quarter_cycle', period=15, fourier_order=3)
        # fit
        m.fit(df_local)
        future = m.make_future_dataframe(periods=periods, freq=freq)
        forecast = m.predict(future)
        return forecast
    except Exception as e:
        print(f"[WARN] Prophet failed to fit/predict: {e}")
        return None

def sla_violation_prob_from_forecast(forecast, threshold, kpi_col='yhat', direction='auto'):
    """
    direction:
      - 'lt' : violation if forecast value < threshold  (e.g. throughput below min)
      - 'gt' : violation if forecast value > threshold  (e.g. latency > max)
      - 'auto': attempt to choose by reading threshold meaning:
          * if threshold > 1 and threshold <= 100 (likely percent) assume 'gt' for loss%
          * else for latency/jitter (threshold in ms) assume 'gt'
          * for throughput (Mbps), typically we want 'lt' (if threshold small)
    NOTE: this simple auto heuristic is fallback; callers should pass explicit direction.
    """
    if forecast is None or forecast.empty:
        return 0.0
    if kpi_col not in forecast.columns:
        kpi_col = 'yhat'

    # choose direction if auto
    if direction == 'auto':
        # if threshold is small (throughput often small number like 5.0), assume 'lt' for throughput,
        # but for latency/jitter/loss often we want 'gt'. We'll use heuristics:
        if threshold <= 100 and threshold > 10:
            # likely latency (ms) or large Mbps -> treat as 'gt' (violation if > threshold)
            direction = 'gt'
        elif threshold <= 10:
            # small number (e.g. Mbps threshold 5) -> treat as 'lt'
            direction = 'lt'
        else:
            direction = 'gt'

    if direction == 'lt':
        viol = (forecast[kpi_col] < threshold).sum()
    else:
        viol = (forecast[kpi_col] > threshold).sum()

    prob = float(viol) / float(len(forecast))
    return round(prob, 4)

# ---------- DB writes ----------
def save_summary(window_start, window_end, burst, anomaly, trend_mbps,
                 seasonality, latency_prob, jitter_prob, loss_prob, app_probs):
    ensure_summary_columns()

    # --- FIX: convert Timestamps to str for JSON ---
    seasonality_safe = None
    if seasonality:
        seasonality_safe = []
        for row in seasonality:
            safe_row = {}
            for k, v in row.items():
                if isinstance(v, pd.Timestamp):
                    safe_row[k] = v.isoformat()
                else:
                    safe_row[k] = v
            seasonality_safe.append(safe_row)
    else:
        seasonality_safe = []

    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.summary_forecast_v2
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, seasonality_pattern, latency_sla_prob, jitter_sla_prob, loss_sla_prob, app_sla_prob)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        window_start, window_end, burst, anomaly, trend_mbps,
        json.dumps(seasonality_safe),  # <- pakai safe
        latency_prob, jitter_prob, loss_prob, json.dumps(app_probs)
    ))
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

# ---------- decision logic ----------
def decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly):
    actions_taken = []
    if latency_prob > SLA_VIOLATION_PROB_TH:
        msg = f"Latency SLA risk: prob={latency_prob}"
        aid = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
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
        out = translate_and_apply("throttle netflix 5mbps")
        insert_action("throttle", out, "ok", aid)
        actions_taken.append(("throttle", "netflix", aid))

    for app, prob in (app_probs or {}).items():
        if prob >= SLA_VIOLATION_PROB_TH:
            msg = f"App {app} SLA risk: prob={prob}"
            aid = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
            out = translate_and_apply(f"throttle {app} 2mbps")
            insert_action("throttle", out, "ok", aid)
            actions_taken.append(("throttle", app, aid))

    if anomaly >= ANOMALY_ACTION_TH:
        msg = f"High anomaly detected (score={anomaly})"
        aid = insert_alert("critical", msg, anomaly_score=anomaly, dpid=1)
        out = translate_and_apply("throttle youtube 1mbps")
        insert_action("throttle", out, "ok", aid)
        actions_taken.append(("throttle", "youtube", aid))

    return actions_taken

# ---------- MAIN ----------
if __name__ == "__main__":
    # 1) total throughput series
    total_df = load_agg_bytes(days=30)
    if total_df is None or total_df.empty:
        print("[ERR] No throughput data available. Exiting.")
        raise SystemExit(0)
    total_df['ds'] = pd.to_datetime(total_df['ds']).dt.tz_localize(None)
    total_df = total_df.dropna(subset=['y'])
    if total_df.empty:
        print("[ERR] Throughput series has no numeric rows. Exiting.")
        raise SystemExit(0)

    total_forecast = prophet_forecast(total_df, periods=15, freq='min')
    if total_forecast is None:
        print("[WARN] total_forecast is None (not enough data or Prophet error)")

    # 2) recent 15-min metrics
    recent = total_df[total_df['ds'] >= total_df['ds'].max() - timedelta(minutes=15)]
    if recent.empty:
        recent = total_df.tail(15)

    burst = burstiness_index(recent['y'])
    anomaly = anomaly_score_mad(recent['y'])
    slope = traffic_trend(recent['y'])
    trend_mbps = slope_to_mbps(slope)

    # seasonality extraction: include columns that look like seasonality/cycle components
    # seasonality extraction: include columns that look like seasonality/cycle components
    seasonality_data = []
    if total_forecast is not None:
        seasonality_cols = [c for c in total_forecast.columns if ('season' in c.lower()) or ('cycle' in c.lower()) or c.lower() in ('weekly','monthly','daily')]
        if seasonality_cols:
            # ensure ds included
            cols = ['ds'] + [c for c in seasonality_cols if c in total_forecast.columns]
            seasonality_df = total_forecast[cols].copy()
            
            # konversi ke Mbps (kecuali kolom ds)
            for c in seasonality_df.columns:
                if c != 'ds':
                    seasonality_df[c] = seasonality_df[c] / 1e6
            
            seasonality_data = seasonality_df.tail(10).to_dict(orient='records')
        else:
            # fallback to the combined 'seasonal' column if present
            if 'seasonal' in total_forecast.columns:
                seasonality_df = total_forecast[['ds','seasonal']].copy()
                seasonality_df['seasonal'] = seasonality_df['seasonal'] / 1e6
                seasonality_data = seasonality_df.tail(10).to_dict(orient='records')

    # 3) latency/jitter/loss forecasts
    latency_df = load_latency_series(days=7)
    jitter_df = load_jitter_series(days=7)
    loss_df = load_loss_series(days=7)

    latency_prob = 0.0
    jitter_prob = 0.0
    loss_prob = 0.0

    if latency_df is None or latency_df.empty:
        print("[INFO] No latency data found for last 7 days; skipping latency forecast.")
    else:
        latency_df['ds'] = pd.to_datetime(latency_df['ds']).dt.tz_localize(None)
        latency_df = latency_df.dropna(subset=['y'])
        if latency_df.empty:
            print("[INFO] Latency series contains no numeric rows; skipping.")
        else:
            lat_fore = prophet_forecast(latency_df, periods=15, freq='min')
            latency_prob = sla_violation_prob_from_forecast(lat_fore, SLA_LATENCY_MS, kpi_col='yhat', direction='gt')
            print(f"[DBG] latency forecast computed, SLA prob (>{SLA_LATENCY_MS}ms) = {latency_prob}")

    if jitter_df is None or jitter_df.empty:
        print("[INFO] No jitter data found for last 7 days; skipping jitter forecast.")
    else:
        jitter_df['ds'] = pd.to_datetime(jitter_df['ds']).dt.tz_localize(None)
        jitter_df = jitter_df.dropna(subset=['y'])
        if jitter_df.empty:
            print("[INFO] Jitter series contains no numeric rows; skipping.")
        else:
            jit_fore = prophet_forecast(jitter_df, periods=15, freq='min')
            jitter_prob = sla_violation_prob_from_forecast(jit_fore, SLA_JITTER_MS, kpi_col='yhat', direction='gt')
            print(f"[DBG] jitter forecast computed, SLA prob (>{SLA_JITTER_MS}ms) = {jitter_prob}")

    if loss_df is None or loss_df.empty:
        print("[INFO] No packet-loss data found for last 7 days; skipping loss forecast.")
    else:
        loss_df['ds'] = pd.to_datetime(loss_df['ds']).dt.tz_localize(None)
        loss_df = loss_df.dropna(subset=['y'])
        if loss_df.empty:
            print("[INFO] Loss series contains no numeric rows; skipping.")
        else:
            loss_fore = prophet_forecast(loss_df, periods=15, freq='min')
            loss_prob = sla_violation_prob_from_forecast(loss_fore, SLA_LOSS_PCT, kpi_col='yhat', direction='gt')
            print(f"[DBG] loss forecast computed, SLA prob (>{SLA_LOSS_PCT}%) = {loss_prob}")

    # 4) per-app throughput forecasts and SLA probs
    app_probs = {}
    for app in MONITORED_APPS:
        app_df = load_app_bytes(app, days=14)  # 2 weeks for app-level
        if app_df is None or app_df.empty:
            app_probs[app] = 0.0
            print(f"[INFO] no data for app {app}; prob=0.0")
            continue
        app_df['ds'] = pd.to_datetime(app_df['ds']).dt.tz_localize(None)
        app_df = app_df.dropna(subset=['y'])
        if app_df.empty:
            app_probs[app] = 0.0
            print(f"[INFO] app {app} has no numeric rows; prob=0.0")
            continue

        # convert bytes -> Mbps (approx avg) for comparison with SLA (Mbps)
        # bytes per minute -> bits per minute /1e6 = Megabits per minute; divide by 60 ~= Mbps average
        app_df = app_df.copy()
        app_df['y'] = (app_df['y'] * 8.0) / 1e6
        app_df['y'] = app_df['y'] / 60.0

        app_fore = prophet_forecast(app_df, periods=15, freq='min')
        prob = sla_violation_prob_from_forecast(app_fore, SLA_THROUGHPUT_MBPS, kpi_col='yhat', direction='lt')
        app_probs[app] = prob
        print(f"[DBG] app {app} SLA prob (<{SLA_THROUGHPUT_MBPS} Mbps) = {prob}")

    # 5) Summary print
    print("=== Forecast Summary ===")
    print(f" Burstiness: {burst}, Anomaly(MAD): {anomaly}, Trend: {trend_mbps:.3f} Mbps/min")
    print(f" Latency SLA prob (> {SLA_LATENCY_MS} ms): {latency_prob}")
    print(f" Jitter SLA prob (> {SLA_JITTER_MS} ms): {jitter_prob}")
    print(f" Loss SLA prob (> {SLA_LOSS_PCT} %): {loss_prob}")
    print(f" App SLA probs: {app_probs}")
    print(f" Seasonality points saved: {len(seasonality_data)}")

    # 6) Save summary
    window_start = recent['ds'].min()
    window_end = recent['ds'].max()
    # save_summary uses variable 'seasonality' in its signature; keep compatibility by passing seasonality_data as that param
    seasonality = seasonality_data
    save_summary(window_start, window_end, burst, anomaly, trend_mbps, seasonality, latency_prob, jitter_prob, loss_prob, app_probs)

    # 7) decision & actions
    actions = decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly)
    print("Actions taken:", actions)
