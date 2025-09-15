#!/usr/bin/env python3
"""
forecast.py - Fixed seasonality and SLA probability calculation
"""

import os
import json
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from prophet import Prophet

DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# SLA thresholds
SLA_THROUGHPUT_MBPS = 2.0
SLA_LATENCY_MS = 100.0
SLA_JITTER_MS = 50.0
SLA_LOSS_PCT = 5.0

# Decision thresholds
SLA_VIOLATION_PROB_TH = 0.30
ANOMALY_ACTION_TH = 3.0
ANOMALY_ACTION_TH_CR = 5.0

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

def ensure_tables_exist():
    run_sql("""
        CREATE TABLE IF NOT EXISTS traffic.summary_forecast_v2 (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            burstiness_index NUMERIC,
            anomaly_score NUMERIC,
            traffic_trend NUMERIC,
            seasonality_pattern JSONB,
            latency_sla_prob NUMERIC,
            jitter_sla_prob NUMERIC,
            loss_sla_prob NUMERIC,
            app_sla_prob JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

# ---------- data loaders ----------
def load_time_series_for_kpi(sql_query, params=None):
    conn = psycopg2.connect(DB_DSN)
    try:
        df = pd.read_sql(sql_query, conn, params=params)
    except Exception as e:
        print(f"Database query error: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def load_agg_bytes(days=1):
    q = f"""
    SELECT timestamp as ds, SUM(bytes_tx + bytes_rx) AS y
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{days} days'
    GROUP BY timestamp 
    ORDER BY timestamp;
    """
    return load_time_series_for_kpi(q)

def load_app_bytes(app, days=1):
    q = f"""
    SELECT timestamp as ds, SUM(bytes_tx + bytes_rx) AS y
    FROM traffic.flow_stats
    WHERE app = %s AND timestamp >= NOW() - interval '{days} days'
    GROUP BY timestamp 
    ORDER BY timestamp;
    """
    return load_time_series_for_kpi(q, params=(app,))

def load_latency_series(days=1):
    q = f"""
    SELECT timestamp as ds, AVG(latency_ms) AS y
    FROM traffic.flow_stats
    WHERE latency_ms IS NOT NULL 
    AND timestamp >= NOW() - interval '{days} days'
    GROUP BY timestamp 
    ORDER BY timestamp;
    """
    return load_time_series_for_kpi(q)

def load_jitter_series(days=1):
    q = f"""
    SELECT timestamp as ds, STDDEV(latency_ms) AS y
    FROM traffic.flow_stats
    WHERE latency_ms IS NOT NULL 
    AND timestamp >= NOW() - interval '{days} days'
    GROUP BY timestamp 
    ORDER BY timestamp;
    """
    return load_time_series_for_kpi(q)

def load_loss_series(days=1):
    q = f"""
    SELECT timestamp as ds,
           CASE WHEN SUM(pkts_tx) = 0 THEN 0 
                ELSE 100.0 * (SUM(pkts_tx) - SUM(pkts_rx)) / SUM(pkts_tx) 
           END AS y
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{days} days'
    GROUP BY timestamp 
    ORDER BY timestamp;
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
    if df is None or df.empty or len(df) < 3:
        return None
        
    df_local = df.copy()
    df_local['ds'] = pd.to_datetime(df_local['ds']).dt.tz_localize(None)
    df_local = df_local.dropna(subset=['y'])
    
    if df_local.empty or len(df_local) < 3:
        return None

    try:
        m = Prophet(daily_seasonality=True, yearly_seasonality=False, weekly_seasonality=True)
        m.fit(df_local)
        future = m.make_future_dataframe(periods=periods, freq=freq, include_history=False)
        forecast = m.predict(future)
        return forecast
    except Exception as e:
        print(f"Prophet forecast error: {e}")
        return None

def sla_violation_prob_from_forecast(forecast, threshold, direction='gt'):
    if forecast is None or forecast.empty or 'yhat' not in forecast.columns:
        return 0.0

    if direction == 'lt':
        viol = (forecast['yhat'] < threshold).sum()
    else:
        viol = (forecast['yhat'] > threshold).sum()

    prob = float(viol) / float(len(forecast))
    return round(prob, 4)

def extract_seasonality(forecast):
    """Extract seasonality components from forecast"""
    if forecast is None or forecast.empty:
        return []
    
    seasonality_data = []
    seasonality_cols = [col for col in forecast.columns if any(x in col for x in ['weekly', 'daily', 'season'])]
    
    if seasonality_cols:
        for _, row in forecast.tail(10).iterrows():
            season_point = {}
            for col in seasonality_cols:
                if col != 'ds':
                    season_point[col] = float(row[col])
            season_point['ds'] = row['ds'].isoformat()
            seasonality_data.append(season_point)
    
    return seasonality_data

# ---------- DB writes ----------
def save_summary(window_start, window_end, burst, anomaly, trend_mbps,
                 seasonality, latency_prob, jitter_prob, loss_prob, app_probs):
    ensure_tables_exist()
    
    run_sql("""
        INSERT INTO traffic.summary_forecast_v2
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, 
         seasonality_pattern, latency_sla_prob, jitter_sla_prob, loss_sla_prob, app_sla_prob)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (window_start, window_end, burst, anomaly, trend_mbps,
          json.dumps(seasonality), latency_prob, jitter_prob, loss_prob, json.dumps(app_probs)))

def insert_alert(level, msg, anomaly_score=None, dpid=None, src_ip=None, dst_ip=None, flow_id=None):
    run_sql("""
        INSERT INTO traffic.alerts(level,msg,anomaly_score,dpid,src_ip,dst_ip,flow_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (level, msg, anomaly_score, dpid, src_ip, dst_ip, flow_id))

# ---------- decision logic ----------
def decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly):
    actions_taken = []
    
    if latency_prob > SLA_VIOLATION_PROB_TH:
        msg = f"High latency probability: {latency_prob:.2f}"
        insert_alert("warning", msg, anomaly)
        actions_taken.append(("alert", "high_latency"))
    
    if jitter_prob > SLA_VIOLATION_PROB_TH:
        msg = f"High jitter probability: {jitter_prob:.2f}"
        insert_alert("warning", msg, anomaly)
        actions_taken.append(("alert", "high_jitter"))
    
    if loss_prob > SLA_VIOLATION_PROB_TH:
        msg = f"High loss probability: {loss_prob:.2f}"
        insert_alert("warning", msg, anomaly)
        actions_taken.append(("alert", "high_loss"))
    
    for app, prob in app_probs.items():
        if prob > SLA_VIOLATION_PROB_TH:
            msg = f"App {app} SLA risk: {prob:.2f}"
            insert_alert("warning", msg, anomaly)
            actions_taken.append(("alert", f"app_{app}_sla"))
    
    if anomaly > ANOMALY_ACTION_TH:
        msg = f"High anomaly detected: {anomaly:.2f}"
        insert_alert("critical", msg, anomaly)
        actions_taken.append(("alert", "high_anomaly"))
    
    return actions_taken

# ---------- MAIN ----------
if __name__ == "__main__":
    print("Starting network forecast...")
    ensure_tables_exist()
    
    # 1) Total throughput forecast
    total_df = load_agg_bytes(days=1)
    if total_df.empty:
        print("No throughput data available")
        total_forecast = None
    else:
        total_df['ds'] = pd.to_datetime(total_df['ds'])
        total_forecast = prophet_forecast(total_df)
    
    # Calculate metrics from recent data
    if not total_df.empty:
        recent = total_df.tail(min(15, len(total_df)))
        burst = burstiness_index(recent['y'])
        anomaly = anomaly_score_mad(recent['y'])
        slope = traffic_trend(recent['y'])
        trend_mbps = slope_to_mbps(slope)
        window_start = recent['ds'].min()
        window_end = recent['ds'].max()
    else:
        burst = anomaly = trend_mbps = 0.0
        window_start = window_end = datetime.now()
    
    # 2) Latency forecast
    latency_df = load_latency_series(days=1)
    latency_prob = 0.0
    if not latency_df.empty:
        latency_df['ds'] = pd.to_datetime(latency_df['ds'])
        latency_forecast = prophet_forecast(latency_df)
        if latency_forecast is not None:
            latency_prob = sla_violation_prob_from_forecast(latency_forecast, SLA_LATENCY_MS, 'gt')
    
    # 3) Jitter forecast
    jitter_df = load_jitter_series(days=1)
    jitter_prob = 0.0
    if not jitter_df.empty:
        jitter_df['ds'] = pd.to_datetime(jitter_df['ds'])
        jitter_forecast = prophet_forecast(jitter_df)
        if jitter_forecast is not None:
            jitter_prob = sla_violation_prob_from_forecast(jitter_forecast, SLA_JITTER_MS, 'gt')
    
    # 4) Loss forecast
    loss_df = load_loss_series(days=1)
    loss_prob = 0.0
    if not loss_df.empty:
        loss_df['ds'] = pd.to_datetime(loss_df['ds'])
        loss_forecast = prophet_forecast(loss_df)
        if loss_forecast is not None:
            loss_prob = sla_violation_prob_from_forecast(loss_forecast, SLA_LOSS_PCT, 'gt')
    
    # 5) App-specific forecasts
    app_probs = {}
    for app in MONITORED_APPS:
        app_df = load_app_bytes(app, days=1)
        if not app_df.empty:
            app_df['ds'] = pd.to_datetime(app_df['ds'])
            # Convert bytes to Mbps (bytes per 5 seconds -> Mbps)
            app_df['y'] = (app_df['y'] * 8) / (5 * 1e6)  # Convert to Mbps
            app_forecast = prophet_forecast(app_df)
            if app_forecast is not None:
                app_probs[app] = sla_violation_prob_from_forecast(app_forecast, SLA_THROUGHPUT_MBPS, 'lt')
            else:
                app_probs[app] = 0.0
        else:
            app_probs[app] = 0.0
    
    # 6) Extract seasonality
    seasonality_data = extract_seasonality(total_forecast) if total_forecast is not None else []
    
    # 7) Save results
    save_summary(window_start, window_end, burst, anomaly, trend_mbps,
                 seasonality_data, latency_prob, jitter_prob, loss_prob, app_probs)
    
    # 8) Make decisions
    actions = decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly)
    
    print("=== Forecast Summary ===")
    print(f"Burstiness: {burst}, Anomaly: {anomaly}, Trend: {trend_mbps:.3f} Mbps/min")
    print(f"Latency SLA prob (> {SLA_LATENCY_MS}ms): {latency_prob:.4f}")
    print(f"Jitter SLA prob (> {SLA_JITTER_MS}ms): {jitter_prob:.4f}")
    print(f"Loss SLA prob (> {SLA_LOSS_PCT}%): {loss_prob:.4f}")
    print(f"App SLA probs: {app_probs}")
    print(f"Seasonality points: {len(seasonality_data)}")
    print(f"Actions taken: {actions}")