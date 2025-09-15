#!/usr/bin/env python3
"""
forecast.py - Updated for 5-second aggregated data
- Adjusted for your network monitoring system
- Compatible with 5-second aggregated traffic data
- Simplified dependencies
"""

import os
import json
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from prophet import Prophet

DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# SLA thresholds adjusted for your test network
SLA_THROUGHPUT_MBPS = 2.0   # Lower threshold for test environment
SLA_LATENCY_MS = 100.0      # Higher latency threshold
SLA_JITTER_MS = 50.0        
SLA_LOSS_PCT = 5.0          

# Decision thresholds
SLA_VIOLATION_PROB_TH = 0.30
ANOMALY_ACTION_TH = 3.0
ANOMALY_ACTION_TH_CR = 5.0 

# Apps from your apps.yaml
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
    """Ensure all required tables exist"""
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
    
    run_sql("""
        CREATE TABLE IF NOT EXISTS traffic.alerts (
            id SERIAL PRIMARY KEY,
            level VARCHAR(20),
            msg TEXT,
            anomaly_score NUMERIC,
            dpid INTEGER,
            src_ip VARCHAR(20),
            dst_ip VARCHAR(20),
            flow_id VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    run_sql("""
        CREATE TABLE IF NOT EXISTS traffic.actions (
            id SERIAL PRIMARY KEY,
            action VARCHAR(50),
            params_json JSONB,
            outcome VARCHAR(20),
            reference_alert_id INTEGER,
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
    """Load aggregated bytes (now in 5-second intervals)"""
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, 
           SUM(bytes_tx + bytes_rx) AS y
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{days} days'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_app_bytes(app, days=1):
    """Load app-specific bytes"""
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, 
           SUM(bytes_tx + bytes_rx) AS y
    FROM traffic.flow_stats
    WHERE app = %s AND timestamp >= NOW() - interval '{days} days'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q, params=(app,))

def load_latency_series(days=1):
    """Load latency data"""
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, 
           AVG(latency_ms) AS y
    FROM traffic.flow_stats
    WHERE latency_ms IS NOT NULL 
    AND timestamp >= NOW() - interval '{days} days'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_jitter_series(days=1):
    """Load jitter data"""
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds, 
           STDDEV(latency_ms) AS y
    FROM traffic.flow_stats
    WHERE latency_ms IS NOT NULL 
    AND timestamp >= NOW() - interval '{days} days'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_loss_series(days=1):
    """Load packet loss data"""
    q = f"""
    SELECT date_trunc('minute', timestamp) AS ds,
           CASE WHEN SUM(pkts_tx) = 0 THEN 0 
                ELSE 100.0 * (SUM(pkts_tx) - SUM(pkts_rx)) / SUM(pkts_tx) 
           END AS y
    FROM traffic.flow_stats
    WHERE timestamp >= NOW() - interval '{days} days'
    GROUP BY ds 
    ORDER BY ds;
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
    """Simple Prophet forecast"""
    if df is None or df.empty or len(df) < 3:
        return None
        
    df_local = df.copy()
    df_local['ds'] = pd.to_datetime(df_local['ds']).dt.tz_localize(None)
    df_local = df_local.dropna(subset=['y'])
    
    if df_local.empty or len(df_local) < 3:
        return None

    try:
        m = Prophet(daily_seasonality=True, yearly_seasonality=False)
        m.fit(df_local)
        future = m.make_future_dataframe(periods=periods, freq=freq)
        forecast = m.predict(future)
        return forecast
    except Exception as e:
        print(f"Prophet forecast error: {e}")
        return None

def sla_violation_prob_from_forecast(forecast, threshold, kpi_col='yhat', direction='gt'):
    """Calculate SLA violation probability"""
    if forecast is None or forecast.empty:
        return 0.0
        
    if direction == 'lt':
        viol = (forecast[kpi_col] < threshold).sum()
    else:
        viol = (forecast[kpi_col] > threshold).sum()

    prob = float(viol) / float(len(forecast))
    return round(prob, 4)

# ---------- DB writes ----------
def save_summary(window_start, window_end, burst, anomaly, trend_mbps,
                 seasonality, latency_prob, jitter_prob, loss_prob, app_probs):
    """Save forecast summary to database"""
    ensure_tables_exist()
    
    seasonality_safe = []
    if seasonality:
        for row in seasonality:
            safe_row = {}
            for k, v in row.items():
                if isinstance(v, pd.Timestamp):
                    safe_row[k] = v.isoformat()
                else:
                    safe_row[k] = v
            seasonality_safe.append(safe_row)

    run_sql("""
        INSERT INTO traffic.summary_forecast_v2
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, 
         seasonality_pattern, latency_sla_prob, jitter_sla_prob, loss_sla_prob, app_sla_prob)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (window_start, window_end, burst, anomaly, trend_mbps,
          json.dumps(seasonality_safe), latency_prob, jitter_prob, loss_prob, json.dumps(app_probs)))

def insert_alert(level, msg, anomaly_score=None, dpid=None, src_ip=None, dst_ip=None, flow_id=None):
    """Insert alert into database"""
    run_sql("""
        INSERT INTO traffic.alerts(level,msg,anomaly_score,dpid,src_ip,dst_ip,flow_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (level, msg, anomaly_score, dpid, src_ip, dst_ip, flow_id))

def insert_action(action, params, outcome, ref_alert_id):
    """Insert action into database"""
    run_sql("""
        INSERT INTO traffic.actions(action,params_json,outcome,reference_alert_id)
        VALUES (%s,%s,%s,%s)
    """, (action, json.dumps(params), outcome, ref_alert_id))

# ---------- Simple action functions ----------
def translate_and_apply(prompt):
    """Simplified translation for your network"""
    if "prioritize" in prompt:
        return {"action": "set_priority", "value": "high"}
    elif "throttle" in prompt:
        return {"action": "set_rate_limit", "value": "2mbps"}
    return {"action": "monitor", "value": "none"}

def top_user_for_app(app, minutes=5):
    """Find top user for specific app"""
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        SELECT src_ip, SUM(bytes_tx + bytes_rx) as total_bytes
        FROM traffic.flow_stats 
        WHERE app = %s AND timestamp >= NOW() - interval '%s minutes'
        GROUP BY src_ip ORDER BY total_bytes DESC LIMIT 1
    """, (app, minutes))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return (row[0], row[1]) if row else (None, 0)

def top_user_overall(minutes=5):
    """Find top user across all apps"""
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        SELECT src_ip, app, SUM(bytes_tx + bytes_rx) as total_bytes
        FROM traffic.flow_stats 
        WHERE timestamp >= NOW() - interval '%s minutes'
        GROUP BY src_ip, app ORDER BY total_bytes DESC LIMIT 1
    """, (minutes,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return (row[0], row[1], row[2]) if row else (None, None, 0)

# ---------- decision logic ----------
def decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly):
    """Simplified decision logic"""
    actions_taken = []
    
    # Global SLA actions
    if latency_prob > SLA_VIOLATION_PROB_TH:
        msg = f"High latency probability: {latency_prob}"
        insert_alert("warning", msg, anomaly)
        actions_taken.append(("alert", "high_latency"))
    
    if any(prob > SLA_VIOLATION_PROB_TH for prob in app_probs.values()):
        for app, prob in app_probs.items():
            if prob > SLA_VIOLATION_PROB_TH:
                msg = f"App {app} SLA risk: {prob}"
                insert_alert("warning", msg, anomaly)
                actions_taken.append(("alert", f"app_{app}_sla"))
    
    # High anomaly actions
    if anomaly > ANOMALY_ACTION_TH:
        user, app, bytes = top_user_overall()
        if user:
            msg = f"High anomaly: {anomaly}, top user: {user} on {app}"
            insert_alert("critical", msg, anomaly, src_ip=user)
            actions_taken.append(("throttle", user, app))
    
    return actions_taken

# ---------- MAIN ----------
if __name__ == "__main__":
    print("Starting network forecast...")
    
    # Ensure database tables exist
    ensure_tables_exist()
    
    # Load data (shorter periods since we have 5-second data now)
    total_df = load_agg_bytes(days=1)  # Only need 1 day of data
    if total_df is None or total_df.empty:
        print("No data available. Exiting.")
        exit(0)
    
    total_df['ds'] = pd.to_datetime(total_df['ds']).dt.tz_localize(None)
    total_df = total_df.dropna(subset=['y'])
    
    if total_df.empty:
        print("No valid data rows. Exiting.")
        exit(0)
    
    # Generate forecast
    total_forecast = prophet_forecast(total_df)
    
    # Calculate metrics
    recent = total_df.tail(15)
    burst = burstiness_index(recent['y'])
    anomaly = anomaly_score_mad(recent['y'])
    slope = traffic_trend(recent['y'])
    trend_mbps = slope_to_mbps(slope)
    
    # Load other metrics
    latency_df = load_latency_series(days=1)
    jitter_df = load_jitter_series(days=1)
    loss_df = load_loss_series(days=1)
    
    latency_prob = jitter_prob = loss_prob = 0.0
    
    if not latency_df.empty:
        latency_forecast = prophet_forecast(latency_df)
        latency_prob = sla_violation_prob_from_forecast(latency_forecast, SLA_LATENCY_MS, direction='gt')
    
    if not jitter_df.empty:
        jitter_forecast = prophet_forecast(jitter_df)
        jitter_prob = sla_violation_prob_from_forecast(jitter_forecast, SLA_JITTER_MS, direction='gt')
    
    if not loss_df.empty:
        loss_forecast = prophet_forecast(loss_df)
        loss_prob = sla_violation_prob_from_forecast(loss_forecast, SLA_LOSS_PCT, direction='gt')
    
    # App-specific forecasts
    app_probs = {}
    for app in MONITORED_APPS:
        app_df = load_app_bytes(app, days=1)
        if not app_df.empty:
            app_forecast = prophet_forecast(app_df)
            app_probs[app] = sla_violation_prob_from_forecast(app_forecast, SLA_THROUGHPUT_MBPS, direction='lt')
        else:
            app_probs[app] = 0.0
    
    # Save results
    window_start = recent['ds'].min()
    window_end = recent['ds'].max()
    save_summary(window_start, window_end, burst, anomaly, trend_mbps, 
                [], latency_prob, jitter_prob, loss_prob, app_probs)
    
    # Make decisions
    actions = decide_and_act(latency_prob, jitter_prob, loss_prob, app_probs, burst, anomaly)
    
    print("Forecast completed. Actions:", actions)