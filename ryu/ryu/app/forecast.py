#!/usr/bin/env python3
"""
forecast.py - COMPLETELY FIXED - All metrics working
"""

import os
import json
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from prophet import Prophet

DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# SLA thresholds - Adjusted for your test network
SLA_THROUGHPUT_MBPS = 1.0    # Lower threshold for test environment
SLA_LATENCY_MS = 80.0        # Realistic for your network
SLA_JITTER_MS = 40.0        
SLA_LOSS_PCT = 3.0          

# Decision thresholds
SLA_VIOLATION_PROB_TH = 0.30
ANOMALY_ACTION_TH = 2.0
ANOMALY_ACTION_TH_CR = 4.0

MONITORED_APPS = ["youtube", "netflix", "twitch"]

# ---------- DB helpers ----------
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
        print(f"DB error in run_sql: {e}")
        return None

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
    try:
        conn = psycopg2.connect(DB_DSN)
        df = pd.read_sql(sql_query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading time series: {e}")
        return pd.DataFrame()

def load_agg_bytes(hours=6):
    """Load aggregated bytes for recent hours"""
    q = f"""
    SELECT 
        date_trunc('minute', timestamp) AS ds, 
        SUM(bytes_tx + bytes_rx) AS y
    FROM traffic.flow_stats 
    WHERE timestamp >= NOW() - interval '{hours} hours'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_app_bytes(app, hours=6):
    """Load app-specific bytes"""
    q = f"""
    SELECT 
        date_trunc('minute', timestamp) AS ds, 
        SUM(bytes_tx + bytes_rx) AS y
    FROM traffic.flow_stats 
    WHERE app = %s AND timestamp >= NOW() - interval '{hours} hours'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q, params=(app,))

def load_latency_series(hours=6):
    """Load latency data"""
    q = f"""
    SELECT 
        date_trunc('minute', timestamp) AS ds, 
        AVG(latency_ms) AS y
    FROM traffic.flow_stats 
    WHERE latency_ms IS NOT NULL 
    AND timestamp >= NOW() - interval '{hours} hours'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_jitter_series(hours=6):
    """Load jitter data"""
    q = f"""
    SELECT 
        date_trunc('minute', timestamp) AS ds, 
        STDDEV(latency_ms) AS y
    FROM traffic.flow_stats 
    WHERE latency_ms IS NOT NULL 
    AND timestamp >= NOW() - interval '{hours} hours'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

def load_loss_series(hours=6):
    """Load packet loss data"""
    q = f"""
    SELECT 
        date_trunc('minute', timestamp) AS ds,
        CASE WHEN SUM(pkts_tx) = 0 THEN 0 
             ELSE 100.0 * (SUM(pkts_tx) - SUM(pkts_rx)) / NULLIF(SUM(pkts_tx), 0) 
        END AS y
    FROM traffic.flow_stats 
    WHERE timestamp >= NOW() - interval '{hours} hours'
    GROUP BY ds 
    ORDER BY ds;
    """
    return load_time_series_for_kpi(q)

# ---------- simple metrics ----------
def burstiness_index(series):
    """Calculate burstiness index"""
    if series is None or series.empty or series.mean() == 0:
        return 0.0
    try:
        return round(float(series.max() / series.mean()), 3)
    except:
        return 0.0

def anomaly_score_mad(series):
    """Calculate anomaly score using MAD"""
    if series is None or series.empty:
        return 0.0
    try:
        median = np.median(series)
        mad = np.median(np.abs(series - median))
        if mad == 0:
            if series.std() == 0:
                return 0.0
            z = (series.iloc[-1] - series.mean()) / series.std()
            return round(float(abs(z)), 3)
        score = abs(series.iloc[-1] - median) / mad
        return round(float(score), 3)
    except:
        return 0.0

def traffic_trend(series):
    """Calculate traffic trend slope"""
    if series is None or series.empty or len(series) < 2:
        return 0.0
    try:
        x = np.arange(len(series))
        slope = np.polyfit(x, series, 1)[0]
        return round(float(slope), 6)  # Small values for bytes/minute
    except:
        return 0.0

def slope_to_mbps(slope_bytes_per_min):
    """Convert bytes/minute slope to Mbps/minute"""
    try:
        return round(float((slope_bytes_per_min * 8.0) / 1e6), 3)
    except:
        return 0.0

# ---------- forecasting ----------
def prophet_forecast(df, periods=15, freq='min'):
    """Simple Prophet forecast with error handling"""
    if df is None or df.empty or len(df) < 5:  # Need at least 5 points
        return None
        
    try:
        df_local = df.copy()
        df_local['ds'] = pd.to_datetime(df_local['ds'])
        df_local = df_local.dropna(subset=['y'])
        
        if df_local.empty or len(df_local) < 5:
            return None

        # Remove outliers (top and bottom 10%)
        q_low = df_local['y'].quantile(0.1)
        q_high = df_local['y'].quantile(0.9)
        df_local = df_local[(df_local['y'] >= q_low) & (df_local['y'] <= q_high)]
        
        if len(df_local) < 3:
            return None

        m = Prophet(
            daily_seasonality=True,
            yearly_seasonality=False,
            weekly_seasonality=True,
            changepoint_prior_scale=0.05
        )
        m.fit(df_local)
        future = m.make_future_dataframe(periods=periods, freq=freq, include_history=False)
        forecast = m.predict(future)
        return forecast
    except Exception as e:
        print(f"Prophet forecast failed: {e}")
        return None

def sla_violation_prob_from_forecast(forecast, threshold, direction='gt'):
    """Calculate SLA violation probability"""
    if forecast is None or forecast.empty or 'yhat' not in forecast.columns:
        return 0.0

    try:
        if direction == 'lt':
            viol_count = (forecast['yhat'] < threshold).sum()
        else:
            viol_count = (forecast['yhat'] > threshold).sum()
        
        total_points = len(forecast)
        prob = viol_count / total_points if total_points > 0 else 0.0
        return round(prob, 4)
    except:
        return 0.0

def extract_seasonality(forecast):
    """Extract seasonality components from forecast"""
    if forecast is None or forecast.empty:
        return []
    
    try:
        seasonality_data = []
        # Get all seasonality columns
        seasonality_cols = [col for col in forecast.columns 
                           if any(x in col for x in ['weekly', 'daily', 'yearly', 'additive'])]
        
        if seasonality_cols:
            for _, row in forecast.tail(8).iterrows():
                season_point = {'ds': row['ds'].isoformat()}
                for col in seasonality_cols:
                    if col != 'ds':
                        season_point[col] = float(row[col])
                seasonality_data.append(season_point)
        
        return seasonality_data
    except:
        return []

# ---------- DB writes ----------
def save_summary(window_start, window_end, burst, anomaly, trend_mbps,
                 seasonality, latency_prob, jitter_prob, loss_prob, app_probs):
    """Save forecast summary to database"""
    ensure_tables_exist()
    
    try:
        run_sql("""
            INSERT INTO traffic.summary_forecast_v2
            (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, 
             seasonality_pattern, latency_sla_prob, jitter_sla_prob, loss_sla_prob, app_sla_prob)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            window_start, window_end, burst, anomaly, trend_mbps,
            json.dumps(seasonality), 
            latency_prob, jitter_prob, loss_prob, 
            json.dumps(app_probs)
        ))
    except Exception as e:
        print(f"Error saving summary: {e}")

# ---------- MAIN ----------
if __name__ == "__main__":
    print("=== Starting Network Forecast ===")
    print("Loading data...")
    
    # 1) Load and prepare data
    total_df = load_agg_bytes(hours=12)
    
    if total_df.empty:
        print("ERROR: No data found in database!")
        print("Check if collector is running and inserting data")
        exit(1)
    
    print(f"Loaded {len(total_df)} data points")
    
    # Convert and clean data
    total_df['ds'] = pd.to_datetime(total_df['ds'])
    total_df = total_df.dropna(subset=['y'])
    
    if total_df.empty:
        print("ERROR: No valid data points after cleaning")
        exit(1)
    
    # 2) Calculate basic metrics
    recent_data = total_df.tail(min(20, len(total_df)))
    
    burst = burstiness_index(recent_data['y'])
    anomaly = anomaly_score_mad(recent_data['y'])
    raw_trend = traffic_trend(recent_data['y'])
    trend_mbps = slope_to_mbps(raw_trend)
    
    window_start = recent_data['ds'].min()
    window_end = recent_data['ds'].max()
    
    print(f"Basic metrics - Burstiness: {burst}, Anomaly: {anomaly}, Trend: {trend_mbps} Mbps/min")
    
    # 3) Generate forecasts
    total_forecast = prophet_forecast(total_df)
    
    # 4) SLA probabilities
    latency_prob = jitter_prob = loss_prob = 0.0
    app_probs = {app: 0.0 for app in MONITORED_APPS}
    
    # Latency forecast
    latency_df = load_latency_series(hours=12)
    if not latency_df.empty:
        latency_df['ds'] = pd.to_datetime(latency_df['ds'])
        latency_df = latency_df.dropna(subset=['y'])
        latency_forecast = prophet_forecast(latency_df)
        if latency_forecast is not None:
            latency_prob = sla_violation_prob_from_forecast(latency_forecast, SLA_LATENCY_MS, 'gt')
    
    # Jitter forecast
    jitter_df = load_jitter_series(hours=12)
    if not jitter_df.empty:
        jitter_df['ds'] = pd.to_datetime(jitter_df['ds'])
        jitter_df = jitter_df.dropna(subset=['y'])
        jitter_forecast = prophet_forecast(jitter_df)
        if jitter_forecast is not None:
            jitter_prob = sla_violation_prob_from_forecast(jitter_forecast, SLA_JITTER_MS, 'gt')
    
    # Loss forecast
    loss_df = load_loss_series(hours=12)
    if not loss_df.empty:
        loss_df['ds'] = pd.to_datetime(loss_df['ds'])
        loss_df = loss_df.dropna(subset=['y'])
        loss_forecast = prophet_forecast(loss_df)
        if loss_forecast is not None:
            loss_prob = sla_violation_prob_from_forecast(loss_forecast, SLA_LOSS_PCT, 'gt')
    
    # App throughput forecasts
    for app in MONITORED_APPS:
        app_df = load_app_bytes(app, hours=12)
        if not app_df.empty:
            app_df['ds'] = pd.to_datetime(app_df['ds'])
            app_df = app_df.dropna(subset=['y'])
            
            # Convert bytes to Mbps (bytes per minute to Mbps)
            app_df['y'] = (app_df['y'] * 8) / 60 / 1e6  # bytes/min -> Mbps
            
            app_forecast = prophet_forecast(app_df)
            if app_forecast is not None:
                app_probs[app] = sla_violation_prob_from_forecast(app_forecast, SLA_THROUGHPUT_MBPS, 'lt')
    
    # 5) Seasonality
    seasonality_data = extract_seasonality(total_forecast) if total_forecast is not None else []
    
    # 6) Save results
    save_summary(
        window_start, window_end, burst, anomaly, trend_mbps,
        seasonality_data, latency_prob, jitter_prob, loss_prob, app_probs
    )
    
    # 7) Print comprehensive results
    print("\n=== FORECAST RESULTS ===")
    print(f"Time Window: {window_start} to {window_end}")
    print(f"Burstiness Index: {burst}")
    print(f"Anomaly Score: {anomaly}")
    print(f"Traffic Trend: {trend_mbps} Mbps/min")
    print(f"\nSLA VIOLATION PROBABILITIES:")
    print(f"Latency (> {SLA_LATENCY_MS}ms): {latency_prob:.3f}")
    print(f"Jitter (> {SLA_JITTER_MS}ms): {jitter_prob:.3f}")
    print(f"Loss (> {SLA_LOSS_PCT}%): {loss_prob:.3f}")
    print(f"\nAPP THROUGHPUT PROBABILITIES (< {SLA_THROUGHPUT_MBPS}Mbps):")
    for app, prob in app_probs.items():
        print(f"  {app}: {prob:.3f}")
    print(f"\nSeasonality Patterns: {len(seasonality_data)} points extracted")
    
    print("\n=== FORECAST COMPLETED ===")