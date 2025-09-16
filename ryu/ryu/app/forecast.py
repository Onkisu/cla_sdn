#!/usr/bin/env python3
"""
forecast.py - FINAL FIXED VERSION
"""

import os
import json
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta

DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# SLA thresholds - Realistic for your test network
SLA_THROUGHPUT_MBPS = 0.3    # Very low threshold for iperf test traffic
SLA_LATENCY_MS = 50.0        
SLA_JITTER_MS = 20.0        
SLA_LOSS_PCT = 1.5          

CATEGORIES = ["video", "gaming", "voip", "data"]

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
        print(f"DB error: {e}")
        return None

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

def load_recent_traffic(minutes=10):
    """Load recent traffic data"""
    q = f"""
    SELECT 
        timestamp as ds, 
        SUM(bytes_tx + bytes_rx) AS bytes,
        AVG(latency_ms) AS latency,
        STDDEV(latency_ms) AS jitter,
        CASE WHEN SUM(pkts_tx) = 0 THEN 0 
             ELSE 100.0 * (SUM(pkts_tx) - SUM(pkts_rx)) / SUM(pkts_tx) 
        END AS loss
    FROM traffic.flow_stats 
    WHERE timestamp >= NOW() - interval '{minutes} minutes'
    GROUP BY timestamp 
    ORDER BY timestamp;
    """
    return load_time_series_for_kpi(q)

def load_recent_category_traffic(category, minutes=10):
    """Load recent category traffic"""
    q = f"""
    SELECT 
        timestamp as ds, 
        SUM(bytes_tx + bytes_rx) AS bytes
    FROM traffic.flow_stats 
    WHERE category = %s AND timestamp >= NOW() - interval '{minutes} minutes'
    GROUP BY timestamp 
    ORDER BY timestamp;
    """
    return load_time_series_for_kpi(q, params=(category,))

# ---------- simple metrics ----------
def calculate_burstiness(series):
    """Calculate realistic burstiness index"""
    if series is None or series.empty or len(series) < 3:
        return 1.0
    
    # Use last 8 values for burstiness
    values = series.tail(8).values
    if np.mean(values) == 0:
        return 1.0
    
    burstiness = np.max(values) / np.mean(values)
    return round(float(max(1.0, min(3.0, burstiness))), 2)  # Limit to reasonable range

def calculate_anomaly(series):
    """Calculate meaningful anomaly score"""
    if series is None or series.empty or len(series) < 5:
        return 0.0
    
    values = series.tail(10).values
    if len(values) < 3:
        return 0.0
    
    # Calculate z-score of last value
    mean_val = np.mean(values[:-1])  # Mean of all but last value
    std_val = np.std(values[:-1]) if len(values) > 2 else 1.0
    
    if std_val == 0:
        return 0.0
    
    z_score = abs(values[-1] - mean_val) / std_val
    return round(float(min(5.0, z_score)), 2)  # Cap at 5.0

def calculate_trend(series):
    """Calculate traffic trend in Mbps per minute"""
    if series is None or series.empty or len(series) < 3:
        return 0.0
    
    values = series.tail(6).values  # Last 6 data points
    x = np.arange(len(values))
    
    try:
        slope = np.polyfit(x, values, 1)[0]
        # Convert bytes to Mbps per minute (assuming 5-second intervals)
        slope_mbps = (slope * 8) / 1e6 * 12  # Convert to Mbps per minute
        return round(float(slope_mbps), 3)
    except:
        return 0.0

def calculate_sla_probability(current_value, threshold, direction='gt'):
    """Calculate realistic SLA violation probability"""
    if current_value is None:
        return 0.0
    
    try:
        if direction == 'lt':
            # Throughput: probability increases as value decreases below threshold
            if current_value <= threshold:
                return round(min(0.9, (threshold - current_value) / threshold + 0.1), 3)
            else:
                return round(max(0.0, 0.2 * (1 - current_value/threshold)), 3)
        else:
            # Latency/Jitter/Loss: probability increases as value increases above threshold
            if current_value >= threshold:
                return round(min(0.9, (current_value - threshold) / threshold + 0.1), 3)
            else:
                return round(max(0.0, 0.2 * (current_value/threshold)), 3)
    except:
        return 0.0

# ---------- MAIN ----------
if __name__ == "__main__":
    print("=== Starting Network Forecast ===")
    print(f"Current time: {datetime.now()}")
    
    # 1) Load recent traffic data
    traffic_df = load_recent_traffic(minutes=15)
    
    if traffic_df.empty:
        print("No data available")
        # Create default values
        current_time = datetime.now()
        result = {
            'window_start': current_time - timedelta(minutes=5),
            'window_end': current_time,
            'burstiness': 1.0,
            'anomaly': 0.0,
            'trend': 0.0,
            'latency_prob': 0.0,
            'jitter_prob': 0.0,
            'loss_prob': 0.0,
            'category_probs': {cat: 0.1 for cat in CATEGORIES}
        }
    else:
        # Convert timestamps
        traffic_df['ds'] = pd.to_datetime(traffic_df['ds'])
        
        # Calculate metrics
        bytes_series = traffic_df['bytes'].dropna()
        latency_series = traffic_df['latency'].dropna()
        jitter_series = traffic_df['jitter'].dropna()
        loss_series = traffic_df['loss'].dropna()
        
        # Get current values
        current_bytes = bytes_series.iloc[-1] if not bytes_series.empty else 0
        current_latency = latency_series.iloc[-1] if not latency_series.empty else 0
        current_jitter = jitter_series.iloc[-1] if not jitter_series.empty else 0
        current_loss = loss_series.iloc[-1] if not loss_series.empty else 0
        
        # Calculate metrics
        burstiness = calculate_burstiness(bytes_series)
        anomaly = calculate_anomaly(bytes_series)
        trend = calculate_trend(bytes_series)
        
        # Calculate SLA probabilities
        latency_prob = calculate_sla_probability(current_latency, SLA_LATENCY_MS, 'gt')
        jitter_prob = calculate_sla_probability(current_jitter, SLA_JITTER_MS, 'gt')
        loss_prob = calculate_sla_probability(current_loss, SLA_LOSS_PCT, 'gt')
        
        # Calculate category probabilities
        category_probs = {}
        for category in CATEGORIES:
            cat_df = load_recent_category_traffic(category, minutes=15)
            if not cat_df.empty:
                cat_df['ds'] = pd.to_datetime(cat_df['ds'])
                bytes_series = cat_df['bytes'].dropna()
                if not bytes_series.empty:
                    current_cat_bytes = bytes_series.iloc[-1]
                    current_cat_mbps = (current_cat_bytes * 8) / 1e6  # Convert to Mbps
                    cat_prob = calculate_sla_probability(current_cat_mbps, SLA_THROUGHPUT_MBPS, 'lt')
                    category_probs[category] = cat_prob
                else:
                    category_probs[category] = 0.1
            else:
                category_probs[category] = 0.1
        
        result = {
            'window_start': traffic_df['ds'].min(),
            'window_end': traffic_df['ds'].max(),
            'burstiness': burstiness,
            'anomaly': anomaly,
            'trend': trend,
            'latency_prob': latency_prob,
            'jitter_prob': jitter_prob,
            'loss_prob': loss_prob,
            'category_probs': category_probs
        }
    
    # 2) Save to database
    try:
        run_sql("""
            INSERT INTO traffic.summary_forecast_v2
            (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, 
             seasonality_pattern, latency_sla_prob, jitter_sla_prob, loss_sla_prob, category_sla_prob)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            result['window_start'],
            result['window_end'],
            result['burstiness'],
            result['anomaly'],
            result['trend'],
            json.dumps([]),  # Empty seasonality
            result['latency_prob'],
            result['jitter_prob'],
            result['loss_prob'],
            json.dumps(result['category_probs'])
        ))
    except Exception as e:
        print(f"Error saving to database: {e}")
    
    # 3) Print results
    print("\n=== FORECAST RESULTS ===")
    print(f"Time Window: {result['window_start']} to {result['window_end']}")
    print(f"Burstiness Index: {result['burstiness']}")
    print(f"Anomaly Score: {result['anomaly']}")
    print(f"Traffic Trend: {result['trend']} Mbps/min")
    print(f"\nNETWORK QUALITY:")
    print(f"Latency SLA Prob (> {SLA_LATENCY_MS}ms): {result['latency_prob']:.3f}")
    print(f"Jitter SLA Prob (> {SLA_JITTER_MS}ms): {result['jitter_prob']:.3f}")
    print(f"Loss SLA Prob (> {SLA_LOSS_PCT}%): {result['loss_prob']:.3f}")
    print(f"\nCATEGORY THROUGHPUT (< {SLA_THROUGHPUT_MBPS}Mbps):")
    for category, prob in result['category_probs'].items():
        print(f"  {category}: {prob:.3f}")
    
    print("\n=== FORECAST COMPLETED ===")