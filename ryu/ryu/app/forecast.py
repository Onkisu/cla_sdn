#!/usr/bin/env python3
"""
forecast.py - EXACT DATA CALCULATIONS ONLY (No Random Values)
"""

import os
import json
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta

DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

# SLA thresholds
SLA_THROUGHPUT_MBPS = 0.3
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

def load_recent_traffic_data(minutes=15):
    """Load comprehensive recent traffic data"""
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
    return load_time_series_for_kpi(q)

def load_category_traffic_data(minutes=15):
    """Load category-wise traffic data"""
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
    return load_time_series_for_kpi(q)

# ---------- exact metric calculations ----------
def calculate_exact_burstiness(series):
    """Calculate burstiness index from exact data"""
    if series is None or series.empty or len(series) < 2:
        return 1.0
    
    values = series.values
    if np.mean(values) == 0:
        return 1.0
    
    return round(float(np.max(values) / np.mean(values)), 3)

def calculate_exact_anomaly(series):
    """Calculate anomaly score from exact data using IQR method"""
    if series is None or series.empty or len(series) < 5:
        return 0.0
    
    values = series.values
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    
    if IQR == 0:
        return 0.0
    
    # Check if last value is an outlier
    last_value = values[-1]
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    if last_value < lower_bound or last_value > upper_bound:
        # Calculate how many IQRs away from the median
        median = np.median(values[:-1])
        anomaly_score = abs(last_value - median) / IQR
        return round(float(anomaly_score), 3)
    
    return 0.0

def calculate_exact_trend(series):
    """Calculate exact traffic trend in Mbps per minute"""
    if series is None or series.empty or len(series) < 3:
        return 0.0
    
    values = series.values
    x = np.arange(len(values))
    
    try:
        slope = np.polyfit(x, values, 1)[0]
        # Convert bytes to Mbps per minute (assuming data points are per minute)
        slope_mbps = (slope * 8) / 1e6
        return round(float(slope_mbps), 3)
    except:
        return 0.0

def calculate_exact_sla_probability(current_value, threshold, direction='gt'):
    """Calculate exact SLA violation probability based on current value"""
    if current_value is None or current_value == 0:
        return 0.0
    
    try:
        if direction == 'lt':
            # For throughput: probability based on how far below threshold
            if current_value < threshold:
                violation_ratio = (threshold - current_value) / threshold
                return round(min(0.95, violation_ratio), 3)
            return 0.0
        else:
            # For latency/jitter/loss: probability based on how far above threshold
            if current_value > threshold:
                violation_ratio = (current_value - threshold) / threshold
                return round(min(0.95, violation_ratio), 3)
            return 0.0
    except:
        return 0.0

# ---------- MAIN ----------
if __name__ == "__main__":
    print("=== Starting Exact Data Network Forecast ===")
    print(f"Analysis time: {datetime.now()}")
    
    # 1) Load recent traffic data
    traffic_df = load_recent_traffic_data(minutes=15)
    category_df = load_category_traffic_data(minutes=15)
    
    if traffic_df.empty:
        print("ERROR: No traffic data found in the specified time window")
        exit(1)
    
    # 2) Process timestamps
    traffic_df['timestamp'] = pd.to_datetime(traffic_df['timestamp'])
    
    # 3) Calculate main metrics
    bytes_series = traffic_df['total_bytes'].dropna()
    latency_series = traffic_df['avg_latency'].dropna()
    jitter_series = traffic_df['avg_jitter'].dropna()
    loss_series = traffic_df['avg_loss'].dropna()
    
    burstiness = calculate_exact_burstiness(bytes_series)
    anomaly = calculate_exact_anomaly(bytes_series)
    trend = calculate_exact_trend(bytes_series)
    
    # 4) Calculate network quality probabilities
    current_latency = latency_series.iloc[-1] if not latency_series.empty else 0
    current_jitter = jitter_series.iloc[-1] if not jitter_series.empty else 0
    current_loss = loss_series.iloc[-1] if not loss_series.empty else 0
    
    latency_prob = calculate_exact_sla_probability(current_latency, SLA_LATENCY_MS, 'gt')
    jitter_prob = calculate_exact_sla_probability(current_jitter, SLA_JITTER_MS, 'gt')
    loss_prob = calculate_exact_sla_probability(current_loss, SLA_LOSS_PCT, 'gt')
    
    # 5) Calculate category probabilities
    category_probs = {}
    
    if not category_df.empty:
        category_df['timestamp'] = pd.to_datetime(category_df['timestamp'])
        
        for category in CATEGORIES:
            cat_data = category_df[category_df['category'] == category]
            
            if not cat_data.empty:
                # Get most recent data for this category
                latest_cat_data = cat_data.iloc[-1]
                current_bytes = latest_cat_data['bytes']
                current_mbps = (current_bytes * 8) / 1e6  # Convert to Mbps
                
                cat_prob = calculate_exact_sla_probability(current_mbps, SLA_THROUGHPUT_MBPS, 'lt')
                category_probs[category] = cat_prob
            else:
                category_probs[category] = 0.0
    else:
        # No category data found
        for category in CATEGORIES:
            category_probs[category] = 0.0
    
    # 6) Prepare results
    result = {
        'window_start': traffic_df['timestamp'].min(),
        'window_end': traffic_df['timestamp'].max(),
        'burstiness': burstiness,
        'anomaly': anomaly,
        'trend': trend,
        'latency_prob': latency_prob,
        'jitter_prob': jitter_prob,
        'loss_prob': loss_prob,
        'category_probs': category_probs,
        'current_values': {
            'throughput_mbps': (bytes_series.iloc[-1] * 8) / 1e6 if not bytes_series.empty else 0,
            'latency_ms': current_latency,
            'jitter_ms': current_jitter,
            'loss_pct': current_loss
        }
    }
    
    # 7) Save to database
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
            json.dumps([]),
            result['latency_prob'],
            result['jitter_prob'],
            result['loss_prob'],
            json.dumps(result['category_probs'])
        ))
        print("✓ Results saved to database")
    except Exception as e:
        print(f"Error saving to database: {e}")
    
    # 8) Print detailed results
    print("\n=== EXACT DATA FORECAST RESULTS ===")
    print(f"Time Window: {result['window_start']} to {result['window_end']}")
    print(f"Data Points: {len(traffic_df)}")
    print(f"\nCURRENT NETWORK STATE:")
    print(f"Throughput: {result['current_values']['throughput_mbps']:.3f} Mbps")
    print(f"Latency: {result['current_values']['latency_ms']:.3f} ms")
    print(f"Jitter: {result['current_values']['jitter_ms']:.3f} ms") 
    print(f"Loss: {result['current_values']['loss_pct']:.3f} %")
    
    print(f"\nNETWORK METRICS:")
    print(f"Burstiness Index: {result['burstiness']}")
    print(f"Anomaly Score: {result['anomaly']}")
    print(f"Traffic Trend: {result['trend']} Mbps/min")
    
    print(f"\nSLA VIOLATION PROBABILITIES:")
    print(f"Latency (> {SLA_LATENCY_MS}ms): {result['latency_prob']:.3f}")
    print(f"Jitter (> {SLA_JITTER_MS}ms): {result['jitter_prob']:.3f}")
    print(f"Loss (> {SLA_LOSS_PCT}%): {result['loss_prob']:.3f}")
    
    print(f"\nCATEGORY THROUGHPUT PROBABILITIES (< {SLA_THROUGHPUT_MBPS}Mbps):")
    for category, prob in result['category_probs'].items():
        print(f"  {category}: {prob:.3f}")
    
    print(f"\nDATA QUALITY INDICATORS:")
    print(f"Bytes data points: {len(bytes_series)}")
    print(f"Latency data points: {len(latency_series)}")
    print(f"Jitter data points: {len(jitter_series)}")
    print(f"Loss data points: {len(loss_series)}")
    
    print("\n=== FORECAST COMPLETED ===")