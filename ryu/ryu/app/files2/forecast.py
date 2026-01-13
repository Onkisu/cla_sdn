#!/usr/bin/env python3
"""
AI BRAIN - SPINE LEAF EDITION (FIXED)
- Target: Switch l1 (DPID 2) where Victim (h2) is connected.
- Updates: Better error handling & aligned with Smart Traffic Gen.
"""
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
import time
import datetime
import warnings

warnings.filterwarnings('ignore')

DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
engine = create_engine(DB_URI)

# WARNING THRESHOLD
# Karena link 10Mbps, warning di set sedikit dibawah congestion point
WARNING_THRESHOLD = 8000000 # 8 Mbps
TARGET_DPID = "2" # FIX: Sesuai dengan Switch l1 di Simulation.py

def get_data(seconds=600):
    query = f"""
        SELECT timestamp, sum(throughput_bps) as throughput, sum(dropped_count) as drops
        FROM traffic.flow_stats_real
        WHERE timestamp >= NOW() - INTERVAL '{seconds} seconds'
        AND dpid = '{TARGET_DPID}' 
        GROUP BY timestamp
        ORDER BY timestamp ASC
    """
    try:
        df = pd.read_sql(query, engine)
        if df.empty: return None
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Resample 1s untuk mengisi detik yang bolong dengan interpolasi
        # atau ffill agar derivatif tidak putus
        df = df.set_index('timestamp').resample('1s').mean().interpolate()
        return df
    except Exception as e:
        print(f"DB Error: {e}")
        return None

def engineer_features(df):
    df = df.copy()
    
    # 1. Lags (Window lebih pendek agar responsif)
    for i in [1, 2]:
        df[f'lag_{i}'] = df['throughput'].shift(i)
    
    # 2. Physics
    # Velocity: Delta throughput saat ini vs 1 detik lalu
    df['velocity'] = df['throughput'] - df['lag_1']
    
    # Acceleration: Apakah kenaikannya makin cepat?
    # Ini fitur KUNCI untuk mendeteksi Ramp-Up sebelum Spike
    df['acceleration'] = df['velocity'] - (df['lag_1'] - df['lag_2'])
    
    # 3. Volatility (Short Window)
    # Window 3 detik cukup untuk melihat ketidakstabilan mendadak
    df['volatility'] = df['throughput'].rolling(window=3).std()
    
    return df.dropna()

def train_and_predict():
    # XGBoost Regressor
    model = xgb.XGBRegressor(n_estimators=100, max_depth=4, learning_rate=0.1)
    print(f"🚀 AI WATCHING DPID {TARGET_DPID} (Connected to Victim)...")
    
    while True:
        # Ambil data training (30 menit terakhir cukup untuk pola pendek)
        df = get_data(seconds=1800)
        
        if df is None or len(df) < 20:
            print("⏳ Waiting for more data generation...", end="\r")
            time.sleep(2)
            continue
            
        df_feat = engineer_features(df)
        if df_feat.empty: continue
        
        # Target: Max throughput in next 2 seconds
        df_feat['target'] = df_feat['throughput'].rolling(window=2).max().shift(-2)
        
        df_train = df_feat.dropna()
        if len(df_train) < 10: continue

        X = df_train.drop(['target', 'drops'], axis=1, errors='ignore')
        y = df_train['target']
        
        model.fit(X, y)
        
        # --- PREDICT LIVE ---
        current_state = X.iloc[[-1]]
        try:
            pred_bps = model.predict(current_state)[0]
        except:
            continue
        
        curr_bps = current_state['throughput'].values[0]
        curr_acc = current_state['acceleration'].values[0]
        
        # --- PROACTIVE LOGIC ---
        status = "🟢 NORMAL"
        
        # LOGIC 1: Hard Threshold Prediction
        if pred_bps > WARNING_THRESHOLD:
            status = "🔴 PREDICTED CONGESTION"
            # Log warning ke DB
            try:
                engine.execute(f"INSERT INTO traffic.congestion_logs VALUES (NOW(), {pred_bps}, {curr_bps}, 'WARNING')")
            except: pass

        # LOGIC 2: Pattern Detection (Ramp Up Phase)
        # Jika Acceleration positif tinggi DAN throughput sedang naik
        elif curr_acc > 1000000 and curr_bps > 4000000:
            status = "⚠️ DETECTING RAMP-UP"
        
        print(f"[{datetime.datetime.now().time().strftime('%H:%M:%S')}] Real:{curr_bps/1e6:.1f}M | Pred:{pred_bps/1e6:.1f}M | Acc:{curr_acc/1e6:.1f} | {status}")
        
        time.sleep(1)

if __name__ == "__main__":
    train_and_predict()