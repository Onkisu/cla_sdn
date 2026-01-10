#!/usr/bin/env python3
"""
AI BRAIN - SPINE LEAF EDITION
- Focus: Monitor Switch l2 (DPID 5) where Victim resides.
- Features: Velocity, Acceleration, Volatility (Std Dev).
- Proactive Warning System.
"""
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
import time
import datetime
import warnings

warnings.filterwarnings('ignore')

DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"
engine = create_engine(DB_URI)

# Target Link Capacity = 10 Mbps
# Warning jika prediksi tembus 8.5 Mbps
WARNING_THRESHOLD = 8500000 
TARGET_DPID = "5" # Switch l2 (Hex 5)

def get_data(seconds=600):
    # Ambil data spesifik DPID 5
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
        df = df.set_index('timestamp').resample('1s').sum().fillna(0)
        return df
    except:
        return None

def engineer_features(df):
    df = df.copy()
    
    # 1. Basic Lags
    for i in [1, 2, 3]:
        df[f'lag_{i}'] = df['throughput'].shift(i)
    
    # 2. Physics (Derivative)
    # Velocity: Kecepatan kenaikan
    df['velocity'] = df['throughput'] - df['lag_1']
    # Acceleration: Percepatan (Jerk)
    df['acceleration'] = df['velocity'] - (df['lag_1'] - df['lag_2'])
    
    # 3. Volatility (Panic Indicator)
    # Jika std dev naik tinggi, trafik tidak stabil -> Tanda bahaya
    df['volatility'] = df['throughput'].rolling(window=4).std()
    
    return df.dropna()

def train_and_predict():
    model = xgb.XGBRegressor(n_estimators=100, max_depth=5, learning_rate=0.1)
    print(f"🚀 AI WATCHING DPID {TARGET_DPID}...")
    
    while True:
        # Ambil data training (1 jam terakhir)
        df = get_data(seconds=3600)
        if df is None or len(df) < 30:
            print("⏳ Collecting data...", end="\r")
            time.sleep(1)
            continue
            
        df_feat = engineer_features(df)
        if df_feat.empty: continue
        
        # Target: Max throughput in next 3 seconds (Peak Prediction)
        df_feat['target'] = df_feat['throughput'].rolling(window=3).max().shift(-3)
        
        df_train = df_feat.dropna()
        if len(df_train) < 10: continue

        X = df_train.drop(['target', 'drops'], axis=1, errors='ignore')
        y = df_train['target']
        
        model.fit(X, y)
        
        # --- PREDICT LIVE ---
        current_state = X.iloc[[-1]]
        pred_bps = model.predict(current_state)[0]
        
        curr_bps = current_state['throughput'].values[0]
        curr_vol = current_state['volatility'].values[0]
        
        # --- PROACTIVE LOGIC ---
        status = "🟢"
        
        # Jika prediksi > threshold (8.5 Mbps)
        if pred_bps > WARNING_THRESHOLD:
            status = "🔴 PREDICTED CONGESTION"
            # INSERT WARNING TO DB
            engine.execute(f"INSERT INTO traffic.congestion_logs VALUES (NOW(), {pred_bps}, {curr_bps}, 'WARNING')")
            
        # Jika Volatilitas Tinggi (Trafik gila-gilaan walau rata-rata masih rendah)
        elif curr_vol > 2000000: 
            status = "🟠 UNSTABLE TRAFFIC"
        
        print(f"[{datetime.datetime.now().time()}] Real:{curr_bps/1e6:.1f}M | Pred:{pred_bps/1e6:.1f}M | Vol:{curr_vol/1e5:.0f} | {status}")
        
        time.sleep(1)

if __name__ == "__main__":
    train_and_predict()