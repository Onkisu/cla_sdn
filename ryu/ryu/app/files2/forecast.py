#!/usr/bin/env python3
"""
PROACTIVE AI FORECASTER
- Retrains every 1 hour.
- Predicts t+1 to t+5 seconds.
- Uses Velocity and Acceleration features to detect congestion ONSET.
"""
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
import time
import datetime
import warnings

warnings.filterwarnings('ignore')

# CONFIG
DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"
TABLE_READ = "traffic.flow_stats_real"
TABLE_ALERT = "traffic.congestion_alerts"
RETRAIN_INTERVAL = 3600 # 1 Jam
PREDICTION_THRESHOLD = 8000000 # 8 Mbps (80% dari link 10Mbps)

engine = create_engine(DB_URI)
model = None

def get_recent_data(seconds=300):
    """Ambil data N detik terakhir untuk input prediksi"""
    query = f"""
        SELECT timestamp, sum(throughput_bps) as throughput
        FROM {TABLE_READ}
        WHERE timestamp >= NOW() - INTERVAL '{seconds} seconds'
        GROUP BY timestamp
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').resample('1s').sum().fillna(0)
    return df

def create_features(df):
    """
    Buat fitur yang menangkap POLA pergerakan trafik.
    - Lag: Nilai masa lalu.
    - Velocity: Perubahan kecepatan (t - t-1).
    - Acceleration: Perubahan percepatan.
    """
    df = df.copy()
    
    # Lag Features
    for i in [1, 2, 3, 5]:
        df[f'lag_{i}'] = df['throughput'].shift(i)
    
    # Derivative Features (Kunci untuk deteksi dini)
    df['velocity'] = df['throughput'] - df['lag_1']
    df['acceleration'] = df['velocity'] - (df['lag_1'] - df['lag_2'])
    
    # Rolling stats
    df['rolling_mean_5'] = df['throughput'].rolling(window=5).mean()
    df['rolling_std_5'] = df['throughput'].rolling(window=5).std()
    
    return df.dropna()

def train_model():
    print(f"[{datetime.datetime.now()}] 🔄 Retraining Model with last 1 hour data...")
    # Ambil data 1 jam terakhir
    query = f"""
        SELECT timestamp, sum(throughput_bps) as throughput
        FROM {TABLE_READ}
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        GROUP BY timestamp
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, engine)
    
    if len(df) < 100:
        print("⚠️ Not enough data to train yet.")
        return None

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').resample('1s').sum().fillna(0)
    
    # Siapkan Supervised Learning Data
    # Target: Throughput 5 detik ke depan (Max value in next 5s)
    # Kita ingin memprediksi 'peak' yang akan datang
    df_feat = create_features(df)
    df_feat['target_future_max'] = df_feat['throughput'].rolling(window=5).max().shift(-5)
    df_feat = df_feat.dropna()
    
    features = [c for c in df_feat.columns if c not in ['target_future_max']]
    X = df_feat[features]
    y = df_feat['target_future_max']
    
    reg = xgb.XGBRegressor(n_estimators=100, max_depth=5, learning_rate=0.1)
    reg.fit(X, y)
    
    print("✅ Model Retrained Successfully.")
    return reg, features

def main_loop():
    global model
    features_col = []
    last_train_time = 0
    
    print("🚀 AI Forecaster Started...")
    
    while True:
        now = time.time()
        
        # 1. Cek Jadwal Retrain
        if now - last_train_time > RETRAIN_INTERVAL or model is None:
            res = train_model()
            if res:
                model, features_col = res
                last_train_time = now
            else:
                time.sleep(10)
                continue

        # 2. Real-time Prediction
        try:
            # Ambil potongan data kecil untuk feature engineering
            df_recent = get_recent_data(seconds=20) 
            if len(df_recent) < 10:
                time.sleep(1)
                continue
                
            df_feat = create_features(df_recent)
            if df_feat.empty: continue
            
            # Ambil baris data terakhir (kondisi saat ini)
            current_state = df_feat.iloc[[-1]][features_col]
            current_throughput = df_feat.iloc[-1]['throughput']
            
            # Predict
            pred_bps = model.predict(current_state)[0]
            
            # 3. Logika Warning Proaktif
            status = "OK"
            if pred_bps > PREDICTION_THRESHOLD:
                status = "⚠️ WARNING: CONGESTION IMMINENT"
                
                # Cek apakah saat ini SUDAH congestion?
                if current_throughput > PREDICTION_THRESHOLD:
                    msg = "CONGESTION ACTIVE"
                else:
                    msg = f"PREDICTED CONGESTION ({int(pred_bps/1000000)} Mbps) IN < 5 SEC"
                
                # Simpan Alert ke DB
                print(f"🚨 {msg}")
                engine.execute(f"INSERT INTO {TABLE_ALERT} VALUES (NOW(), {pred_bps}, '{msg}')")
            
            print(f"\r[{datetime.datetime.now().time()}] Curr: {current_throughput/1e6:.2f} Mbps | Pred (t+5): {pred_bps/1e6:.2f} Mbps | {status}", end="")
            
        except Exception as e:
            print(f"\n❌ Error in loop: {e}")
        
        time.sleep(1)

if __name__ == "__main__":
    main_loop()