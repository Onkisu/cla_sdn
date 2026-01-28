

#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta
import time
import warnings

# Matikan warning pandas agar log bersih
warnings.simplefilter(action='ignore', category=FutureWarning)

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TABLE_FORECAST = "forecast_1h"

# Konfigurasi Target
TARGET = "throughput_bps"
BURST_THRESHOLD_BPS = 120000 
PREDICTION_HORIZON_SEC = 10 
TRAIN_INTERVAL_SEC = 1800  # Train ulang setiap 30 menit

# Global Variables (Menyimpan Model di RAM)
TRAINED_MODEL = None
LAST_TRAIN_TIME = 0

engine = create_engine(DB_URI)

# =========================
# 1. FETCH DATA (Flexible)
# =========================
def get_data(hours=1):
    """
    Mengambil data traffic.
    - hours=6 untuk Training (agar pola siklus 30 menit terlihat)
    - hours=1.5 untuk Prediction (agar query super cepat)
    """
    query = f"""
        with x as (
            SELECT 
                date_trunc('second', timestamp) as detik, 
                dpid, 
                max(bytes_tx) as total_bytes
            FROM traffic.flow_stats_
            WHERE timestamp >= NOW() - INTERVAL '{hours} hour'
            GROUP BY detik, dpid
        )
        SELECT 
            detik as ts, 
            total_bytes * 8 as throughput_bps 
        FROM x 
        WHERE dpid = 5 
        
        ORDER BY ts ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return None

    if df.empty: return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    # Resample ke 1 detik, isi bolong dengan nilai sebelumnya (ffill)
    df = df.resample('1s').max().ffill().fillna(0)
    
    return df

# =========================
# 2. FEATURE ENGINEERING
# =========================
def create_features(df, for_training=False):
    data = df.copy()

    # 1. Labeli State: 0 = Normal/Sepi, 1 = Burst
    data['is_burst'] = (data[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    # 2. Hitung "Consecutive Steady Seconds" (Fitur KUNCI)
    group_id = data['is_burst'].cumsum()
    data['consecutive_steady_sec'] = data.groupby(group_id).cumcount()
    data.loc[data['is_burst'] == 1, 'consecutive_steady_sec'] = 0

    # 3. Rolling Statistics
    data['roll_mean_30s'] = data[TARGET].rolling(window=30).mean()
    data['roll_std_30s'] = data[TARGET].rolling(window=30).std()
    
    # 4. Lag Features
    for l in [1, 5, 10]:
        data[f'lag_{l}'] = data[TARGET].shift(l)

    # 5. Target Variable
    if for_training:
        data['target_future'] = data[TARGET].shift(-PREDICTION_HORIZON_SEC)
        data = data.dropna()
    else:
        data = data.dropna(subset=['roll_mean_30s', 'lag_10'])

    return data

# =========================
# 3. TRAINING ROUTINE (Background Process)
# =========================
def train_model():
    global TRAINED_MODEL, LAST_TRAIN_TIME
    
    print("\n🧠 Starting Model Training (Wait)...")
    start_t = time.time()
    
    # Ambil data 6 jam
    df = get_data(hours=6)
    if df is None or len(df) < 1000:
        print("⚠️ Not enough data to train.")
        return

    # Buat Fitur
    df_train = create_features(df, for_training=True)
    
    features = [
        'consecutive_steady_sec', 
        'throughput_bps', 
        'roll_mean_30s', 
        'roll_std_30s', 
        'lag_1', 'lag_5'
    ]
    
    X = df_train[features]
    y = df_train['target_future']

    # Latih Model
    model = xgb.XGBRegressor(
        n_estimators=500, max_depth=7, learning_rate=0.03,
        n_jobs=-1, random_state=42
    )
    model.fit(X, y)
    
    TRAINED_MODEL = model
    LAST_TRAIN_TIME = time.time()
    
    duration = time.time() - start_t
    print(f"✅ Training Done in {duration:.2f}s. Next training in 30 mins.")

# =========================
# 4. PREDICTION ROUTINE (Fast Process)
# =========================
def run_prediction():
    global TRAINED_MODEL
    
    if TRAINED_MODEL is None: return 

    # Ambil data pendek (1.5 jam)
    df = get_data(hours=1.5) 
    if df is None: return

    df_feat = create_features(df, for_training=False)
    if df_feat.empty: return
    
    # Ambil data DETIK INI (Baris Terakhir)
    last_row = df_feat.iloc[[-1]]
    
    features = [
        'consecutive_steady_sec', 
        'throughput_bps', 
        'roll_mean_30s', 
        'roll_std_30s', 
        'lag_1', 'lag_5'
    ]
    
    # Prediksi
    pred_val = TRAINED_MODEL.predict(last_row[features])[0]
    
    # --- UPDATE LOGIC DISINI ---
    ts_now = pd.Timestamp.now() # Waktu Dibuat (Created At)
    ts_future = ts_now + timedelta(seconds=PREDICTION_HORIZON_SEC) # Waktu Prediksi (Target Time)
    
    # Log ke Layar
    steady_sec = last_row['consecutive_steady_sec'].values[0]
    status = "⚠️ DANGER" if pred_val > BURST_THRESHOLD_BPS else "SAFE"
    
    print(f"[{ts_now.strftime('%H:%M:%S')}] Steady: {steady_sec:.0f}s | Pred (+10s): {pred_val:,.0f} bps [{status}]", end='\r')

    # Simpan ke Database dengan 2 timestamp
    try:
        res_df = pd.DataFrame([{
            'ts_created': ts_now,   # <--- Kolom Baru: Kapan prediksi dibuat
            'ts': ts_future,        # <--- Kolom Lama: Untuk jam berapa prediksi ini
            'y_pred': pred_val
        }])
        res_df.to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
    except Exception as e:
        # Pass jika error minor, print jika perlu debug
        pass 

# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print("🚀 Starting Optimized Forecast Monitor...")
    
    try:
        while True:
            loop_start = time.time()
            
            # 1. Cek apakah waktunya Training Ulang
            if TRAINED_MODEL is None or (time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC):
                train_model()
            
            # 2. Jalankan Prediksi
            run_prediction()
            
            # 3. Sleep Cerdas
            elapsed = time.time() - loop_start
            sleep_time = max(0, 5.0 - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")

