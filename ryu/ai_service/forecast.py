#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine, text
from datetime import timedelta
import time

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"
TABLE = "traffic.flow_stats_"
TABLE_FORECAST = "forecast_1h"

# Target Kolom
TARGET = "throughput_bps"

# Threshold untuk membedakan 'Sine Wave Controller' vs 'Real Burst'
# Sine wave max ~19,800 bytes/sec * 8 = ~158,400 bps. 
# Kita set sedikit di atas itu untuk aman.
BURST_THRESHOLD_BPS = 250_000 

# Kita coba prediksi 10 detik ke depan agar sempat mitigasi
PREDICTION_HORIZON_SEC = 10 

engine = create_engine(DB_URI)

# =========================
# 1. FETCH DATA
# =========================
def get_training_data(hours=6):
    """
    Ambil data cukup banyak (6 jam) agar pola siklus 
    20 menit/30 menit dari spine_leaf_bursty.py terlihat oleh model.
    """
    print(f"📥 Fetching data for last {hours} hours...")
    
    # Ambil data bytes_tx, sum per detik (agregat seluruh DPID jika perlu, atau spesifik dpid=5)
    # Kita ambil agregat SUM semua flow di detik yang sama untuk melihat total load
    query = f"""
        with x as (
        SELECT 
            date_trunc('second', timestamp) as detik, 
            dpid, 
            max(bytes_tx) as total_bytes
        FROM traffic.flow_stats_
        GROUP BY detik, dpid
        ORDER BY detik desc, dpid 

        )
        select detik as ts, total_bytes * 8 as throughput_bps from x where dpid = 5
        and (total_bytes * 8 ) > 100000 
        and detik >= NOW() - INTERVAL '{hours} hour'
        order by 1 asc
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return None

    if df.empty:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    # Resample ke 1 detik, isi bolong dengan fill forward (atau 0 jika mati total)
    df = df.resample('1s').max().fillna(method='ffill').fillna(0)
    
    return df

# =========================
# 2. FEATURE ENGINEERING (CRITICAL FOR PREDICTION)
# =========================
def create_features(df):
    data = df.copy()

    # 1. Labeli State: 0 = Normal (Sine Wave), 1 = Burst
    data['is_burst'] = (data[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    # 2. Hitung "Consecutive Steady Seconds" (Fitur KUNCI)
    # Ini menghitung sudah berapa detik kita "aman". 
    # Karena script burst pakai timer (sleep 1800s), fitur ini akan naik terus 
    # sampai mendekati 1800, lalu model belajar bahwa di angka 1800 -> BOOM.
    
    # Logika: Reset counter jika is_burst == 1, else increment
    # Menggunakan metode grouping pandas yang cepat
    group_id = data['is_burst'].cumsum() # Buat grup baru tiap kali burst muncul
    
    # Kita hanya peduli menghitung durasi '0' (tidak burst)
    data['consecutive_steady_sec'] = data.groupby(group_id).cumcount()
    
    # Jika saat ini sedang burst, set counter steady ke 0
    data.loc[data['is_burst'] == 1, 'consecutive_steady_sec'] = 0

    # 3. Rolling Statistics (Trend jangka pendek)
    # Apakah rata-rata 30 detik terakhir mulai naik sedikit?
    data['roll_mean_30s'] = data[TARGET].rolling(window=30).mean()
    data['roll_std_30s'] = data[TARGET].rolling(window=30).std()
    
    # 4. Temporal Features (Siklus Jam)
    # Membantu jika pola terkait jam dinding (kurang relevan buat script sleep, tapi bagus buat validasi)
    data['minute'] = data.index.minute
    data['second'] = data.index.second

    # 5. Lag Features (Tetap simpan untuk konteks jangka pendek)
    # Tapi jangan bergantung hanya pada t-1
    for l in [1, 5, 10]:
        data[f'lag_{l}'] = data[TARGET].shift(l)

    # 6. Target Variable (Shift ke Masa Depan)
    # Kita ingin memprediksi throughput di masa depan (t + PREDICTION_HORIZON)
    # menggunakan data saat ini (t)
    data['target_future'] = data[TARGET].shift(-PREDICTION_HORIZON_SEC)

    # Hapus NaN akibat shifting/rolling
    data = data.dropna()

    return data

# =========================
# 3. TRAIN & PREDICT
# =========================
def train_and_forecast(df):
    # Fitur yang digunakan untuk prediksi
    features = [
        'consecutive_steady_sec', # <--- INI PALING PENTING
        'throughput_bps',         # Nilai sekarang
        'roll_mean_30s',          # Rata-rata pelan
        'roll_std_30s',           # Volatilitas
        'lag_1', 'lag_5'
    ]
    
    if len(df) < 500:
        print("❌ Data terlalu sedikit untuk training.")
        return []

    # Split Train/Test (Time Series split)
    train_size = int(len(df) * 0.9)
    train = df.iloc[:train_size]
    test = df.iloc[train_size:] # Sebenarnya kita predict 'live', test hanya validasi
    
    X_train = train[features]
    y_train = train['target_future'] # Target adalah masa depan

    # Setup XGBoost Regressor
    model = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=7,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        objective='reg:squarederror',
        n_jobs=-1,
        random_state=42
    )

    print("🧠 Training XGBoost Model...")
    model.fit(X_train, y_train)

    # --- LIVE PREDICTION ---
    # Ambil baris data PALING AKHIR (Current State)
    last_row = df.iloc[[-1]][features].copy()
    current_time = df.index[-1]
    
    # Lakukan prediksi untuk horizon ke depan
    pred_bps = model.predict(last_row)[0]
    
    # Cek 'Consecutive Steady' saat ini untuk log
    curr_steady = last_row['consecutive_steady_sec'].values[0]
    curr_bps = last_row['throughput_bps'].values[0]
    
    print("\n" + "="*40)
    print(f"⏱️  Current Time    : {current_time}")
    print(f"📊 Current Traffic : {curr_bps:,.0f} bps")
    print(f"⏳ Time Since Burst: {curr_steady:.0f} sec ({(curr_steady/60):.1f} mins)")
    print("-" * 40)
    
    # Alert Logic
    status = "SAFE"
    if pred_bps > BURST_THRESHOLD_BPS:
        status = "⚠️ CONGESTION PREDICTED"
        if curr_bps < BURST_THRESHOLD_BPS:
            print(f"🚀 PREDICTION: Burst will start in ~{PREDICTION_HORIZON_SEC} seconds!")
        else:
            print(f"🔥 PREDICTION: Congestion will continue.")
            
    print(f"🔮 Forecast (+{PREDICTION_HORIZON_SEC}s): {pred_bps:,.0f} bps [{status}]")
    print("="*40 + "\n")

    return [(current_time + timedelta(seconds=PREDICTION_HORIZON_SEC), pred_bps)]

# =========================
# 4. MAIN LOOP
# =========================
# =========================
# 4. OPTIMIZED MAIN LOOP
# =========================

def main():
    print("🚀 Starting Optimized Forecast Monitor...")
    
    model = None
    last_train_time = 0
    TRAIN_INTERVAL = 900  # Train ulang setiap 15 menit (900 detik)
    
    try:
        while True:
            loop_start = time.time()
            
            # --- STEP 1: FETCH DATA ---
            # Kita butuh data terbaru untuk feature engineering (terutama 'steady_sec')
            df_raw = get_training_data(hours=6)
            
            if df_raw is None:
                time.sleep(5)
                continue

            df_features = create_features(df_raw)
            if df_features.empty:
                time.sleep(5)
                continue

            # --- STEP 2: TRAIN (Hanya jika belum ada model atau sudah > 15 menit) ---
            if model is None or (time.time() - last_train_time > TRAIN_INTERVAL):
                print(f"\n🔄 Retraining Model (Interval {TRAIN_INTERVAL}s passed)...")
                
                # Logic training ditarik kesini (simplified version of train_and_forecast)
                features = [
                    'consecutive_steady_sec', 'throughput_bps', 
                    'roll_mean_30s', 'roll_std_30s', 'lag_1', 'lag_5'
                ]
                X_train = df_features[features]
                y_train = df_features['target_future']
                
                model = xgb.XGBRegressor(
                    n_estimators=500, max_depth=7, learning_rate=0.03,
                    n_jobs=-1, random_state=42
                )
                model.fit(X_train, y_train)
                last_train_time = time.time()
                print("✅ Model Updated.")

            # --- STEP 3: PREDICT (Sangat Cepat) ---
            features = [
                'consecutive_steady_sec', 'throughput_bps', 
                'roll_mean_30s', 'roll_std_30s', 'lag_1', 'lag_5'
            ]
            last_row = df_features.iloc[[-1]][features]
            current_time = df_features.index[-1]
            
            pred_bps = model.predict(last_row)[0]
            
            # Logging & Alerting
            curr_steady = last_row['consecutive_steady_sec'].values[0]
            print(f"[{current_time}] Steady: {curr_steady}s | Pred: {pred_bps:,.0f} bps", end="\r")

            # --- STEP 4: SAVE TO DB ---
            # (Simpan logic save to DB disini)
            ts_pred = current_time + timedelta(seconds=PREDICTION_HORIZON_SEC)
            try:
                pd.DataFrame({'ts': [ts_pred], 'y_pred': [pred_bps]}).to_sql(
                TABLE_FORECAST, engine, if_exists='append', index=False
                )

            except:
                pass

            # Hitung waktu eksekusi
            exec_time = time.time() - loop_start
            
            # Sleep cerdas: Biar total loop pas 5 detik
            sleep_time = max(0, 5 - exec_time) 
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Stopped.")

if __name__ == "__main__":
    main()