#!/usr/bin/env python3
# file: brain_ml.py

import pandas as pd
import xgboost as xgb
from sqlalchemy import create_engine
import time
import joblib
import os
import warnings
warnings.filterwarnings('ignore')

# === CONFIG ===
DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"
MODEL_FILE = "congestion_predictor.pkl"
RETRAIN_INTERVAL = 3600  # 1 Jam
RETRY_FAST = 10          # 10 Detik (Coba cepat saat awal)

engine = create_engine(DB_URI)

def get_raw_data(minutes=60):
    """Mengambil raw data dari DB"""
    query = f"""
        SELECT timestamp, tx_packets, tx_bytes, tx_dropped
        FROM traffic.port_stats_real
        WHERE timestamp >= NOW() - INTERVAL '{minutes} minutes'
        ORDER BY timestamp ASC
    """
    try:
        return pd.read_sql(query, engine)
    except:
        return pd.DataFrame()

def feature_engineering(df):
    """
    MENGUBAH DATA MENTAH MENJADI PENGETAHUAN
    Disini kita hitung 'Akselerasi' dan 'Rasio'.
    """
    if df.empty: return None, None
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').resample('1s').sum()
    
    # 1. Hitung Delta (Perubahan per detik)
    df['pps'] = df['tx_packets'].diff().fillna(0)
    df['bps'] = df['tx_bytes'].diff().fillna(0)
    df['drops'] = df['tx_dropped'].diff().fillna(0)
    
    # Filter noise
    df = df[df['bps'] >= 0]
    
    # --- FITUR KUNCI PREDIKSI ---
    # Akselerasi: Seberapa cepat trafik naik?
    df['accel_pps'] = df['pps'].diff()
    
    # Rasio Paket: Membedakan Download Normal vs Serangan
    # (Serangan = PPS Tinggi, BPS Kecil -> Rasio Kecil)
    df['pkt_ratio'] = df['bps'] / (df['pps'] + 1)
    
    # Jitter Proxy: Kestabilan trafik
    df['jitter'] = df['pps'].rolling(3).std()
    
    # --- TARGET LABEL (MASA DEPAN) ---
    # Apakah ada drop dalam 3 detik ke depan?
    indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=3)
    df['future_drops'] = df['drops'].rolling(window=indexer).sum()
    
    # Label: 1 jika macet, 0 jika aman
    df['target'] = (df['future_drops'] > 5).astype(int)
    
    df_clean = df.dropna()
    features = ['pps', 'bps', 'accel_pps', 'pkt_ratio', 'jitter']
    
    return df_clean, features

def train_job():
    print("\n🔄 [TRAIN] Melatih Model...")
    df = get_raw_data(minutes=120) # Data 2 jam terakhir
    
    # Butuh minimal 3 menit data
    if len(df) < 180: 
        print(f"⚠️ Data belum cukup ({len(df)} baris). Menunggu Traffic Generator...")
        return None, None
        
    data, cols = feature_engineering(df)
    
    # Cek apakah pernah ada macet?
    if data['target'].sum() == 0:
        print("⚠️ Belum ada sejarah kemacetan. ML belum bisa belajar.")
        print("👉 Tunggu sampai simulasi menjalankan mode 'ATTACK'.")
        return None, None

    # Train XGBoost
    model = xgb.XGBClassifier(n_estimators=100, max_depth=5, eval_metric='logloss')
    model.fit(data[cols], data['target'])
    
    joblib.dump((model, cols), MODEL_FILE)
    print("✅ Model BERHASIL dibuat & disimpan!")
    return model, cols

def run():
    model = None
    cols = None
    last_train = 0
    
    # Load jika ada
    if os.path.exists(MODEL_FILE):
        try: model, cols = joblib.load(MODEL_FILE); print("📂 Model loaded.")
        except: pass
        
    print("🚀 PREDICTION ENGINE STARTED...")
    
    while True:
        now = time.time()
        
        # Logika Retrain
        interval = RETRAIN_INTERVAL if model else RETRY_FAST
        if now - last_train > interval:
            res = train_job()
            if res[0]: model, cols = res
            last_train = now
            
        # Logika Prediksi Real-time
        if model:
            # Ambil data sedikit saja (10 detik)
            df_recent = get_raw_data(minutes=0.5)
            data, _ = feature_engineering(df_recent)
            
            if data is not None and not data.empty:
                last_row = data.iloc[[-1]][cols]
                prob = model.predict_proba(last_row)[0][1]
                
                pps = last_row['pps'].values[0]
                accel = last_row['accel_pps'].values[0]
                
                if prob > 0.7:
                    print(f"🔴 BAHAYA! Congestion Diprediksi! (Prob: {prob:.0%} | Accel: {accel:.0f})")
                elif prob > 0.4:
                    print(f"🟡 WARNING: Traffic Mencurigakan (Prob: {prob:.0%})")
                else:
                    print(f"🟢 Aman (PPS: {pps:.0f})", end='\r')
        else:
            print("⏳ Menunggu data training...", end='\r')
            
        time.sleep(1)

if __name__ == "__main__":
    run()