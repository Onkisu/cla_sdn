#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
import time
import joblib
import os
import sys
import warnings

warnings.filterwarnings('ignore')

# === CONFIG ===
DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"
MODEL_FILE = "congestion_model_v2.pkl"
RETRAIN_INTERVAL = 3600  # 1 Jam (sesuai request)
PREDICT_HORIZON = 3      # Prediksi 3 detik ke depan

engine = create_engine(DB_URI)

def get_data(limit_hours=1):
    """Mengambil data mentah dari DB"""
    query = f"""
        SELECT timestamp, dpid, port_no, tx_bytes, tx_packets, tx_dropped
        FROM traffic.port_stats_real
        WHERE timestamp >= NOW() - INTERVAL '{limit_hours} hour'
        ORDER BY timestamp ASC
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"DB Error: {e}")
        return pd.DataFrame()

def feature_engineering(df):
    """
    Mencari pola dari data mentah.
    Disini kita membuat parameter baru seperti Packet Rate, Acceleration, Jitter.
    """
    if df.empty: return None, None
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Fokus pada switch yang sibuk saja (aggregasi per detik)
    df = df.set_index('timestamp')
    df_res = df.resample('1s').sum()
    
    # 1. Hitung Rate (BPS & PPS) dari counter kumulatif
    df_res['bps'] = df_res['tx_bytes'].diff() * 8
    df_res['pps'] = df_res['tx_packets'].diff()
    df_res['drops_now'] = df_res['tx_dropped'].diff().fillna(0)
    
    # Bersihkan noise data awal
    df_res = df_res[df_res['bps'] >= 0].fillna(0)
    
    # --- FITUR BARU (PREDICTOR PARAMETERS) ---
    
    # A. Acceleration (Seberapa cepat trafik naik?)
    # Jika trafik naik drastis dalam 1 detik, buffer mungkin belum penuh, tapi AKAN penuh.
    df_res['accel_bps'] = df_res['bps'].diff()
    df_res['accel_pps'] = df_res['pps'].diff()
    
    # B. Packet Density (Rasio)
    # Serangan biasanya paket kecil (PPS tinggi, BPS rendah).
    # Download normal paket besar (PPS rendah, BPS tinggi).
    df_res['pkt_density'] = df_res['bps'] / (df_res['pps'] + 1)
    
    # C. Jitter Proxy (Standard Deviasi PPS dalam 3 detik terakhir)
    df_res['pps_jitter'] = df_res['pps'].rolling(3).std()
    
    # --- TARGET LABEL (MASA DEPAN) ---
    # Kita ingin memprediksi: Apakah Total Drops dalam 3 detik KEDEPAN > 0?
    indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=PREDICT_HORIZON)
    df_res['future_drops'] = df_res['drops_now'].rolling(window=indexer).sum()
    
    # Label: 1 = Akan Macet, 0 = Aman
    df_res['target'] = (df_res['future_drops'] > 10).astype(int) # Threshold toleransi 10 drop
    
    # Hapus baris NaN
    df_clean = df_res.dropna()
    
    # Daftar kolom fitur untuk ML
    features = ['bps', 'pps', 'accel_bps', 'accel_pps', 'pkt_density', 'pps_jitter']
    
    return df_clean, features

def train_job():
    print(f"\n🔄 [RETRAIN] Starting hourly training job...")
    df = get_data(limit_hours=2) # Ambil data 2 jam terakhir
    
    if len(df) < 300:
        print("⚠️ Data belum cukup untuk training (Need > 5 mins data).")
        return None, None
        
    data, cols = feature_engineering(df)
    
    if data is None or len(data) < 50:
        print("⚠️ Data fitur tidak valid.")
        return None, None
        
    X = data[cols]
    y = data['target']
    
    if y.sum() == 0:
        print("⚠️ Belum ada event congestion (Drop=0). ML belum bisa belajar mana yang 'macet'.")
        return None, None

    # Model XGBoost (Gradient Boosting)
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.05,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    
    model.fit(X, y)
    
    # Simpan
    joblib.dump((model, cols), MODEL_FILE)
    print("✅ Model Trained & Saved Successfully.")
    return model, cols

def prediction_loop():
    model = None
    features = None
    last_train = 0
    
    # Coba load model lama
    if os.path.exists(MODEL_FILE):
        try:
            model, features = joblib.load(MODEL_FILE)
            print("📂 Loaded existing model.")
        except: pass

    print("🚀 Prediction Service Running... (Press Ctrl+C to stop)")
    
    while True:
        now = time.time()
        
        # 1. Cek Jadwal Retrain (Setiap 1 Jam)
        if now - last_train > RETRAIN_INTERVAL:
            new_model, new_feats = train_job()
            if new_model:
                model = new_model
                features = new_feats
            last_train = now
            
        # 2. Real-time Prediction
        if model:
            # Ambil data sedikit saja (10 detik terakhir) untuk inferensi cepat
            df_recent = get_data(limit_hours=0.005) 
            if not df_recent.empty:
                df_feat, _ = feature_engineering(df_recent)
                
                if df_feat is not None and not df_feat.empty:
                    # Ambil kondisi detik terakhir
                    last_state = df_feat.iloc[[-1]][features]
                    
                    # Prediksi Probabilitas Macet
                    prob_congestion = model.predict_proba(last_state)[0][1]
                    
                    # Metrics Display
                    cur_pps = last_state['pps'].values[0]
                    cur_accel = last_state['accel_pps'].values[0]
                    
                    timestamp = df_feat.index[-1].strftime('%H:%M:%S')
                    
                    # Logic Output
                    if prob_congestion > 0.7:
                        status = "🔴 PREDIKSI: CONGESTION AKAN TERJADI!"
                        reason = f"(High PPS Accel: {cur_accel:.0f})"
                    elif prob_congestion > 0.4:
                        status = "🟡 WARNING: Traffic Ramp-Up Detected"
                        reason = f"(PPS Rising)"
                    else:
                        status = "🟢 Aman"
                        reason = ""
                        
                    print(f"[{timestamp}] {status} | Prob: {prob_congestion:.0%} | PPS: {cur_pps:.0f} {reason}")
        else:
            print("⏳ Menunggu training data pertama...", end='\r')
            
        time.sleep(1)

if __name__ == "__main__":
    try:
        prediction_loop()
    except KeyboardInterrupt:
        print("\nStopped.")