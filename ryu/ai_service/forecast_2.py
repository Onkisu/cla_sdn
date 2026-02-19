#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TABLE_FORECAST = "forecast_1h"

TARGET = "throughput_bps"
BURST_THRESHOLD_BPS = 40_000_000        # 40 Mbps
PREDICTION_HORIZON_SEC = 10
TRAIN_INTERVAL_SEC = 1800               # Re-train setiap 30 menit

# Global Variables
TRAINED_MODEL = None
LAST_TRAIN_TIME = 0

engine = create_engine(DB_URI)

# =========================
# FEATURE LIST (single source of truth)
# =========================
FEATURES = [
    'throughput_bps',
    'consecutive_steady_sec',
    'roll_mean_10s',
    'roll_mean_30s',
    'roll_std_10s',
    'roll_std_30s',
    'roll_max_10s',
    'roll_max_30s',
    'lag_1',
    'lag_3',
    'lag_5',
    'lag_10',
    'lag_20',
    'slope_3s',
    'slope_5s',
    'slope_10s',
    'accel_5s',         # perubahan slope (slope of slope)
    'ratio_to_threshold',
    'is_burst',
    'burst_streak',     # berapa detik berturut-turut dalam kondisi burst
    'pct_change_5s',    # % perubahan 5 detik terakhir
    'ema_10s',          # Exponential Moving Average 10s
    'ema_30s',          # Exponential Moving Average 30s
]

# =========================
# 1. FETCH DATA
# =========================
def get_data(hours=2):
    """
    Ambil data traffic dari DB.
    - hours=8 untuk Training (pola siklus burst 8-10 menit terlihat penuh)
    - hours=2  untuk Prediction (cukup untuk warmup semua fitur)
    """
    query = f"""
        WITH x AS (
            SELECT
                date_trunc('second', timestamp) AS detik,
                dpid,
                MAX(bytes_tx) AS total_bytes
            FROM traffic.flow_stats_
            WHERE timestamp >= NOW() - INTERVAL '{hours} hour'
            GROUP BY detik, dpid
        )
        SELECT
            detik AS ts,
            total_bytes * 8 AS throughput_bps
        FROM x
        WHERE dpid = 5
        ORDER BY ts ASC
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

    # Resample ke 1 detik, ffill lalu bfill untuk gap awal
    df = df.resample('1s').max().ffill().bfill().fillna(0)

    return df


# =========================
# 2. FEATURE ENGINEERING
# =========================
def create_features(df, for_training=False):
    data = df.copy()
    T = TARGET  # alias pendek

    # ── 1. Burst label ──────────────────────────────────────────────
    data['is_burst'] = (data[T] > BURST_THRESHOLD_BPS).astype(int)

    # ── 2. consecutive_steady_sec (FIX: iterative, bukan cumsum) ────
    # Hitung berapa detik berturut-turut dalam kondisi STEADY (is_burst == 0)
    steady_streak = []
    count = 0
    for val in data['is_burst']:
        count = 0 if val == 1 else count + 1
        steady_streak.append(count)
    data['consecutive_steady_sec'] = steady_streak

    # ── 3. burst_streak (kebalikannya: durasi burst berlangsung) ────
    burst_streak = []
    count = 0
    for val in data['is_burst']:
        count = count + 1 if val == 1 else 0
        burst_streak.append(count)
    data['burst_streak'] = burst_streak

    # ── 4. Rolling Statistics ────────────────────────────────────────
    data['roll_mean_10s'] = data[T].rolling(window=10, min_periods=1).mean()
    data['roll_mean_30s'] = data[T].rolling(window=30, min_periods=1).mean()
    data['roll_std_10s']  = data[T].rolling(window=10, min_periods=2).std().fillna(0)
    data['roll_std_30s']  = data[T].rolling(window=30, min_periods=2).std().fillna(0)
    data['roll_max_10s']  = data[T].rolling(window=10, min_periods=1).max()
    data['roll_max_30s']  = data[T].rolling(window=30, min_periods=1).max()

    # ── 5. Exponential Moving Averages ───────────────────────────────
    data['ema_10s'] = data[T].ewm(span=10, adjust=False).mean()
    data['ema_30s'] = data[T].ewm(span=30, adjust=False).mean()

    # ── 6. Lag Features ──────────────────────────────────────────────
    for lag in [1, 3, 5, 10, 20]:
        data[f'lag_{lag}'] = data[T].shift(lag)

    # ── 7. Slope Features (gradient traffic) ─────────────────────────
    # slope_Ns = rata-rata kenaikan per detik dalam N detik terakhir
    data['slope_3s']  = data[T].diff(3)  / 3
    data['slope_5s']  = data[T].diff(5)  / 5
    data['slope_10s'] = data[T].diff(10) / 10

    # Acceleration: apakah slope sedang naik atau turun?
    data['accel_5s'] = data['slope_5s'].diff(5) / 5

    # ── 8. Ratio & Pct Change ────────────────────────────────────────
    data['ratio_to_threshold'] = data[T] / BURST_THRESHOLD_BPS
    data['pct_change_5s'] = data[T].pct_change(periods=5).replace([np.inf, -np.inf], 0).fillna(0)

    # ── 9. Target Variable ───────────────────────────────────────────
    if for_training:
        data['target_future'] = data[T].shift(-PREDICTION_HORIZON_SEC)
        data = data.dropna(subset=['target_future'] + [f'lag_{l}' for l in [1, 3, 5, 10, 20]])
    else:
        # Untuk prediksi: pastikan semua fitur minimal sudah ada
        data = data.dropna(subset=['lag_20', 'roll_mean_30s', 'slope_10s'])

    return data


# =========================
# 3. TRAINING ROUTINE
# =========================
def train_model():
    global TRAINED_MODEL, LAST_TRAIN_TIME

    print("\n🧠 Starting Model Training...")
    start_t = time.time()

    # Ambil data 8 jam untuk menangkap lebih banyak pola burst
    df = get_data(hours=8)
    if df is None or len(df) < 500:
        print("⚠️  Not enough data to train.")
        return

    df_train = create_features(df, for_training=True)

    if len(df_train) < 200:
        print("⚠️  Not enough valid training rows after feature engineering.")
        return

    X = df_train[FEATURES]
    y = df_train['target_future']

    # ── Cek distribusi target ─────────────────────────────────────────
    burst_rows  = (y > BURST_THRESHOLD_BPS).sum()
    normal_rows = (y <= BURST_THRESHOLD_BPS).sum()
    print(f"   Training rows: {len(y)} | Burst: {burst_rows} | Normal: {normal_rows}")

    # ── Sample weight: beri bobot lebih pada transisi burst ──────────
    # Row yang tepat sebelum/sesudah burst lebih penting untuk akurasi
    weights = np.ones(len(y))

    # Identifikasi transisi (perubahan state)
    is_burst_target = (y > BURST_THRESHOLD_BPS).astype(int).values
    for i in range(1, len(is_burst_target)):
        if is_burst_target[i] != is_burst_target[i - 1]:
            # Beri bobot 3x untuk 5 detik di sekitar transisi
            lo = max(0, i - 5)
            hi = min(len(weights), i + 5)
            weights[lo:hi] = 3.0

    # ── XGBoost: parameter yang balance antara akurasi & generalisasi ─
    model = xgb.XGBRegressor(
        n_estimators=600,
        max_depth=5,            # Tidak terlalu dalam → kurangi overfit noise
        learning_rate=0.04,
        subsample=0.8,          # Regularisasi baris
        colsample_bytree=0.8,   # Regularisasi kolom
        colsample_bylevel=0.8,
        min_child_weight=3,     # Hindari split pada noise kecil
        gamma=1.0,              # Minimum gain untuk split
        reg_alpha=0.1,          # L1 regularisasi
        reg_lambda=1.5,         # L2 regularisasi
        n_jobs=-1,
        random_state=42,
        tree_method='hist',     # Lebih cepat
    )

    model.fit(X, y, sample_weight=weights)

    TRAINED_MODEL = model
    LAST_TRAIN_TIME = time.time()

    duration = time.time() - start_t

    # ── Print feature importance top 10 ──────────────────────────────
    importance = sorted(
        zip(FEATURES, model.feature_importances_),
        key=lambda x: x[1], reverse=True
    )
    print(f"✅ Training Done in {duration:.2f}s.")
    print("   Top 10 Features:")
    for fname, fimp in importance[:10]:
        bar = '█' * int(fimp * 200)
        print(f"   {fname:<25} {fimp:.4f} {bar}")
    print(f"   Next training in 30 mins.")


# =========================
# 4. PREDICTION ROUTINE
# =========================
def run_prediction():
    global TRAINED_MODEL

    if TRAINED_MODEL is None:
        return

    df = get_data(hours=2)
    if df is None or len(df) < 30:
        return

    df_feat = create_features(df, for_training=False)
    if df_feat.empty:
        return

    last_row = df_feat.iloc[[-1]]

    # Pastikan semua fitur ada
    missing = [f for f in FEATURES if f not in last_row.columns]
    if missing:
        print(f"\n⚠️  Missing features: {missing}")
        return

    # Prediksi
    pred_val = TRAINED_MODEL.predict(last_row[FEATURES])[0]
    pred_val = max(0, pred_val)  # Tidak mungkin negatif

    ts_now    = pd.Timestamp.now()
    ts_future = ts_now + timedelta(seconds=PREDICTION_HORIZON_SEC)

    # Log
    steady_sec  = last_row['consecutive_steady_sec'].values[0]
    burst_sec   = last_row['burst_streak'].values[0]
    slope_5     = last_row['slope_5s'].values[0]
    actual_bps  = last_row['throughput_bps'].values[0]
    status      = "⚠️  DANGER" if pred_val > BURST_THRESHOLD_BPS else "✅ SAFE"
    trend       = "↑" if slope_5 > 500_000 else ("↓" if slope_5 < -500_000 else "→")

    print(
        f"[{ts_now.strftime('%H:%M:%S')}] "
        f"Now: {actual_bps/1e6:5.1f}M {trend} | "
        f"Steady: {steady_sec:3.0f}s | Burst: {burst_sec:3.0f}s | "
        f"Pred(+10s): {pred_val/1e6:6.1f}M [{status}]",
        end='\r'
    )

    # Simpan ke DB
    try:
        res_df = pd.DataFrame([{
            'ts_created': ts_now,
            'ts':         ts_future,
            'y_pred':     pred_val
        }])
        res_df.to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
    except Exception:
        pass


# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print("🚀 Starting Optimized Forecast Monitor (High Accuracy Mode)...")
    print(f"   Threshold  : {BURST_THRESHOLD_BPS/1e6:.0f} Mbps")
    print(f"   Horizon    : +{PREDICTION_HORIZON_SEC}s")
    print(f"   Features   : {len(FEATURES)}")
    print(f"   Retrain    : every {TRAIN_INTERVAL_SEC//60} min")

    try:
        while True:
            loop_start = time.time()

            # 1. Re-train jika perlu
            if TRAINED_MODEL is None or (time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC):
                train_model()

            # 2. Prediksi
            run_prediction()

            # 3. Sleep cerdas: target loop setiap 5 detik
            elapsed    = time.time() - loop_start
            sleep_time = max(0, 5.0 - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")