#!/usr/bin/env python3
"""
forecast_2.py - High Accuracy Traffic Burst Predictor
======================================================
ROOT CAUSES FIXED:
  1. NaN di fitur (slope/accel/lag) → XGBoost predict 0  
     FIX: fillna(0) di SEMUA fitur setelah kalkulasi
  2. Training tanpa NaN, prediksi ada NaN → inconsistent behavior
     FIX: fillna(0) dilakukan SEBELUM training DAN prediksi (konsisten)
  3. Class imbalance 80% normal vs 20% burst
     FIX: sample_weight burst=5x, transisi=10x
  4. accel_5s warmup 10 row, lag_20 warmup 20 row → window pendek = NaN
     FIX: accel_3s (warmup 6 row), hapus lag_20, semua fillna(0)
  5. dropna() terlalu agresif untuk prediksi → rows hilang → predict 0
     FIX: tidak ada dropna saat prediksi, NaN diganti 0 + median fallback
"""
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
DB_URI                 = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TABLE_FORECAST         = "forecast_1h"
TARGET                 = "throughput_bps"
BURST_THRESHOLD_BPS    = 40_000_000   # 40 Mbps
PREDICTION_HORIZON_SEC = 10
TRAIN_INTERVAL_SEC     = 1800         # Re-train setiap 30 menit

# Globals
TRAINED_MODEL   = None
LAST_TRAIN_TIME = 0
FEATURE_MEDIANS = {}   # Median tiap fitur dari training, untuk fallback NaN

engine = create_engine(DB_URI)

# =========================
# FEATURE LIST — Single source of truth
# Semua window PENDEK agar warmup minimal → NaN minimal
# SEMUA fitur dijamin fillna(0) di create_features()
# =========================
FEATURES = [
    # Nilai saat ini
    'throughput_bps',
    'is_burst',
    'ratio_to_threshold',

    # Streak counter (tidak pernah NaN — loop iteratif)
    'consecutive_steady_sec',
    'burst_streak',

    # Rolling — min_periods=1 → tidak pernah NaN
    'roll_mean_5s',
    'roll_mean_10s',
    'roll_mean_30s',
    'roll_std_5s',
    'roll_std_10s',
    'roll_max_5s',
    'roll_max_10s',

    # EMA — ewm adjust=False → tidak pernah NaN
    'ema_5s',
    'ema_10s',
    'ema_30s',
    'ema_cross_5_30',   # ema_5s - ema_30s (positif = naik)

    # Lag — fillna(0)
    'lag_1',
    'lag_3',
    'lag_5',
    'lag_10',

    # Slope — fillna(0)
    'slope_1s',
    'slope_3s',
    'slope_5s',
    'slope_10s',

    # Acceleration — window pendek 3 → warmup hanya 8 row, fillna(0)
    'accel_3s',

    # Pct change — fillna(0)
    'pct_change_3s',
    'pct_change_5s',
]


# =========================
# 1. FETCH DATA
# =========================
def get_data(hours=2):
    """
    Ambil data traffic dari DB.
    hours=8 untuk Training, hours=2 untuk Prediksi
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
        print(f"\n❌ DB Error: {e}")
        return None

    if df.empty:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    # Resample ke 1s, ffill untuk gap kecil
    df = df.resample('1s').max().ffill().bfill().fillna(0)

    # Clip nilai negatif (artifact dari delta counter)
    df[TARGET] = df[TARGET].clip(lower=0)

    return df


# =========================
# 2. FEATURE ENGINEERING
# ATURAN WAJIB: SEMUA fitur harus fillna(0) setelah dihitung.
# TIDAK BOLEH ada NaN masuk ke model — baik saat training maupun prediksi.
# =========================
def create_features(df, for_training=False):
    data = df.copy()
    T = TARGET

    # ── 1. Label & Ratio ─────────────────────────────────────────────────
    data['is_burst']           = (data[T] > BURST_THRESHOLD_BPS).astype(float)
    data['ratio_to_threshold'] = (data[T] / BURST_THRESHOLD_BPS).clip(upper=5.0)

    # ── 2. Streak counters (loop iteratif, tidak pernah NaN) ─────────────
    steady, burst_s = [], []
    cs = cb = 0
    for v in data['is_burst']:
        if v == 1:
            cs = 0
            cb += 1
        else:
            cs += 1
            cb = 0
        steady.append(float(cs))
        burst_s.append(float(cb))
    data['consecutive_steady_sec'] = steady
    data['burst_streak']           = burst_s

    # ── 3. Rolling — min_periods=1 → TIDAK PERNAH NaN ───────────────────
    for w in [5, 10, 30]:
        data[f'roll_mean_{w}s'] = data[T].rolling(w, min_periods=1).mean()
    for w in [5, 10]:
        data[f'roll_std_{w}s']  = data[T].rolling(w, min_periods=2).std().fillna(0)
        data[f'roll_max_{w}s']  = data[T].rolling(w, min_periods=1).max()

    # ── 4. EMA — adjust=False → TIDAK PERNAH NaN ────────────────────────
    data['ema_5s']        = data[T].ewm(span=5,  adjust=False).mean()
    data['ema_10s']       = data[T].ewm(span=10, adjust=False).mean()
    data['ema_30s']       = data[T].ewm(span=30, adjust=False).mean()
    data['ema_cross_5_30']= data['ema_5s'] - data['ema_30s']

    # ── 5. Lag Features — fillna(0) ──────────────────────────────────────
    # fillna(0) → row warmup dianggap "tidak ada history" (0 bps)
    # Model belajar bahwa 0 = tidak ada data sebelumnya
    # Konsisten antara training dan prediksi
    for lag in [1, 3, 5, 10]:
        data[f'lag_{lag}'] = data[T].shift(lag).fillna(0)

    # ── 6. Slope Features — fillna(0) ────────────────────────────────────
    data['slope_1s']  = data[T].diff(1).fillna(0)
    data['slope_3s']  = (data[T].diff(3)  / 3 ).fillna(0)
    data['slope_5s']  = (data[T].diff(5)  / 5 ).fillna(0)
    data['slope_10s'] = (data[T].diff(10) / 10).fillna(0)

    # Acceleration: seberapa cepat slope berubah
    # Window 3 → warmup hanya 3+5=8 row (jauh lebih kecil dari accel_5s=10)
    data['accel_3s'] = data['slope_5s'].diff(3).fillna(0)

    # ── 7. Pct Change — fillna(0) ────────────────────────────────────────
    data['pct_change_3s'] = (
        data[T].pct_change(periods=3)
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )
    data['pct_change_5s'] = (
        data[T].pct_change(periods=5)
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )

    # ── 8. Target ────────────────────────────────────────────────────────
    if for_training:
        data['target_future'] = data[T].shift(-PREDICTION_HORIZON_SEC)
        # Hanya drop baris ujung (target NaN karena shift)
        data = data.dropna(subset=['target_future'])

    # ── FINAL SAFETY NET ─────────────────────────────────────────────────
    # Pastikan benar-benar tidak ada NaN di FEATURES (double-check)
    for col in FEATURES:
        if col in data.columns:
            data[col] = data[col].fillna(0)

    return data


# =========================
# 3. TRAINING ROUTINE
# =========================
def train_model():
    global TRAINED_MODEL, LAST_TRAIN_TIME, FEATURE_MEDIANS

    print("\n🧠 Starting Model Training...")
    start_t = time.time()

    df = get_data(hours=8)
    if df is None or len(df) < 500:
        print("⚠️  Not enough data to train.")
        return

    df_train = create_features(df, for_training=True)

    if len(df_train) < 200:
        print("⚠️  Not enough rows after feature engineering.")
        return

    X = df_train[FEATURES]
    y = df_train['target_future']

    # Simpan median setiap fitur sebagai fallback NaN saat prediksi
    FEATURE_MEDIANS = X.median().to_dict()

    # ── Distribusi ──────────────────────────────────────────────────────
    burst_rows  = (y > BURST_THRESHOLD_BPS).sum()
    normal_rows = (y <= BURST_THRESHOLD_BPS).sum()
    ratio       = normal_rows / max(burst_rows, 1)
    print(f"   Rows: {len(y)} | Burst: {burst_rows} ({burst_rows/len(y)*100:.1f}%) "
          f"| Normal: {normal_rows} ({normal_rows/len(y)*100:.1f}%) | Ratio: {ratio:.1f}x")

    # ── NaN check wajib ─────────────────────────────────────────────────
    nan_total = X.isna().sum().sum()
    if nan_total > 0:
        bad_cols = X.isna().sum()[X.isna().sum() > 0]
        print(f"   ⚠️  WARNING: {nan_total} NaN ditemukan:\n{bad_cols}")
    else:
        print("   ✅ Zero NaN in training features — OK")

    # ── Sample Weights ───────────────────────────────────────────────────
    # - Burst row   : bobot 5x (minoritas)
    # - Normal row  : bobot 1x
    # - Transisi ±10 detik: bobot 10x (momen paling kritis)
    weights        = np.where(y > BURST_THRESHOLD_BPS, 5.0, 1.0).astype(float)
    is_burst_arr   = (y > BURST_THRESHOLD_BPS).astype(int).values
    for i in range(1, len(is_burst_arr)):
        if is_burst_arr[i] != is_burst_arr[i - 1]:
            lo = max(0, i - 10)
            hi = min(len(weights), i + 10)
            weights[lo:hi] = 10.0

    # ── Model ────────────────────────────────────────────────────────────
    model = xgb.XGBRegressor(
        n_estimators      = 700,
        max_depth         = 4,       # Dangkal → generalisasi baik, kurangi overfit noise
        learning_rate     = 0.03,
        subsample         = 0.85,
        colsample_bytree  = 0.85,
        colsample_bylevel = 0.85,
        min_child_weight  = 2,
        gamma             = 0.5,
        reg_alpha         = 0.05,
        reg_lambda        = 1.0,
        n_jobs            = -1,
        random_state      = 42,
        tree_method       = 'hist',
    )
    model.fit(X, y, sample_weight=weights)

    TRAINED_MODEL   = model
    LAST_TRAIN_TIME = time.time()
    duration        = time.time() - start_t

    # ── In-sample accuracy ───────────────────────────────────────────────
    y_pred_train = model.predict(X)
    mae          = np.mean(np.abs(y_pred_train - y))

    pred_burst   = y_pred_train > BURST_THRESHOLD_BPS
    actual_burst = y.values > BURST_THRESHOLD_BPS
    tp = ( pred_burst  &  actual_burst).sum()
    fp = ( pred_burst  & ~actual_burst).sum()
    fn = (~pred_burst  &  actual_burst).sum()
    precision = tp / max(tp + fp, 1)
    recall    = tp / max(tp + fn, 1)
    f1        = 2 * precision * recall / max(precision + recall, 1e-9)

    print(f"✅ Training Done in {duration:.2f}s")
    print(f"   MAE: {mae/1e6:.3f} Mbps | Precision: {precision:.3f} | Recall: {recall:.3f} | F1: {f1:.3f}")

    # ── Feature Importance ────────────────────────────────────────────────
    importance = sorted(zip(FEATURES, model.feature_importances_), key=lambda x: x[1], reverse=True)
    print("   Top 10 Features:")
    for fname, fimp in importance[:10]:
        bar = '█' * int(fimp * 300)
        print(f"   {fname:<25} {fimp:.4f}  {bar}")
    print(f"   Next training in {TRAIN_INTERVAL_SEC // 60} mins.")


# =========================
# 4. PREDICTION ROUTINE
# =========================
def run_prediction():
    global TRAINED_MODEL

    if TRAINED_MODEL is None:
        return

    df = get_data(hours=2)
    if df is None or len(df) < 15:
        return

    df_feat = create_features(df, for_training=False)
    if df_feat.empty:
        return

    last_row = df_feat.iloc[[-1]]

    # Cek semua fitur tersedia
    missing = [f for f in FEATURES if f not in last_row.columns]
    if missing:
        print(f"\n⚠️  Missing features: {missing}")
        return

    X_pred = last_row[FEATURES].copy()

    # SAFETY: Ganti sisa NaN dengan median dari training
    # Seharusnya tidak ada (sudah fillna di create_features), tapi jaga-jaga
    nan_cols = [c for c in FEATURES if X_pred[c].isna().any()]
    for col in nan_cols:
        fallback = FEATURE_MEDIANS.get(col, 0)
        X_pred[col] = X_pred[col].fillna(fallback)
        print(f"\n⚠️  NaN fallback applied: {col} → {fallback:.0f}")

    # Prediksi
    pred_val = float(TRAINED_MODEL.predict(X_pred)[0])
    pred_val = max(0.0, pred_val)

    ts_now    = pd.Timestamp.now()
    ts_future = ts_now + timedelta(seconds=PREDICTION_HORIZON_SEC)

    # Log ke layar
    actual_bps = float(last_row['throughput_bps'].values[0])
    steady_sec = float(last_row['consecutive_steady_sec'].values[0])
    burst_sec  = float(last_row['burst_streak'].values[0])
    slope_5    = float(last_row['slope_5s'].values[0])
    ema_cross  = float(last_row['ema_cross_5_30'].values[0])

    status = "⚠️  DANGER" if pred_val > BURST_THRESHOLD_BPS else "✅ SAFE  "
    trend  = ("↑↑" if slope_5 >  5_000_000 else
              "↑"  if slope_5 >    500_000 else
              "↓↓" if slope_5 < -5_000_000 else
              "↓"  if slope_5 <   -500_000 else "→")

    print(
        f"[{ts_now.strftime('%H:%M:%S')}] "
        f"Now:{actual_bps/1e6:6.1f}M {trend} "
        f"EMA✕:{ema_cross/1e6:+5.1f}M | "
        f"Stdy:{steady_sec:4.0f}s Brst:{burst_sec:4.0f}s | "
        f"Pred(+10s):{pred_val/1e6:7.2f}M [{status}]",
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
    print("🚀 Forecast Monitor — Zero-NaN High Accuracy Mode")
    print(f"   Threshold : {BURST_THRESHOLD_BPS/1e6:.0f} Mbps")
    print(f"   Horizon   : +{PREDICTION_HORIZON_SEC}s")
    print(f"   Features  : {len(FEATURES)}")
    print(f"   Retrain   : every {TRAIN_INTERVAL_SEC // 60} min")
    print(f"   NaN policy: fillna(0) + median fallback ✅")

    try:
        while True:
            loop_start = time.time()

            if TRAINED_MODEL is None or (time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC):
                train_model()

            run_prediction()

            elapsed    = time.time() - loop_start
            sleep_time = max(0, 5.0 - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")