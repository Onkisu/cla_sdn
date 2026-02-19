#!/usr/bin/env python3
"""
forecast_2.py - High Accuracy Traffic Burst Predictor
======================================================
ROOT CAUSE PREDICT=0 DI TENGAH BURST (FINAL FIX):
  OVS counter bytes_tx RESET ke 0 saat controller melakukan reroute
  (delete all flows → install new flows → counter restart dari 0).
  Ini menyebabkan 1-3 detik nilai throughput=0 di tengah burst.
  
  Efek berantai ke training:
    - target_future = throughput.shift(-10)
    - Baris "burst sekarang" yang punya target_future=0 (artifact reset)
      → model belajar: "traffic tinggi sekarang → 0 nanti" → PREDICT 0!
  
  3 lapisan fix:
    FIX A: get_data() → tutup zero-holes di tengah burst dengan ffill
    FIX B: train_model() → filter artifact rows (burst_now + target≈0)
    FIX C: run_prediction() → kalau actual burst, floor prediction ke threshold
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
TRAIN_INTERVAL_SEC     = 1800

# Globals
TRAINED_MODEL   = None
LAST_TRAIN_TIME = 0
FEATURE_MEDIANS = {}

engine = create_engine(DB_URI)

FEATURES = [
    'throughput_bps',
    'is_burst',
    'ratio_to_threshold',
    'consecutive_steady_sec',
    'burst_streak',
    'roll_mean_5s',
    'roll_mean_10s',
    'roll_mean_30s',
    'roll_std_5s',
    'roll_std_10s',
    'roll_max_5s',
    'roll_max_10s',
    'ema_5s',
    'ema_10s',
    'ema_30s',
    'ema_cross_5_30',
    'lag_1',
    'lag_3',
    'lag_5',
    'lag_10',
    'slope_1s',
    'slope_3s',
    'slope_5s',
    'slope_10s',
    'accel_3s',
    'pct_change_3s',
    'pct_change_5s',
]


# =========================
# 1. FETCH DATA
# =========================
def get_data(hours=2):
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
    df = df.resample('1s').max()

    # ── FIX A: Tutup zero-holes akibat OVS counter reset ─────────────────
    # Logika: nilai 0 yang muncul tiba-tiba saat traffic sebelumnya
    # sedang burst (>threshold) adalah PASTI artifact, bukan traffic real.
    # Ganti dengan nilai sebelumnya (forward-fill terbatas 5 detik).
    #
    # Kenapa 5 detik? Reroute di ksp_controller_2.py:
    #   FLOW_DELETE_WAIT_SEC = 2.0 + TRAFFIC_SETTLE_WAIT_SEC = 2.0 → max 4 detik gap
    #   Pakai 5 detik sebagai buffer aman.
    T = TARGET
    for _ in range(5):   # iterasi max 5x untuk tutup gap sampai 5 detik
        prev = df[T].shift(1)
        # Zero hole: nilai sekarang sangat kecil (<1 Mbps) tapi sebelumnya burst
        mask_artifact = (df[T] < 1_000_000) & (prev > BURST_THRESHOLD_BPS)
        if mask_artifact.sum() == 0:
            break
        df.loc[mask_artifact, T] = prev[mask_artifact]

    df = df.ffill().bfill().fillna(0)
    df[T] = df[T].clip(lower=0)

    return df


# =========================
# 2. FEATURE ENGINEERING
# =========================
def create_features(df, for_training=False):
    data = df.copy()
    T = TARGET

    data['is_burst']           = (data[T] > BURST_THRESHOLD_BPS).astype(float)
    data['ratio_to_threshold'] = (data[T] / BURST_THRESHOLD_BPS).clip(upper=5.0)

    # Streak counters
    steady, burst_s = [], []
    cs = cb = 0
    for v in data['is_burst']:
        if v == 1: cs = 0;  cb += 1
        else:      cs += 1; cb = 0
        steady.append(float(cs))
        burst_s.append(float(cb))
    data['consecutive_steady_sec'] = steady
    data['burst_streak']           = burst_s

    # Rolling — min_periods=1 → tidak pernah NaN
    for w in [5, 10, 30]:
        data[f'roll_mean_{w}s'] = data[T].rolling(w, min_periods=1).mean()
    for w in [5, 10]:
        data[f'roll_std_{w}s'] = data[T].rolling(w, min_periods=2).std().fillna(0)
        data[f'roll_max_{w}s'] = data[T].rolling(w, min_periods=1).max()

    # EMA — tidak pernah NaN
    data['ema_5s']         = data[T].ewm(span=5,  adjust=False).mean()
    data['ema_10s']        = data[T].ewm(span=10, adjust=False).mean()
    data['ema_30s']        = data[T].ewm(span=30, adjust=False).mean()
    data['ema_cross_5_30'] = data['ema_5s'] - data['ema_30s']

    # Lag — fillna(0)
    for lag in [1, 3, 5, 10]:
        data[f'lag_{lag}'] = data[T].shift(lag).fillna(0)

    # Slope — fillna(0)
    data['slope_1s']  = data[T].diff(1).fillna(0)
    data['slope_3s']  = (data[T].diff(3)  / 3 ).fillna(0)
    data['slope_5s']  = (data[T].diff(5)  / 5 ).fillna(0)
    data['slope_10s'] = (data[T].diff(10) / 10).fillna(0)
    data['accel_3s']  = data['slope_5s'].diff(3).fillna(0)

    # Pct change — fillna(0)
    data['pct_change_3s'] = data[T].pct_change(3).replace([np.inf, -np.inf], 0).fillna(0)
    data['pct_change_5s'] = data[T].pct_change(5).replace([np.inf, -np.inf], 0).fillna(0)

    if for_training:
        data['target_future'] = data[T].shift(-PREDICTION_HORIZON_SEC)
        data = data.dropna(subset=['target_future'])

    # Final safety — zero NaN guarantee
    for col in FEATURES:
        if col in data.columns:
            data[col] = data[col].fillna(0)

    return data


# =========================
# 3. TRAINING
# =========================
def train_model():
    global TRAINED_MODEL, LAST_TRAIN_TIME, FEATURE_MEDIANS

    print("\n🧠 Training model...")
    t0 = time.time()

    df = get_data(hours=8)
    if df is None or len(df) < 500:
        print("⚠️  Not enough data.")
        return

    df_train = create_features(df, for_training=True)
    if len(df_train) < 200:
        print("⚠️  Not enough rows.")
        return

    # ── FIX B: Hapus artifact rows sebelum training ───────────────────────
    # Baris yang masih lolos: "is_burst=1 sekarang TAPI target_future < 1 Mbps"
    # Ini terjadi karena ada zero-hole di window target (10 detik ke depan)
    # yang tidak tertutup oleh FIX A (mis. zero-hole di akhir window)
    is_burst_now      = df_train[TARGET] > BURST_THRESHOLD_BPS
    target_near_zero  = df_train['target_future'] < 1_000_000   # < 1 Mbps
    artifact_mask     = is_burst_now & target_near_zero
    n_artifact        = artifact_mask.sum()

    if n_artifact > 0:
        print(f"   🧹 Removing {n_artifact} artifact rows (burst→zero contradictions)")
        df_train = df_train[~artifact_mask]

    X = df_train[FEATURES]
    y = df_train['target_future']

    FEATURE_MEDIANS = X.median().to_dict()

    burst_rows  = (y > BURST_THRESHOLD_BPS).sum()
    normal_rows = (y <= BURST_THRESHOLD_BPS).sum()
    print(f"   Rows: {len(y)} | Burst: {burst_rows} ({burst_rows/len(y)*100:.1f}%) "
          f"| Normal: {normal_rows} ({normal_rows/len(y)*100:.1f}%)")

    nan_total = X.isna().sum().sum()
    print(f"   NaN in features: {nan_total} {'✅' if nan_total == 0 else '⚠️'}")

    # Sample weights
    weights      = np.where(y > BURST_THRESHOLD_BPS, 5.0, 1.0).astype(float)
    is_burst_arr = (y > BURST_THRESHOLD_BPS).astype(int).values
    for i in range(1, len(is_burst_arr)):
        if is_burst_arr[i] != is_burst_arr[i - 1]:
            lo = max(0, i - 10)
            hi = min(len(weights), i + 10)
            weights[lo:hi] = 10.0

    model = xgb.XGBRegressor(
        n_estimators      = 700,
        max_depth         = 4,
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
    duration        = time.time() - t0

    # Accuracy report
    y_pred_train = model.predict(X)
    mae          = np.mean(np.abs(y_pred_train - y))
    pred_burst   = y_pred_train > BURST_THRESHOLD_BPS
    actual_burst = y.values > BURST_THRESHOLD_BPS
    tp = ( pred_burst &  actual_burst).sum()
    fp = ( pred_burst & ~actual_burst).sum()
    fn = (~pred_burst &  actual_burst).sum()
    precision = tp / max(tp + fp, 1)
    recall    = tp / max(tp + fn, 1)
    f1        = 2 * precision * recall / max(precision + recall, 1e-9)

    print(f"✅ Done in {duration:.1f}s | MAE: {mae/1e6:.2f} Mbps | "
          f"P: {precision:.3f} R: {recall:.3f} F1: {f1:.3f}")

    importance = sorted(zip(FEATURES, model.feature_importances_), key=lambda x: x[1], reverse=True)
    print("   Top 8 Features:")
    for fname, fimp in importance[:8]:
        print(f"   {fname:<25} {fimp:.4f}  {'█' * int(fimp*300)}")
    print(f"   Next retrain in {TRAIN_INTERVAL_SEC // 60} min.")


# =========================
# 4. PREDICTION
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

    missing = [f for f in FEATURES if f not in last_row.columns]
    if missing:
        print(f"\n⚠️  Missing: {missing}")
        return

    X_pred = last_row[FEATURES].copy()

    # NaN fallback
    for col in FEATURES:
        if X_pred[col].isna().any():
            X_pred[col] = X_pred[col].fillna(FEATURE_MEDIANS.get(col, 0))

    pred_val   = float(TRAINED_MODEL.predict(X_pred)[0])
    pred_val   = max(0.0, pred_val)

    actual_bps = float(last_row['throughput_bps'].values[0])
    is_burst_now = actual_bps > BURST_THRESHOLD_BPS

    # ── FIX C: Kalau sedang burst, floor prediksi ke threshold ───────────
    # Jika traffic aktual sedang burst DAN model predict < threshold
    # itu hampir pasti artifact sisa training → override dengan nilai aman.
    # Hanya apply kalau burst sudah berlangsung >5 detik (bukan transisi awal).
    burst_sec = float(last_row['burst_streak'].values[0])
    if is_burst_now and burst_sec > 5 and pred_val < BURST_THRESHOLD_BPS:
        # Gunakan rolling mean sebagai estimasi lebih baik daripada 0
        pred_val = float(last_row['roll_mean_10s'].values[0])
        pred_val = max(pred_val, BURST_THRESHOLD_BPS)  # minimal threshold

    ts_now    = pd.Timestamp.now()
    ts_future = ts_now + timedelta(seconds=PREDICTION_HORIZON_SEC)

    steady_sec = float(last_row['consecutive_steady_sec'].values[0])
    slope_5    = float(last_row['slope_5s'].values[0])
    ema_cross  = float(last_row['ema_cross_5_30'].values[0])
    status     = "⚠️  DANGER" if pred_val > BURST_THRESHOLD_BPS else "✅ SAFE  "
    trend      = ("↑↑" if slope_5 >  5_000_000 else
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

    try:
        pd.DataFrame([{
            'ts_created': ts_now,
            'ts':         ts_future,
            'y_pred':     pred_val
        }]).to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
    except Exception:
        pass


# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print("🚀 Forecast Monitor — Zero-Hole Fix Mode")
    print(f"   Threshold  : {BURST_THRESHOLD_BPS/1e6:.0f} Mbps")
    print(f"   Horizon    : +{PREDICTION_HORIZON_SEC}s")
    print(f"   Features   : {len(FEATURES)}")
    print(f"   Retrain    : every {TRAIN_INTERVAL_SEC // 60} min")
    print(f"   Zero-hole  : ffill burst gaps (max 5s)")
    print(f"   Artifact   : filter burst→zero training rows")
    print(f"   Floor pred : roll_mean if burst>5s & pred<threshold")

    try:
        while True:
            loop_start = time.time()

            if TRAINED_MODEL is None or (time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC):
                train_model()

            run_prediction()

            sleep_time = max(0, 5.0 - (time.time() - loop_start))
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Stopped.")