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
BURST_THRESHOLD_BPS = 120000
PREDICTION_HORIZON_SEC = 10

TRAIN_INTERVAL_SEC = 1800        # 30 menit
MIN_HISTORY_SEC = 3600           # ≥ 1 jam

TRAINED_MODEL = None
LAST_TRAIN_TIME = 0

engine = create_engine(DB_URI)

# =========================
# FETCH DATA
# =========================
def get_data(hours=1):
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
        SELECT detik AS ts, total_bytes * 8 AS throughput_bps
        FROM x
        WHERE dpid = 5
        ORDER BY ts ASC
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')
    df = df.resample('1s').max().ffill().fillna(0)
    return df

# =========================
# FEATURE ENGINEERING (PURE LEARNING)
# =========================
def create_features(df, for_training=False):
    d = df.copy()

    # burst state
    d['is_burst'] = (d[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    # time since last burst
    grp = d['is_burst'].cumsum()
    d['consecutive_steady_sec'] = d.groupby(grp).cumcount()
    d.loc[d['is_burst'] == 1, 'consecutive_steady_sec'] = 0

    # non-linear temporal encoding (NO cycle knowledge)
    d['steady_norm'] = d['consecutive_steady_sec'] / (d['consecutive_steady_sec'].max() + 1)
    d['steady_sq'] = d['consecutive_steady_sec'] ** 2

    # burst memory
    d['burst_rate_5m'] = d['is_burst'].rolling(300).mean()
    d['burst_rate_15m'] = d['is_burst'].rolling(900).mean()

    # rolling stats
    d['roll_mean_30s'] = d[TARGET].rolling(30).mean()
    d['roll_std_30s'] = d[TARGET].rolling(30).std()

    # lag
    for l in [1, 5, 10]:
        d[f'lag_{l}'] = d[TARGET].shift(l)

    if for_training:
        # EVENT TARGET (BUKAN REGRESSION)
        d['burst_future'] = (
            d[TARGET].shift(-PREDICTION_HORIZON_SEC) > BURST_THRESHOLD_BPS
        ).astype(int)
        d = d.dropna()
    else:
        d = d.dropna(subset=['roll_mean_30s', 'lag_10'])

    return d

# =========================
# TRAINING
# =========================
def train_model():
    global TRAINED_MODEL, LAST_TRAIN_TIME

    print("\n🧠 Training Model...")
    start_t = time.time()

    df = get_data(hours=6)
    if df is None:
        print("⚠️ No data.")
        return

    history_sec = (df.index.max() - df.index.min()).total_seconds()
    if history_sec < MIN_HISTORY_SEC:
        print("⏳ Waiting for ≥ 1 hour history...")
        return

    df_train = create_features(df, for_training=True)

    features = [
        'consecutive_steady_sec',
        'steady_norm',
        'steady_sq',
        'burst_rate_5m',
        'burst_rate_15m',
        'roll_mean_30s',
        'roll_std_30s',
        'lag_1', 'lag_5'
    ]

    X = df_train[features]
    y = df_train['burst_future']

    model = xgb.XGBClassifier(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric='logloss',
        random_state=42,
        n_jobs=-1
    )

    model.fit(X, y)

    TRAINED_MODEL = model
    LAST_TRAIN_TIME = time.time()

    print(f"✅ Training done in {time.time() - start_t:.1f}s")

# =========================
# PREDICTION
# =========================
def run_prediction():
    global TRAINED_MODEL
    if TRAINED_MODEL is None:
        return

    df = get_data(hours=1.5)
    if df is None:
        return

    df_feat = create_features(df, for_training=False)
    if df_feat.empty:
        return

    last = df_feat.iloc[[-1]]

    features = [
        'consecutive_steady_sec',
        'steady_norm',
        'steady_sq',
        'burst_rate_5m',
        'burst_rate_15m',
        'roll_mean_30s',
        'roll_std_30s',
        'lag_1', 'lag_5'
    ]

    prob = TRAINED_MODEL.predict_proba(last[features])[0][1]

    ts_now = pd.Timestamp.now()
    ts_future = ts_now + timedelta(seconds=PREDICTION_HORIZON_SEC)

    steady = last['consecutive_steady_sec'].values[0]
    status = "⚠️ DANGER" if prob > 0.7 else "SAFE"

    print(
        f"[{ts_now.strftime('%H:%M:%S')}] "
        f"Steady={steady:.0f}s | "
        f"P(burst+10s)={prob:.2f} [{status}]",
        end='\r'
    )

    try:
        pd.DataFrame([{
            'ts_created': ts_now,
            'ts': ts_future,
            'y_pred': prob
        }]).to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
    except:
        pass

# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print("🚀 Forecast Monitor (PURE LEARNING, FIXED)")

    try:
        while True:
            t0 = time.time()

            if TRAINED_MODEL is None or (time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC):
                train_model()

            run_prediction()

            time.sleep(max(0, 5 - (time.time() - t0)))

    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
