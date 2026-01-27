#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta
import time

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"
TABLE_FLOW = "traffic.flow_stats_"
TABLE_FORECAST = "forecast_1h"

TARGET = "throughput_bps"
BURST_THRESHOLD_BPS = 250_000
PRED_HORIZON_SEC = 10

# learning constraints (NO hardcoded cycle)
MIN_HISTORY_SEC = 3600      # ≥ 1 jam data
TRAIN_INTERVAL = 900        # retrain tiap 15 menit

engine = create_engine(DB_URI)

# =========================
# FETCH DATA
# =========================
def fetch_data(hours=6):
    query = f"""
    WITH x AS (
        SELECT
            date_trunc('second', timestamp) AS ts,
            dpid,
            MAX(bytes_tx) AS bytes
        FROM traffic.flow_stats_
        GROUP BY ts, dpid
    )
    SELECT ts, bytes * 8 AS throughput_bps
    FROM x
    WHERE dpid = 5
      AND ts >= NOW() - INTERVAL '{hours} hour'
    ORDER BY ts;
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
def create_features(df):
    d = df.copy()

    # burst state
    d['is_burst'] = (d[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    # time since last burst (key signal)
    grp = d['is_burst'].cumsum()
    d['steady_sec'] = d.groupby(grp).cumcount()
    d.loc[d['is_burst'] == 1, 'steady_sec'] = 0

    # learned temporal context (NO PERIOD KNOWLEDGE)
    d['steady_norm'] = d['steady_sec'] / (d['steady_sec'].max() + 1)
    d['steady_sq'] = d['steady_sec'] ** 2

    # burst frequency memory
    d['burst_rate_5m'] = d['is_burst'].rolling(300).mean()
    d['burst_rate_15m'] = d['is_burst'].rolling(900).mean()

    # short-term dynamics
    d['roll_mean_30'] = d[TARGET].rolling(30).mean()
    d['roll_std_30'] = d[TARGET].rolling(30).std()

    # lag context
    for l in [1, 5, 10]:
        d[f'lag_{l}'] = d[TARGET].shift(l)

    # target: burst event in future
    d['burst_future'] = (
        d[TARGET].shift(-PRED_HORIZON_SEC) > BURST_THRESHOLD_BPS
    ).astype(int)

    d = d.dropna()
    return d

# =========================
# TRAIN GUARD
# =========================
def has_enough_history(df):
    return (df.index.max() - df.index.min()).total_seconds() >= MIN_HISTORY_SEC

# =========================
# TRAIN MODEL
# =========================
def train_model(df):
    features = [
        'steady_sec',
        'steady_norm',
        'steady_sq',
        'burst_rate_5m',
        'burst_rate_15m',
        'roll_mean_30',
        'roll_std_30',
        'lag_1',
        'lag_5'
    ]

    X = df[features]
    y = df['burst_future']

    model = xgb.XGBClassifier(
        n_estimators=450,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric='logloss',
        random_state=42,
        n_jobs=-1
    )
    model.fit(X, y)
    return model

# =========================
# MAIN LOOP
# =========================
def main():
    print("🚀 Burst Forecast (PURE LEARNING) Started")

    model = None
    last_train = 0

    while True:
        t0 = time.time()

        df_raw = fetch_data()
        if df_raw is None:
            time.sleep(5)
            continue

        df = create_features(df_raw)
        if df.empty:
            time.sleep(5)
            continue

        # TRAIN (NO CHEATING)
        if model is None or (time.time() - last_train > TRAIN_INTERVAL):
            if has_enough_history(df):
                print("\n🧠 Training model (data-driven)")
                model = train_model(df)
                last_train = time.time()
            else:
                print("⏳ Waiting for ≥ 1 hour data...")
                time.sleep(5)
                continue

        # PREDICT
        features = [
            'steady_sec',
            'steady_norm',
            'steady_sq',
            'burst_rate_5m',
            'burst_rate_15m',
            'roll_mean_30',
            'roll_std_30',
            'lag_1',
            'lag_5'
        ]

        last = df.iloc[[-1]][features]
        ts = df.index[-1]

        prob = model.predict_proba(last)[0][1]

        status = "SAFE"
        if prob > 0.7:
            status = "⚠️ BURST IMMINENT"

        print(
            f"[{ts}] Steady={int(df.iloc[-1]['steady_sec'])}s | "
            f"P(burst+{PRED_HORIZON_SEC}s)={prob:.2f} → {status}",
            end="\r"
        )

        # SAVE RESULT
        pd.DataFrame({
            'ts': [ts + timedelta(seconds=PRED_HORIZON_SEC)],
            'burst_prob': [prob]
        }).to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)

        time.sleep(max(0, 5 - (time.time() - t0)))

if __name__ == "__main__":
    main()
