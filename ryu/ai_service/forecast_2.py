#!/usr/bin/env python3
"""
forecast_2_remake.py
====================

GOAL:
Forecast sesuai dengan traffic generator simulation & bursty v2,
dengan akurasi setara atau lebih baik dari versi 1.

KEY FIXES:
• Proper throughput calculation from cumulative counter
• Remove counter reset artifacts safely
• Preserve burst continuity
• Train model sesuai karakter traffic generator bursty v2
• Stabil prediction saat plateau burst
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta
import time
import warnings

warnings.simplefilter("ignore")

# =========================
# CONFIG
# =========================

DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"

TABLE_FORECAST = "forecast_1h"

TARGET = "throughput_bps"

BURST_THRESHOLD = 40_000_000

HORIZON = 10

TRAIN_INTERVAL = 1800

engine = create_engine(DB_URI)

MODEL = None
LAST_TRAIN = 0

FEATURE_MEDIAN = {}

# =========================
# FETCH DATA (FIXED CORE)
# =========================

def get_data(hours=4):

    query = f"""
    WITH x AS (
        SELECT
            date_trunc('second', timestamp) ts,
            dpid,
            MAX(bytes_tx) bytes
        FROM traffic.flow_stats_
        WHERE timestamp >= NOW() - interval '{hours} hour'
        GROUP BY ts, dpid
    )
    SELECT ts, bytes
    FROM x
    WHERE dpid = 5
    ORDER BY ts
    """

    df = pd.read_sql(query, engine)

    if len(df) < 10:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    df = df.resample("1s").max()

    # =============================
    # CRITICAL FIX: USE DELTA BYTES
    # =============================

    df['delta_bytes'] = df['bytes'].diff()

    # detect counter reset
    reset = df['delta_bytes'] < 0

    df.loc[reset, 'delta_bytes'] = 0

    df['throughput_bps'] = df['delta_bytes'] * 8

    # remove impossible spikes
    df['throughput_bps'] = df['throughput_bps'].clip(0, 1_000_000_000)

    df[TARGET] = df['throughput_bps']

    df = df.ffill().fillna(0)

    return df


# =========================
# FEATURE ENGINEERING
# =========================

FEATURES = [

    TARGET,

    "roll_mean_5",
    "roll_mean_10",

    "roll_max_5",
    "roll_max_10",

    "ema_5",
    "ema_10",
    "ema_30",

    "lag_1",
    "lag_3",
    "lag_5",

    "slope_3",
    "slope_5",

    "burst_flag",
    "burst_streak"
]


def create_features(df, training=False):

    data = df.copy()

    T = TARGET

    data["burst_flag"] = (data[T] > BURST_THRESHOLD).astype(float)

    # burst streak
    streak = 0
    out = []

    for v in data["burst_flag"]:

        if v:
            streak += 1
        else:
            streak = 0

        out.append(streak)

    data["burst_streak"] = out

    data["roll_mean_5"] = data[T].rolling(5, min_periods=1).mean()
    data["roll_mean_10"] = data[T].rolling(10, min_periods=1).mean()

    data["roll_max_5"] = data[T].rolling(5, min_periods=1).max()
    data["roll_max_10"] = data[T].rolling(10, min_periods=1).max()

    data["ema_5"] = data[T].ewm(span=5).mean()
    data["ema_10"] = data[T].ewm(span=10).mean()
    data["ema_30"] = data[T].ewm(span=30).mean()

    data["lag_1"] = data[T].shift(1).fillna(0)
    data["lag_3"] = data[T].shift(3).fillna(0)
    data["lag_5"] = data[T].shift(5).fillna(0)

    data["slope_3"] = data[T].diff(3).fillna(0)
    data["slope_5"] = data[T].diff(5).fillna(0)

    if training:

        # smoothed target → critical for bursty v2
        data["target"] = (
            data[T]
            .shift(-HORIZON)
            .rolling(3, min_periods=1)
            .mean()
        )

        data = data.dropna()

    return data


# =========================
# TRAIN MODEL
# =========================

def train():

    global MODEL, LAST_TRAIN, FEATURE_MEDIAN

    df = get_data(8)

    if df is None:
        return

    df = create_features(df, True)

    X = df[FEATURES]
    y = df["target"]

    FEATURE_MEDIAN = X.median().to_dict()

    weights = np.where(y > BURST_THRESHOLD, 6, 1)

    MODEL = xgb.XGBRegressor(

        n_estimators=500,

        max_depth=5,

        learning_rate=0.05,

        subsample=0.9,

        colsample_bytree=0.9,

        tree_method="hist",

        n_jobs=-1
    )

    MODEL.fit(X, y, sample_weight=weights)

    LAST_TRAIN = time.time()

    print("MODEL TRAINED")


# =========================
# PREDICT
# =========================

def predict():

    df = get_data(2)

    if df is None:
        return

    df = create_features(df)

    row = df.iloc[-1]

    X = row[FEATURES].fillna(pd.Series(FEATURE_MEDIAN))

    pred = float(MODEL.predict(X.values.reshape(1, -1))[0])

    actual = row[TARGET]

    burst = row["burst_streak"]

    # enforce burst continuity
    if burst > 5 and pred < BURST_THRESHOLD:

        pred = max(
            pred,
            row["roll_mean_10"],
            BURST_THRESHOLD
        )

    print(
        f"Now {actual/1e6:.1f} Mbps → Pred {pred/1e6:.1f} Mbps",
        end="\r"
    )

    pd.DataFrame([{

        "ts_created": pd.Timestamp.now(),

        "ts": pd.Timestamp.now() + timedelta(seconds=HORIZON),

        "y_pred": pred

    }]).to_sql(TABLE_FORECAST, engine, if_exists="append", index=False)


# =========================
# MAIN LOOP
# =========================

print("Forecast v2 Remake started")

while True:

    if MODEL is None or time.time() - LAST_TRAIN > TRAIN_INTERVAL:

        train()

    predict()

    time.sleep(5)