#!/usr/bin/env python3
"""
forecast_2.py — Real-time Traffic Burst Forecasting (UPDATED PRODUCTION)
ML bagian diupdate dari forecast_development.py (features, model config).
Scheduling / loop logic tidak diubah.
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# ==============================================================================
# CONFIG (UNCHANGED)
# ==============================================================================
DB_URI                 = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TABLE_FORECAST         = "forecast_1h"

TARGET                 = "throughput_bps"
BURST_THRESHOLD_BPS    = 15_000_000          # updated dari dev: 15 Mbps
PREDICTION_HORIZON_SEC = 10
TRAIN_INTERVAL_SEC     = 1800
LOOP_INTERVAL_SEC      = 5.0

# UPDATED FEATURES — dari forecast_development.py
FEATURES = [
    'throughput_bps', 'is_burst', 'consecutive_steady_sec',

    'lag_1','lag_5','lag_10','lag_15','lag_30',
    'lag_60','lag_120','lag_300','lag_450','lag_600',   # +lag_600

    'delta_1s','delta_5s','delta_10s',

    'roll_mean_10s','roll_std_10s','roll_max_10s',
    'roll_mean_30s','roll_std_30s',
    'roll_mean_60s','roll_std_60s',                    # +60s rolling

    'ema_5s','ema_15s','ema_cross',

    # time-of-day cyclical features (baru dari dev)
    'hour_sin','hour_cos',
    'min_sin','min_cos',
    'dow_sin','dow_cos',
]

TRAINED_MODEL   = None
LAST_TRAIN_TIME = 0

engine = create_engine(DB_URI, pool_pre_ping=True)


# ==============================================================================
# FIXED DATA FETCH — TRUE THROUGHPUT (UNCHANGED)
# ==============================================================================
def get_data(hours: float = 6):

    query = f"""
        WITH base AS (
            SELECT
                date_trunc('second', timestamp) AS ts,
                MAX(bytes_tx) AS total_bytes
            FROM traffic.flow_stats_1
            WHERE timestamp >= NOW() - INTERVAL '{hours} hours'
              AND dpid = 5
              AND bytes_tx > 0
            GROUP BY ts
        )
        SELECT
            ts,
            total_bytes * 8 AS throughput_bps
        FROM base
        ORDER BY ts
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print("DB error:", e)
        return None

    if df.empty:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    df = df.resample('1s').last()
    df[TARGET] = df[TARGET].interpolate()
    df = df.dropna()

    return df


# ==============================================================================
# FEATURE ENGINEERING — UPDATED dari forecast_development.py
# ==============================================================================
def create_features(df, for_training=False):

    data = df.copy()

    data['is_burst'] = (data[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    group_id = data['is_burst'].cumsum()
    data['consecutive_steady_sec'] = data.groupby(group_id).cumcount()
    data.loc[data['is_burst'] == 1, 'consecutive_steady_sec'] = 0

    # lags — extended dengan lag_600
    for l in [1, 5, 10, 15, 30, 60, 120, 300, 450, 600]:
        data[f'lag_{l}'] = data[TARGET].shift(l)

    data['delta_1s']  = data[TARGET].diff(1)
    data['delta_5s']  = data[TARGET].diff(5)
    data['delta_10s'] = data[TARGET].diff(10)

    data['roll_mean_10s'] = data[TARGET].rolling(10).mean()
    data['roll_std_10s']  = data[TARGET].rolling(10).std()
    data['roll_max_10s']  = data[TARGET].rolling(10).max()
    data['roll_mean_30s'] = data[TARGET].rolling(30).mean()
    data['roll_std_30s']  = data[TARGET].rolling(30).std()
    data['roll_mean_60s'] = data[TARGET].rolling(60).mean()   # NEW
    data['roll_std_60s']  = data[TARGET].rolling(60).std()    # NEW

    data['ema_5s']    = data[TARGET].ewm(span=5).mean()
    data['ema_15s']   = data[TARGET].ewm(span=15).mean()
    data['ema_cross'] = data['ema_5s'] - data['ema_15s']

    # cyclical time features (NEW)
    data['hour_sin'] = np.sin(2 * np.pi * data.index.hour / 24)
    data['hour_cos'] = np.cos(2 * np.pi * data.index.hour / 24)
    data['min_sin']  = np.sin(2 * np.pi * data.index.minute / 60)
    data['min_cos']  = np.cos(2 * np.pi * data.index.minute / 60)
    data['dow_sin']  = np.sin(2 * np.pi * data.index.dayofweek / 7)
    data['dow_cos']  = np.cos(2 * np.pi * data.index.dayofweek / 7)

    if for_training:
        data['target_future'] = data[TARGET].shift(-PREDICTION_HORIZON_SEC)
        data = data.dropna()
    else:
        data = data.dropna(subset=['lag_600'])

    return data


# ==============================================================================
# TRAIN MODEL — UPDATED hyperparameter + early stopping dari forecast_development
# ==============================================================================
def train_model():

    global TRAINED_MODEL, LAST_TRAIN_TIME

    print("\nTraining model...")

    df = get_data(12)

    if df is None or len(df) < 2000:
        print("Not enough data")
        return

    df_train = create_features(df, True)

    X = df_train[FEATURES]
    y = df_train['target_future']

    # split 10% akhir untuk validasi / early stopping
    val_size = int(len(df_train) * 0.1)
    X_tr, X_val = X.iloc[:-val_size], X.iloc[-val_size:]
    y_tr, y_val = y.iloc[:-val_size], y.iloc[-val_size:]

    model = xgb.XGBRegressor(
        n_estimators=1000,
        max_depth=5,
        learning_rate=0.04,

        subsample=0.8,
        colsample_bytree=0.8,

        min_child_weight=5,
        gamma=0.5,
        reg_alpha=0.05,
        reg_lambda=1.0,

        early_stopping_rounds=40,

        objective="reg:squarederror",
        n_jobs=-1,
        random_state=42
    )

    model.fit(
        X_tr, y_tr,
        eval_set=[(X_val, y_val)],
        verbose=False
    )

    TRAINED_MODEL   = model
    LAST_TRAIN_TIME = time.time()

    print(f"Training complete: {len(X_tr):,} samples | Best round: {model.best_iteration}")


# ==============================================================================
# PREDICTION (UNCHANGED LOGIC)
# ==============================================================================
def run_prediction():

    global TRAINED_MODEL

    if TRAINED_MODEL is None:
        return

    df = get_data(2)

    if df is None:
        return

    df_feat = create_features(df, False)

    if df_feat.empty:
        return

    last_row = df_feat.iloc[[-1]]

    pred = float(TRAINED_MODEL.predict(last_row[FEATURES])[0])
    pred = max(0, pred)

    ts_data   = last_row.index[-1]
    ts_target = ts_data + timedelta(seconds=PREDICTION_HORIZON_SEC)

    print(ts_data, "Forecast:", int(pred))

    res = pd.DataFrame([{
        "ts_created": ts_data,
        "ts":         ts_target,
        "y_pred":     pred
    }])

    try:
        res.to_sql(TABLE_FORECAST, engine, if_exists="append", index=False)
    except:
        pass


# ==============================================================================
# MAIN LOOP (UNCHANGED)
# ==============================================================================
if __name__ == "__main__":

    print("Forecaster started")

    while True:

        if TRAINED_MODEL is None or time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC:
            train_model()

        run_prediction()

        time.sleep(LOOP_INTERVAL_SEC)