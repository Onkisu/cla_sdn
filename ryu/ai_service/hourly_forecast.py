#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TABLE = "traffic.flow_stats_"         # ganti sesuai table asli
TABLE_FORECAST = "forecast_1h"        # tabel untuk simpan prediksi
TARGET = "throughput_bps"
LAGS = [1, 2, 3, 5, 10, 30]           # lag fitur
ROLL_WINDOWS = [5, 15, 30]            # rolling window

engine = create_engine(DB_URI)

# =========================
# HELPERS
# =========================
def get_latest_data(hours=24):
    """Ambil 24 jam terakhir dari DB"""
    q = f"""
        WITH x AS (
            SELECT 
                date_trunc('second', timestamp) AS ts, 
                dpid, 
                max(bytes_tx) AS total_bytes
            FROM {TABLE}
            GROUP BY ts, dpid
            ORDER BY ts DESC, dpid
        )
        SELECT ts, total_bytes*8 AS throughput_bps
        FROM x
        WHERE dpid = 5
          AND (total_bytes*8) > 100000
          AND ts >= NOW() - INTERVAL '{hours} hour'
        ORDER BY ts ASC
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return None
    df = df.set_index('ts')
    df.index = pd.to_datetime(df.index)
    return df

def create_features(df):
    """Buat fitur lag, delta, rolling, time features"""
    df = df.copy()

    # Time features
    df['hour']   = df.index.hour
    df['minute'] = df.index.minute
    df['second'] = df.index.second

    # Lag & delta features
    for lag in LAGS:
        df[f'lag_{lag}'] = df[TARGET].shift(lag)
        df[f'delta_{lag}'] = df[TARGET] - df[f'lag_{lag}']
        df[f'abs_delta_{lag}'] = df[f'delta_{lag}'].abs()

    # Rolling stats
    for w in ROLL_WINDOWS:
        df[f'roll_mean_{w}'] = df[TARGET].rolling(w).mean()
        df[f'roll_std_{w}']  = df[TARGET].rolling(w).std()

    # Buang NaN akibat lag / rolling
    df = df.dropna()

    # Buat list fitur
    FEATURES = (
        [f'lag_{l}' for l in LAGS] +
        sum([[f'delta_{l}', f'abs_delta_{l}'] for l in LAGS], []) +
        [f'roll_mean_{w}' for w in ROLL_WINDOWS] +
        [f'roll_std_{w}' for w in ROLL_WINDOWS] +
        ['hour', 'minute', 'second']
    )
    return df, FEATURES

def train_model(X_train, y_train):
    """Latih XGBoost"""
    split = int(len(X_train)*0.8)
    X_tr, y_tr = X_train.iloc[:split], y_train.iloc[:split]
    X_val, y_val = X_train.iloc[split:], y_train.iloc[split:]

    reg = xgb.XGBRegressor(
        n_estimators=4000,
        learning_rate=0.01,
        max_depth=3,
        min_child_weight=5,
        gamma=0.5,
        subsample=0.8,
        colsample_bytree=0.8,
        objective='reg:squarederror',
        random_state=42,
        early_stopping_rounds=50
    )
    reg.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], verbose=False)
    return reg

def forecast_next_hour(df_feat, reg, FEATURES):
    """Forecast 1 jam ke depan NON-RECURSIVE"""
    # Ambil data terakhir 1 jam untuk forecast
    freq = df_feat.index.inferred_freq or 's'
    last_ts = df_feat.index[-1]
    next_timestamps = pd.date_range(
        last_ts + pd.Timedelta(seconds=1),
        last_ts + pd.Timedelta(hours=1),
        freq=freq
    )

    # Ambil fitur dari data asli
    # Kita pakai semua lag/rolling dari 24 jam terakhir
    df_pred = df_feat.copy()
    preds = []

    # Ambil subset untuk 1 jam ke depan sebagai "test set"
    test_index = next_timestamps
    X_test = df_pred[FEATURES].iloc[-len(test_index):]  # ambil 3600 baris terakhir

    # Forecast langsung
    y_hat = reg.predict(X_test)
    preds = list(zip(test_index, y_hat))

    return preds

def save_forecast(preds):
    """Simpan hasil forecast ke DB, avoid duplicate"""
    df_save = pd.DataFrame(preds, columns=['ts', 'y_pred'])
    
    # hapus ts yg sudah ada di DB
    existing = pd.read_sql(
        f"SELECT ts FROM {TABLE_FORECAST} WHERE ts >= %s",
        engine,
        params=(df_save['ts'].min(),)
    )
    df_save = df_save[~df_save['ts'].isin(existing['ts'])]
    
    if not df_save.empty:
        df_save.to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
        print(f"✅ Forecast saved ({len(df_save)} rows)")
    else:
        print("ℹ️ No new rows to save")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    df_latest = get_latest_data(hours=24)
    if df_latest is None or len(df_latest) < max(LAGS)+max(ROLL_WINDOWS):
        print("⏳ Not enough data for training")
    else:
        df_feat, FEATURES = create_features(df_latest)
        X_train = df_feat[FEATURES]
        y_train = df_feat[TARGET]

        print(f"🕒 Training model on last 24 hours ({len(X_train)} samples)...")
        reg = train_model(X_train, y_train)

        print("📈 Forecasting next 1 hour...")
        preds = forecast_next_hour(df_feat, reg, FEATURES)

        save_forecast(preds)
