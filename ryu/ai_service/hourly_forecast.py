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
TABLE = "traffic.flow_stats_"        # ganti sesuai table asli
TABLE_FORECAST = "forecast_1h"  # tabel untuk simpan prediksi
TARGET = "throughput_bps"
LAGS = [1, 2, 3, 5, 10, 30, 60]      # lag panjang untuk 1 jam horizon

engine = create_engine(DB_URI)

# =========================
# HELPERS
# =========================
def get_latest_data(hours=24):
    """Ambil 24 jam terakhir dari DB"""
    q = f"""
        with x as (
        SELECT 
            date_trunc('second', timestamp) as detik, 
            dpid, 
            max(bytes_tx) as total_bytes
        FROM traffic.flow_stats_
        GROUP BY detik, dpid
        ORDER BY detik desc, dpid 

        )
        select detik as ts, total_bytes * 8 as throughput_bps from x where dpid = 5
        and (total_bytes * 8 ) > 100000 
        and detik >= NOW() - INTERVAL '{hours} hour' 
        order by 1 asc
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return None
    df = df.set_index('ts')
    df.index = pd.to_datetime(df.index)
    return df

def create_features(df):
    """Buat semua fitur lengkap untuk forecast 1 jam"""
    df = df.copy()

    # Time features
    df['hour']   = df.index.hour
    df['minute'] = df.index.minute
    df['second'] = df.index.second

    # Lag features
    for lag in LAGS:
        df[f'lag_{lag}'] = df[TARGET].shift(lag)

    # Delta features
    for lag in LAGS:
        df[f'delta_{lag}'] = df[TARGET] - df[f'lag_{lag}']
        df[f'abs_delta_{lag}'] = df[f'delta_{lag}'].abs()

    # Rolling statistics
    df['roll_mean_5']   = df[TARGET].rolling(5).mean()
    df['roll_std_5']    = df[TARGET].rolling(5).std()
    df['roll_mean_30']  = df[TARGET].rolling(30).mean()
    df['roll_std_30']   = df[TARGET].rolling(30).std()
    df['roll_mean_60']  = df[TARGET].rolling(60).mean()
    df['roll_std_60']   = df[TARGET].rolling(60).std()

    df = df.dropna()

    FEATURES = (
        [f'lag_{l}' for l in LAGS] +
        ['roll_mean_5','roll_std_5','roll_mean_30','roll_std_30','roll_mean_60','roll_std_60'] +
        ['second', 'minute', 'hour'] +
        sum([[f'delta_{l}', f'abs_delta_{l}'] for l in LAGS], [])
    )
    return df, FEATURES

def train_model(X_train, y_train):
    """Latih XGBoost model"""
    split = int(len(X_train)*0.8)
    X_tr = X_train.iloc[:split]
    y_tr = y_train.iloc[:split]
    X_val = X_train.iloc[split:]
    y_val = y_train.iloc[split:]

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

def forecast_next_hour(df, reg, FEATURES):
    """Forecast 1 jam ke depan"""
    last_ts = df.index[-1]
    freq = df.index.inferred_freq or 'S'
    next_timestamps = pd.date_range(last_ts + pd.Timedelta(seconds=1),
                                    last_ts + pd.Timedelta(hours=1),
                                    freq=freq)
    df_pred = df.copy()
    preds = []

    for ts in next_timestamps:
        row = {}
        row['hour'] = ts.hour
        row['minute'] = ts.minute
        row['second'] = ts.second

        for lag in LAGS:
            row[f'lag_{lag}'] = df_pred[TARGET].iloc[-lag]
            row[f'delta_{lag}'] = row[f'lag_{lag}'] - df_pred[TARGET].iloc[-1]
            row[f'abs_delta_{lag}'] = abs(row[f'delta_{lag}'])

        # rolling stats
        row['roll_mean_5']  = df_pred[TARGET].iloc[-5:].mean()
        row['roll_std_5']   = df_pred[TARGET].iloc[-5:].std()
        row['roll_mean_30'] = df_pred[TARGET].iloc[-30:].mean()
        row['roll_std_30']  = df_pred[TARGET].iloc[-30:].std()
        row['roll_mean_60'] = df_pred[TARGET].iloc[-60:].mean()
        row['roll_std_60']  = df_pred[TARGET].iloc[-60:].std()

        X_row = pd.DataFrame([row], index=[ts])
        X_row = X_row[FEATURES]

        y_hat = reg.predict(X_row)[0]
        preds.append((ts, y_hat))
        df_pred.loc[ts] = [y_hat]

    return preds

def save_forecast(preds):
    """Simpan hasil forecast ke DB, avoid duplicate"""
    df_save = pd.DataFrame(preds, columns=['ts', 'y_pred'])
    # hapus ts yg sudah ada di DB
    existing = pd.read_sql(f"SELECT ts FROM {TABLE_FORECAST} WHERE ts >= %s", engine, params=[df_save['ts'].min()])
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
    if df_latest is None or len(df_latest) < max(LAGS)+60:
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
