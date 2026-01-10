#!/usr/bin/env python3
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"

TABLE = "traffic.flow_stats_"
TABLE_FORECAST = "forecast_1h"

TARGET = "throughput_bps"

# Predict 1–5 seconds ahead
HORIZON_STEPS = [1, 2, 3, 4, 5]

engine = create_engine(DB_URI)

# =========================
# 1. FETCH DATA
# =========================
def get_training_data(hours=24):
    query = f"""
        WITH x AS (
            SELECT
                date_trunc('second', timestamp) AS ts,
                max(bytes_tx) AS total_bytes
            FROM {TABLE}
            WHERE dpid = 5
            GROUP BY ts
            ORDER BY ts DESC
        )
        SELECT
            ts,
            total_bytes * 8 AS throughput_bps
        FROM x
        WHERE ts >= NOW() - INTERVAL '{hours} hour'
        ORDER BY ts ASC
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    # Force 1s time step
    df = df.resample('1s').max().fillna(0)

    return df

# =========================
# 2. FEATURE ENGINEERING
# =========================
def create_features(df):
    df = df.copy()

    # --- Burst detection ---
    df['is_burst'] = (df[TARGET] > 500_000).astype(int)

    # --- Idle timer ---
    mask = df['is_burst'] == 1
    first_ts = df.index[0]
    df['last_burst_ts'] = (
        df.index.to_series()
        .where(mask)
        .ffill()
        .fillna(first_ts)
    )


    df['seconds_since_burst'] = (
        df.index - df['last_burst_ts']
    ).dt.total_seconds()

    max_idle = df['seconds_since_burst'].max()
    df['seconds_since_burst'] = df['seconds_since_burst'].fillna(max_idle)

    # --- Time context ---
    df['hour'] = df.index.hour

    # --- Short lags (cukup untuk 1–5s) ---
    lags = [1, 2, 3, 5]
    for l in lags:
        df[f'lag_{l}'] = df[TARGET].shift(l)

    df = df.dropna()

    if df.empty:
        return None, None

    feature_cols = ['seconds_since_burst', 'hour'] + [f'lag_{l}' for l in lags]
    return df, feature_cols

# =========================
# 3. TRAIN & PREDICT (DIRECT STRATEGY)
# =========================
def train_and_predict(df, feature_cols):

    if df is None or len(df) < 100:
        print("❌ Data fitur terlalu sedikit")
        return []

    latest_row = df[feature_cols].iloc[[-1]]
    last_ts = df.index[-1]

    predictions = []

    for step in HORIZON_STEPS:
        y = df[TARGET].shift(-step)

        valid_idx = y.dropna().index
        X_train = df.loc[valid_idx, feature_cols]
        y_train = y.loc[valid_idx]

        if len(X_train) < 200:
            continue

        model = xgb.XGBRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            objective="reg:squarederror",
            subsample=0.8,
            colsample_bytree=0.8,
            n_jobs=-1,
            random_state=42
        )

        model.fit(X_train, y_train)

        pred = model.predict(latest_row)[0]
        pred = max(0, float(pred))

        predictions.append(
            (last_ts + timedelta(seconds=step), pred)
        )

    return predictions

# =========================
# 4. MAIN
# =========================
def main():
    print("📥 Fetching training data...")
    df = get_training_data(hours=24)

    if df is None or len(df) < 2000:
        print("❌ Data tidak cukup")
        return

    df_feat, feature_cols = create_features(df)

    if df_feat is None:
        print("❌ Feature engineering gagal")
        return

    last_idle = df_feat['seconds_since_burst'].iloc[-1]
    last_val = df_feat[TARGET].iloc[-1]

    print(
        f"⏱️ CURRENT STATE | Traffic={last_val:.0f} bps | "
        f"Idle={last_idle:.0f}s"
    )

    preds = train_and_predict(df_feat, feature_cols)

    if not preds:
        print("❌ Tidak ada prediksi")
        return

    df_pred = pd.DataFrame(preds, columns=['ts', 'y_pred'])
    print("\n🔮 PREDIKSI 1–5 DETIK KEDEPAN")
    print(df_pred)

    # --- Save to DB (avoid duplicates) ---
    existing = pd.read_sql(
        f"SELECT ts FROM {TABLE_FORECAST} WHERE ts >= %s",
        engine,
        params=(df_pred['ts'].min(),)
    )

    if not existing.empty:
        df_pred = df_pred[~df_pred['ts'].isin(existing['ts'])]

    if not df_pred.empty:
        df_pred.to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
        print("✅ Saved to DB")
    else:
        print("ℹ️ Semua timestamp sudah ada")

# =========================
if __name__ == "__main__":
    main()
