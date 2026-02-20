```python
#!/usr/bin/env python3
"""
forecast_2.py — Real-time Traffic Burst Forecasting (FIXED VERSION)
Fully compatible dengan versi lama + critical fixes untuk akurasi tinggi
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
BURST_THRESHOLD_BPS    = 40_000_000
PREDICTION_HORIZON_SEC = 10
TRAIN_INTERVAL_SEC     = 1800
LOOP_INTERVAL_SEC      = 5.0

# ORIGINAL FEATURES + NEW LONG-CYCLE SUPPORT
FEATURES = [
    'throughput_bps',
    'is_burst',
    'consecutive_steady_sec',

    'lag_1','lag_5','lag_10','lag_15','lag_30',

    # NEW: critical for burst cycle detection
    'lag_60','lag_120','lag_300','lag_450',

    'delta_1s','delta_5s','delta_10s',

    'roll_mean_10s','roll_std_10s','roll_max_10s',
    'roll_mean_30s','roll_std_30s',

    'ema_5s','ema_15s','ema_cross',
]

TRAINED_MODEL   = None
LAST_TRAIN_TIME = 0

engine = create_engine(DB_URI, pool_pre_ping=True)


# ==============================================================================
# FIXED DATA FETCH — TRUE THROUGHPUT (CRITICAL FIX)
# ==============================================================================
def get_data(hours: float = 6):

    query = f"""
        WITH base AS (
            SELECT
                date_trunc('second', timestamp) AS ts,
                MAX(bytes_tx) AS total_bytes
            FROM traffic.flow_stats_
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

    # proper resampling
    df = df.resample('1s').last()
    df[TARGET] = df[TARGET].interpolate()

    df = df.dropna()

    return df


# ==============================================================================
# FEATURE ENGINEERING (FULL COMPATIBLE + EXTENDED)
# ==============================================================================
def create_features(df, for_training=False):

    data = df.copy()

    data['is_burst'] = (data[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    group_id = data['is_burst'].cumsum()
    data['consecutive_steady_sec'] = data.groupby(group_id).cumcount()
    data.loc[data['is_burst']==1,'consecutive_steady_sec']=0

    # original lags
    for l in [1,5,10,15,30,60,120,300,450]:
        data[f'lag_{l}'] = data[TARGET].shift(l)

    data['delta_1s']=data[TARGET].diff(1)
    data['delta_5s']=data[TARGET].diff(5)
    data['delta_10s']=data[TARGET].diff(10)

    data['roll_mean_10s']=data[TARGET].rolling(10).mean()
    data['roll_std_10s']=data[TARGET].rolling(10).std()
    data['roll_max_10s']=data[TARGET].rolling(10).max()

    data['roll_mean_30s']=data[TARGET].rolling(30).mean()
    data['roll_std_30s']=data[TARGET].rolling(30).std()

    data['ema_5s']=data[TARGET].ewm(span=5).mean()
    data['ema_15s']=data[TARGET].ewm(span=15).mean()

    data['ema_cross']=data['ema_5s']-data['ema_15s']

    if for_training:
        data['target_future']=data[TARGET].shift(-PREDICTION_HORIZON_SEC)
        data=data.dropna()
    else:
        data=data.dropna(subset=['lag_450'])

    return data


# ==============================================================================
# TRAIN MODEL (IMPROVED STABILITY)
# ==============================================================================
def train_model():

    global TRAINED_MODEL, LAST_TRAIN_TIME

    print("\nTraining model...")

    df=get_data(12)

    if df is None or len(df)<2000:
        print("Not enough data")
        return

    df_train=create_features(df,True)

    X=df_train[FEATURES]
    y=df_train['target_future']

    model=xgb.XGBRegressor(

        n_estimators=400,
        max_depth=4,
        learning_rate=0.05,

        subsample=0.8,
        colsample_bytree=0.8,

        min_child_weight=5,
        gamma=0.5,

        objective="reg:squarederror",
        n_jobs=-1,
        random_state=42
    )

    model.fit(X,y)

    TRAINED_MODEL=model
    LAST_TRAIN_TIME=time.time()

    print("Training complete:",len(X),"samples")


# ==============================================================================
# PREDICTION (UNCHANGED LOGIC)
# ==============================================================================
def run_prediction():

    global TRAINED_MODEL

    if TRAINED_MODEL is None:
        return

    df=get_data(2)

    if df is None:
        return

    df_feat=create_features(df,False)

    if df_feat.empty:
        return

    last_row=df_feat.iloc[[-1]]

    pred=float(TRAINED_MODEL.predict(last_row[FEATURES])[0])

    pred=max(0,pred)

    ts_data=last_row.index[-1]

    ts_target=ts_data+timedelta(seconds=PREDICTION_HORIZON_SEC)

    print(ts_data,"Forecast:",int(pred))

    res=pd.DataFrame([{

        "ts_created":ts_data,
        "ts":ts_target,
        "y_pred":pred

    }])

    try:
        res.to_sql(TABLE_FORECAST,engine,if_exists="append",index=False)
    except:
        pass


# ==============================================================================
# MAIN LOOP (UNCHANGED)
# ==============================================================================
if __name__=="__main__":

    print("Forecaster started")

    while True:

        if TRAINED_MODEL is None or time.time()-LAST_TRAIN_TIME>TRAIN_INTERVAL_SEC:

            train_model()

        run_prediction()

        time.sleep(LOOP_INTERVAL_SEC)
```
