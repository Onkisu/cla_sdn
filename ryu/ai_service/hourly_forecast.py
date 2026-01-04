import pandas as pd
import xgboost as xgb
from sqlalchemy import create_engine
from datetime import timedelta

# =====================
# CONFIG
# =====================
DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TARGET = "throughput_bps"

LAGS = [1,2,3,5,10]
FEATURES = (
    [f"lag_{l}" for l in LAGS] +
    ["roll_mean_5", "roll_std_5", "second", "delta_1", "abs_delta_1"]
)

TRAIN_HOURS  = 24
FORECAST_SEC = 3600  # 1 jam

engine = create_engine(DB_URI)

# =====================
# LOAD DATA
# =====================
def load_data(hours):
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
    and detik >= now() - interval '{hours} hour' order by 1 asc
    
    """
    df = pd.read_sql(q, engine)
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")

# =====================
# FEATURE ENGINEERING
# =====================
def build_features(df):
    df = df.copy()
    df["second"] = df.index.second

    for lag in LAGS:
        df[f"lag_{lag}"] = df[TARGET].shift(lag)

    df["delta_1"] = df[TARGET] - df["lag_1"]
    df["abs_delta_1"] = df["delta_1"].abs()

    df["roll_mean_5"] = df[TARGET].rolling(5).mean()
    df["roll_std_5"]  = df[TARGET].rolling(5).std()

    return df.dropna()

# =====================
# MAIN
# =====================
print("📥 loading 24h data")
df_raw = load_data(TRAIN_HOURS + 1)
df_feat = build_features(df_raw)

ts_last = df_feat.index.max()
X_base = df_feat[FEATURES]

predictions = []

print("🚀 training + direct forecasting")

for h in range(1, FORECAST_SEC + 1):
    df_feat[f"y_t+{h}"] = df_feat[TARGET].shift(-h)

data = df_feat.dropna()

X = data[FEATURES]

for h in range(1, FORECAST_SEC + 1):
    y = data[f"y_t+{h}"]

    model = xgb.XGBRegressor(
        n_estimators=10000,
        learning_rate=0.01,
        max_depth=3,
        min_child_weight=5,
        gamma=0.5,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42
    )

    model.fit(X, y, verbose=False)

    y_pred = model.predict(X_base.iloc[-1:])[0]

    predictions.append({
        "ts": ts_last + timedelta(seconds=h),
        "y_pred": y_pred
    })

# =====================
# SAVE FORECAST
# =====================
forecast_df = pd.DataFrame(predictions).set_index("ts")

forecast_df.to_sql(
    "forecast_1h",
    engine,
    if_exists="append",
    index=True
)

print("✅ NON-RECURSIVE hourly forecast completed")
