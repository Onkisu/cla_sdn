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
FORECAST_SEC = 3600

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
print("📥 load data")
df_raw = load_data(TRAIN_HOURS + 1)
df_feat = build_features(df_raw)

# TRAIN / VAL SPLIT (SAMA KAYAK OFFLINE)
split = int(len(df_feat) * 0.8)

train = df_feat.iloc[:split]
val   = df_feat.iloc[split:]

X_tr, y_tr = train[FEATURES], train[TARGET]
X_val, y_val = val[FEATURES], val[TARGET]

# =====================
# TRAIN MODEL (IDENTIK)
# =====================
print("🚀 training model")

model = xgb.XGBRegressor(
    n_estimators=10000,
    learning_rate=0.01,
    max_depth=3,
    min_child_weight=5,
    gamma=0.5,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="reg:squarederror",
    random_state=42,
    early_stopping_rounds=50
)

model.fit(
    X_tr, y_tr,
    eval_set=[(X_val, y_val)],
    verbose=False
)

# =====================
# DIRECT 1H FORECAST
# =====================
print("🔮 forecasting 1 hour")

last_block = df_feat.iloc[-FORECAST_SEC:][FEATURES]
pred = model.predict(last_block)

forecast_ts = df_feat.index[-FORECAST_SEC:]
forecast_df = pd.DataFrame({
    "ts": forecast_ts,
    "y_pred": pred
}).set_index("ts")

# =====================
# SAVE TO DB
# =====================
forecast_df.to_sql(
    "traffic.forecast_1h",
    engine,
    if_exists="append",
    index=True
)

print("✅ train + forecast + save done")
