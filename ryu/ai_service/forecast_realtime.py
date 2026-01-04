import time
import pandas as pd
import xgboost as xgb
from sqlalchemy import create_engine

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
MODEL_PATH = "beta_model_voip_v3.json"

TARGET = "throughput_bps"
LAGS   = [1, 2, 3, 5, 10]

FEATURES = (
    [f"lag_{l}" for l in LAGS] +
    ["roll_mean_5", "roll_std_5", "second", "delta_1", "abs_delta_1"]
)

BUFFER_SIZE = 50
POLL_SEC    = 1
MIN_HISTORY = max(LAGS) + 5   # safety for lag + rolling

# =========================
# LOAD MODEL
# =========================
reg = xgb.XGBRegressor()
reg.load_model(MODEL_PATH)
print("✅ Model loaded")

# =========================
# DB CONNECTION
# =========================
engine = create_engine(DB_URI)

# =========================
# FETCH DATA
# =========================
def fetch_latest(n=50):
    query = f"""
        WITH x AS (
            SELECT
                date_trunc('second', timestamp) AS ts,
                dpid,
                max(bytes_tx) AS total_bytes
            FROM traffic.flow_stats_
            GROUP BY ts, dpid
        )
        SELECT
            ts,
            total_bytes * 8 AS throughput_bps
        FROM x
        WHERE dpid = 5
          AND total_bytes * 8 > 100000
        ORDER BY ts DESC
        LIMIT {n}
    """
    df = pd.read_sql(query, engine)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.set_index("ts").sort_index()
    return df

# =========================
# FEATURE BUILDER
# =========================
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

# =========================
# 1-STEP FORECAST
# =========================
def one_step_forecast(model, df_hist):
    if len(df_hist) < MIN_HISTORY:
        return None, None

    ts_next = df_hist.index.max() + pd.Timedelta(seconds=1)
    row = pd.DataFrame(index=[ts_next])

    row["second"] = ts_next.second

    for lag in LAGS:
        row[f"lag_{lag}"] = df_hist[TARGET].iloc[-lag]

    row["roll_mean_5"] = df_hist[TARGET].iloc[-5:].mean()
    row["roll_std_5"]  = df_hist[TARGET].iloc[-5:].std()

    row["delta_1"] = df_hist[TARGET].iloc[-1] - df_hist[TARGET].iloc[-2]
    row["abs_delta_1"] = abs(row["delta_1"])

    y_pred = model.predict(row[FEATURES])[0]
    return ts_next, y_pred

# =========================
# INIT BUFFER
# =========================
df_rt = fetch_latest(BUFFER_SIZE)
df_rt = build_features(df_rt)

print("🚀 Realtime forecasting started")

# =========================
# REALTIME LOOP
# =========================
while True:
    try:
        new = fetch_latest(1)

        if not new.empty and new.index.max() > df_rt.index.max():
            df_rt = pd.concat([df_rt, new])
            df_rt = df_rt.iloc[-BUFFER_SIZE:]
            df_rt = build_features(df_rt)

            if len(df_rt) < MIN_HISTORY:
                print("⏳ waiting buffer...")
                time.sleep(POLL_SEC)
                continue

            ts_pred, y_pred = one_step_forecast(reg, df_rt)
            if ts_pred is None:
                continue

            last_real = df_rt[TARGET].iloc[-1]

            print(
                f"[REAL {df_rt.index.max()}] {last_real:.0f} | "
                f"[PRED {ts_pred}] {y_pred:.0f}"
            )

            # OPTIONAL: reroute logic
            if y_pred > last_real * 1.05:
                print("⚠️  POTENTIAL CONGESTION → PREPARE REROUTE")

        time.sleep(POLL_SEC)

    except KeyboardInterrupt:
        print("\n🛑 stopped")
        break
    except Exception as e:
        print("❌ error:", e)
        time.sleep(1)
