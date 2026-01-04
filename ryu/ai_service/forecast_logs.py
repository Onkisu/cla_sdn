import time
import pandas as pd
from sqlalchemy import create_engine

# =========================
# CONFIG
# =========================
DB_URI = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
POLL_SEC = 1

engine = create_engine(DB_URI)

# =========================
# HELPERS
# =========================
def get_latest_real():
    q = """
        SELECT
            date_trunc('second', timestamp) AS ts,
            max(bytes_tx) * 8 AS throughput_bps
        FROM traffic.flow_stats_
        GROUP BY ts
        ORDER BY ts DESC
        LIMIT 1
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return None
    return df.iloc[0]["ts"], df.iloc[0]["throughput_bps"]


def get_forecast(ts):
    q = """
        SELECT y_pred
        FROM forecast_1h
        WHERE ts = %s
        LIMIT 1
    """
    df = pd.read_sql(q, engine, params=[ts])
    if df.empty:
        return None
    return df.iloc[0]["y_pred"]

# =========================
# LOOP
# =========================
print("🚀 realtime REAL vs FORECAST")

while True:
    try:
        real = get_latest_real()
        if real is None:
            time.sleep(1)
            continue

        ts_real, y_real = real
        ts_pred = ts_real + pd.Timedelta(seconds=1)

        y_pred = get_forecast(ts_pred)

        if y_pred is not None:
            print(
                f"[REAL {ts_real}] {y_real:.0f}  |  "
                f"[PRED {ts_pred}] {y_pred:.0f}"
            )
        else:
            print(f"[REAL {ts_real}] {y_real:.0f}  |  [PRED] N/A")

        time.sleep(POLL_SEC)

    except KeyboardInterrupt:
        print("\n🛑 stopped")
        break
    except Exception as e:
        print("❌ error:", e)
        time.sleep(1)
