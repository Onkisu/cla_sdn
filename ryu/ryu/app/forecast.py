import pandas as pd, psycopg2, numpy as np, json
from prophet import Prophet
from datetime import datetime, timedelta

def load_agg_data():
    conn = psycopg2.connect("dbname=development user=dev_one password=hijack332. host=127.0.0.1")
    query = """
        SELECT date_trunc('minute', timestamp) AS ts_min, SUM(bytes_tx) AS total_bytes
        FROM traffic.flow_stats
        WHERE timestamp >= NOW() - interval '15 minutes'
        GROUP BY ts_min ORDER BY ts_min;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def burstiness_index(series):
    return round(series.max() / series.mean(), 2) if series.mean() > 0 else 0

def anomaly_score(series):
    if series.std() == 0: return 0
    z = (series - series.mean()) / series.std()
    return round(abs(z.iloc[-1]), 2)

def traffic_trend(series):
    if len(series) < 2: return 0
    slope = np.polyfit(range(len(series)), series, 1)[0]
    return round(slope, 4)

def slope_to_mbps(slope_bytes_per_min):
    """Konversi slope (bytes/min) → Mbps/min"""
    # 1 byte = 8 bit, 1e6 bit = 1 Megabit
    return (slope_bytes_per_min * 8) / 1e6

def save_forecast(start, end, burst, anom, trend, seasonality):
    conn = psycopg2.connect("dbname=development user=dev_one password=hijack332. host=127.0.0.1")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.summary_forecast_v2
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, seasonality_pattern)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (start, end, burst, anom, trend, json.dumps(seasonality)))
    conn.commit()
    conn.close()

# === MAIN ===
df = load_agg_data()
if len(df) < 5:
    print("❌ Data tidak cukup untuk 15-min forecast")
else:
    df = df.rename(columns={"ts_min":"ds","total_bytes":"y"})
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_localize(None)

    # Prophet model (paksa aktifkan seasonality)
    m = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=True,
        seasonality_mode="additive"
    )

    # Tambahkan seasonality custom biar Prophet selalu bikin kolom seasonal
    m.add_seasonality(name="minute_cycle", period=60, fourier_order=3)  # siklus tiap 1 jam
    m.add_seasonality(name="quarter_cycle", period=15, fourier_order=2) # siklus 15 menit

    m.fit(df)
    future = m.make_future_dataframe(periods=15, freq="min")
    forecast = m.predict(future)

    burst = burstiness_index(df["y"])
    anom = anomaly_score(df["y"])
    trend = traffic_trend(df["y"])
    trend_mbps = slope_to_mbps(trend)
    # Ambil kolom seasonal kalau ada
    seasonality_cols = [c for c in forecast.columns if "seasonal" in c]
    if seasonality_cols:
        seasonality_data = forecast[["ds"] + seasonality_cols].tail(10).to_dict(orient="records")
    else:
        seasonality_data = []

    window_start = df["ds"].min()
    window_end = df["ds"].max()

    print(f"✅ 15-min Forecast Window {window_start} → {window_end}")
    print(f"   Burstiness: {burst}, Anomaly: {anom}, Trend: {trend_mbps}")
    print(f"   Seasonality cols: {seasonality_cols}")
    save_forecast(window_start, window_end, burst, anom, trend_mbps, seasonality_data)
