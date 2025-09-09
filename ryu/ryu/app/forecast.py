#!/usr/bin/env python3
import pandas as pd, psycopg2, numpy as np, json
from prophet import Prophet
from datetime import datetime, timedelta
from translator_auto import translate_and_apply


DB_DSN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"

def load_agg_data(days=30):
    conn = psycopg2.connect(DB_DSN)
    query = """
        SELECT date_trunc('minute', timestamp) AS ts_min, SUM(bytes_tx) AS total_bytes
        FROM traffic.flow_stats
        WHERE timestamp >= NOW() - interval '%s days'
        GROUP BY ts_min ORDER BY ts_min;
    """ % days
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# === Metrics ===
def burstiness_index(series):
    if len(series) == 0 or series.mean() == 0:
        return 0.0
    return round(series.max() / series.mean(), 2)

def anomaly_score_mad(series):
    if len(series) == 0:
        return 0.0
    median = series.median()
    mad = np.median(np.abs(series - median))
    if mad == 0:
        # fallback ke z-score
        if series.std() == 0:
            return 0.0
        z = (series - series.mean()) / series.std()
        return round(abs(z.iloc[-1]), 2)
    score = abs(series.iloc[-1] - median) / mad
    return round(float(score), 2)

def traffic_trend(series):
    if len(series) < 2:
        return 0.0
    slope = np.polyfit(range(len(series)), series, 1)[0]
    return float(slope)

def slope_to_mbps(slope_bytes_per_min):
    return (slope_bytes_per_min * 8.0) / 1e6

def save_forecast(start, end, burst, anom, trend_mbps, seasonality):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.summary_forecast_v2
        (window_start, window_end, burstiness_index, anomaly_score, traffic_trend, seasonality_pattern)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (start, end, burst, anom, trend_mbps, json.dumps(seasonality)))
    conn.commit()
    cur.close()
    conn.close()

# === MAIN ===
if __name__ == '__main__':
    df = load_agg_data(days=30)
    if len(df) < 5:
        print("❌ Data tidak cukup for forecast")
        raise SystemExit(0)

    df = df.rename(columns={'ts_min': 'ds', 'total_bytes': 'y'})
    df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)

    # Hitung rentang data (jam)
    data_range_hours = (df['ds'].max() - df['ds'].min()).total_seconds() / 3600.0

    # Prophet setup
    m = Prophet(daily_seasonality=True, yearly_seasonality=False)
    if data_range_hours >= 24*7:
        m.add_seasonality(name='weekly', period=10080, fourier_order=8)
    if data_range_hours >= 24*30:
        m.add_seasonality(name='monthly', period=43200, fourier_order=5)
    # Custom cycles
    m.add_seasonality(name='quarter_cycle', period=15, fourier_order=3)
    m.add_seasonality(name='hour_cycle', period=60, fourier_order=4)

    m.fit(df)
    future = m.make_future_dataframe(periods=15, freq='min')
    forecast = m.predict(future)

    # Pakai window 15 menit terakhir untuk metric
    recent = df[df['ds'] >= df['ds'].max() - timedelta(minutes=15)]
    burst = burstiness_index(recent['y'])
    anom = anomaly_score_mad(recent['y'])
    slope = traffic_trend(recent['y'])
    trend_mbps = slope_to_mbps(slope)

    # Ambil seasonality columns dari forecast
    seasonality_cols = [c for c in forecast.columns if "seasonal" in c]
    if seasonality_cols:
        seasonality_data = forecast[["ds"] + seasonality_cols].tail(10).to_dict(orient="records")
    else:
        seasonality_data = []

    window_start = recent['ds'].min()
    window_end = recent['ds'].max()

    print(f"✅ 15-min Forecast Window {window_start} → {window_end}")
    print(f"   Burstiness Index: {burst}")
    print(f"   Anomaly Score (MAD): {anom}")
    print(f"   Traffic Trend (Mbps/min): {trend_mbps:.4f}")
    print(f"   Seasonality cols: {seasonality_cols}")

    save_forecast(window_start, window_end, burst, anom, trend_mbps, seasonality_data)

def insert_alert(level, msg, anomaly_score=None, dpid=None, src_ip=None, dst_ip=None, flow_id=None):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.alerts(level,msg,anomaly_score,dpid,src_ip,dst_ip,flow_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """,(level,msg,anomaly_score,dpid,src_ip,dst_ip,flow_id))
    alert_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return alert_id

def insert_action(action, params, outcome, ref_alert_id):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO traffic.actions(action,params_json,outcome,reference_alert_id)
        VALUES (%s,%s,%s,%s)
    """,(action,json.dumps(params),outcome,ref_alert_id))
    conn.commit()
    cur.close()
    conn.close()

# === Decision logic based on forecast ===
def process_forecast(burst, anomaly, trend_mbps):
    if anomaly > 5:
        msg = f"⚠️ High anomaly detected (score={anomaly})"
        alert_id = insert_alert("critical", msg, anomaly_score=anomaly, dpid=1)
        # Example: throttle YouTube
        prompt = "throttle youtube 2mbps"
        out = translate_and_apply(prompt)
        insert_action("throttle", out, "ok", alert_id)
        return msg

    elif burst > 3:
        msg = f"⚠️ Burstiness detected (index={burst})"
        alert_id = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
        prompt = "prioritize netflix"
        out = translate_and_apply(prompt)
        insert_action("prioritize", out, "ok", alert_id)
        return msg

    elif trend_mbps > 50:  # >50 Mbps/minute naik
        msg = f"📈 Rapid traffic growth (trend={trend_mbps:.2f} Mbps/min)"
        alert_id = insert_alert("warn", msg, anomaly_score=anomaly, dpid=1)
        prompt = "block twitch"
        out = translate_and_apply(prompt)
        insert_action("block", out, "ok", alert_id)
        return msg

    else:
        msg = "ℹ️ Traffic normal"
        insert_alert("info", msg, anomaly_score=anomaly, dpid=1)
        return msg
