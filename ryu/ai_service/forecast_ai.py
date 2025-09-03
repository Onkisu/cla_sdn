import psycopg2
import pandas as pd
import numpy as np
import os
import joblib
import time
from datetime import datetime
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from tensorflow.keras.losses import MeanSquaredError


# --- CONFIG ---
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
AGG_INTERVAL_SEC = 60  # 1 menit
SEQ_LEN = 10
MODEL_DIR = "./models"
os.makedirs(MODEL_DIR, exist_ok=True)

host_app_list = [
    ('10.0.0.1','youtube'),
    ('10.0.0.2','netflix'),
    ('10.0.0.3','twitch')
]

# mapping policy limit (bisa disesuaikan)
policy_limits = {
    "youtube": "3Mbps",
    "netflix": "5Mbps",
    "twitch": "2Mbps"
}

# -----------------------------------
def get_aggregated_data(host, app):
    conn = psycopg2.connect(DB_CONN)
    query = f"""
        SELECT date_trunc('minute', timestamp) AS bucket,
               SUM(bytes_tx) AS total_tx
        FROM traffic.flow_stats
        WHERE host='{host}' AND app='{app}'
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df['total_tx'].values

# -----------------------------------
def prepare_sequences(data, scaler):
    data_scaled = scaler.transform(data.reshape(-1,1))
    X, Y = [], []
    for i in range(len(data_scaled)-SEQ_LEN):
        X.append(data_scaled[i:i+SEQ_LEN])
        Y.append(data_scaled[i+SEQ_LEN])
    return np.array(X), np.array(Y)

# -----------------------------------
def train_or_load_model(host, app, data):
    model_file = f"{MODEL_DIR}/{host.replace('.','_')}_{app}_lstm.h5"
    scaler_file = f"{MODEL_DIR}/{host.replace('.','_')}_{app}_scaler.pkl"

    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data.reshape(-1,1))

    if os.path.exists(model_file) and os.path.exists(scaler_file):
        # load model + scaler
        model = load_model(model_file)
        scaler = joblib.load(scaler_file)
    else:
        # build model baru
        model = Sequential()
        model.add(LSTM(32, input_shape=(SEQ_LEN,1)))
        model.add(Dense(1))
        model.compile(optimizer="adam", loss=MeanSquaredError())

        X, Y = prepare_sequences(data, scaler)
        if len(X) > 0:
            model.fit(X, Y, epochs=5, batch_size=8, verbose=0)
            model.save(model_file)
            joblib.dump(scaler, scaler_file)

    return model, scaler

# -----------------------------------
def predict_next(model, scaler, data):
    data_scaled = scaler.transform(data.reshape(-1,1))
    if len(data_scaled) < SEQ_LEN:
        return None
    last_seq = data_scaled[-SEQ_LEN:].reshape(1, SEQ_LEN, 1)
    pred_scaled = model.predict(last_seq, verbose=0)
    pred = scaler.inverse_transform(pred_scaled)[0][0]
    return pred

# -----------------------------------
def get_trend(data):
    if len(data) < 5:
        return "stable"
    last = np.mean(data[-3:])
    prev = np.mean(data[-6:-3])
    if last > prev * 1.1:
        return "up"
    elif last < prev * 0.9:
        return "down"
    else:
        return "stable"

# -----------------------------------
def insert_forecast(rows):
    """Insert multiple rows ke traffic.forecast"""
    try:
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO traffic.forecast
            (ts, host, app, actual_bps, pred_bps, confidence, trend, policy_limit, qoe_risk)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB insert error: {e}")

# -----------------------------------
if __name__=="__main__":
    models = {}
    scalers = {}
    for host, app in host_app_list:
        data = get_aggregated_data(host, app)
        if len(data) < 2:
            print(f"{host}/{app}: data tidak cukup untuk inisialisasi model.")
            continue
        model, scaler = train_or_load_model(host, app, data)
        models[(host, app)] = model
        scalers[(host, app)] = scaler

    while True:
        rows_to_insert = []
        for host, app in host_app_list:
            data = get_aggregated_data(host, app)
            if len(data) <= SEQ_LEN:
                continue

            model = models.get((host, app))
            scaler = scalers.get((host, app))
            if model is None or scaler is None:
                continue

            # --- actual last value (ground truth) ---
            actual_bytes = data[-1]
            actual_bps = int(actual_bytes * 8 / AGG_INTERVAL_SEC)

            # --- forecast ---
            pred_bytes = predict_next(model, scaler, data)
            if pred_bytes is None:
                continue
            pred_bps = int(pred_bytes * 8 / AGG_INTERVAL_SEC)

            # --- confidence score ---
            X, Y = prepare_sequences(data, scaler)
            conf = None
            if len(X) > 0:
                yhat = model.predict(X, verbose=0)
                rmse = np.sqrt(mean_squared_error(Y, yhat))
                conf = float(max(0, 1 - rmse))

            # --- trend ---
            trend = get_trend(data)

            # --- policy ---
            policy_limit = policy_limits.get(app, "N/A")

            # --- QoE risk ---
            if app == "youtube" and pred_bps < 3_000_000:
                qoe_risk = "high"
            elif app == "twitch" and pred_bps < 2_000_000:
                qoe_risk = "high"
            else:
                qoe_risk = "low"

            # --- append row ke buffer ---
            rows_to_insert.append((
                datetime.now(), host, app,
                actual_bps, pred_bps, conf, trend, policy_limit, qoe_risk
            ))

            # --- throttle command ---
            os.system(f"python3 translator_auto.py \"batasi {app} {pred_bps}bps\"")
            print(f"{host}/{app} → actual={actual_bps}bps, pred={pred_bps}bps, conf={conf}, trend={trend}, qoe={qoe_risk}")

        if rows_to_insert:
            insert_forecast(rows_to_insert)

        time.sleep(60)
