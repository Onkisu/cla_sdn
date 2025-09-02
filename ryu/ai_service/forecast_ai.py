import psycopg2
import pandas as pd
import numpy as np
import os
import joblib
import time
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler

# --- CONFIG ---
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
AGG_INTERVAL_SEC = 60  # 1 menit
SEQ_LEN = 10           # jumlah time steps untuk LSTM
MODEL_DIR = "./models"  # folder save model/scaler
os.makedirs(MODEL_DIR, exist_ok=True)

host_app_list = [
    ('10.0.0.1','youtube'),
    ('10.0.0.2','netflix'),
    ('10.0.0.3','twitch')
]

# -----------------------------------
def get_aggregated_data(host, app):
    """Ambil data bytes_tx per menit dari database"""
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
    """Prepare X, Y sequences untuk LSTM"""
    data_scaled = scaler.transform(data.reshape(-1,1))
    X, Y = [], []
    for i in range(len(data_scaled)-SEQ_LEN):
        X.append(data_scaled[i:i+SEQ_LEN])
        Y.append(data_scaled[i+SEQ_LEN])
    return np.array(X), np.array(Y)

# -----------------------------------
def train_or_load_model(host, app, data):
    """Load model kalau ada, kalau belum build dan train"""
    model_file = f"{MODEL_DIR}/{host.replace('.','_')}_{app}_lstm.h5"
    scaler_file = f"{MODEL_DIR}/{host.replace('.','_')}_{app}_scaler.pkl"

    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data.reshape(-1,1))

    if os.path.exists(model_file) and os.path.exists(scaler_file):
        model = load_model(model_file)
        scaler = joblib.load(scaler_file)
    else:
        # Build model baru
        model = Sequential()
        model.add(LSTM(32, input_shape=(SEQ_LEN,1)))
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mse')

        X, Y = prepare_sequences(data, scaler)
        if len(X) > 0:
            model.fit(X, Y, epochs=5, batch_size=8, verbose=0)
            model.save(model_file)
            joblib.dump(scaler, scaler_file)
    return model, scaler

# -----------------------------------
def predict_next(model, scaler, data):
    """Predict next value menggunakan LSTM"""
    data_scaled = scaler.transform(data.reshape(-1,1))
    if len(data_scaled) < SEQ_LEN:
        return None
    last_seq = data_scaled[-SEQ_LEN:].reshape(1, SEQ_LEN, 1)
    pred_scaled = model.predict(last_seq, verbose=0)
    pred = scaler.inverse_transform(pred_scaled)[0][0]
    return pred

# -----------------------------------
if __name__=="__main__":
    # Load semua model/scaler dulu supaya tidak retrain tiap loop
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

    # Loop prediksi tiap 1 menit
    while True:
        for host, app in host_app_list:
            data = get_aggregated_data(host, app)
            if len(data) <= SEQ_LEN:
                print(f"{host}/{app}: data tidak cukup untuk prediksi.")
                continue

            model = models.get((host, app))
            scaler = scalers.get((host, app))
            if model is None or scaler is None:
                print(f"{host}/{app}: model/scaler tidak tersedia.")
                continue

            pred_bytes = predict_next(model, scaler, data)
            if pred_bytes is None:
                print(f"{host}/{app}: prediksi gagal.")
                continue

            bps = int(pred_bytes * 8 / AGG_INTERVAL_SEC)

            # Kirim ke translator_auto.py untuk throttle
            os.system(f"python3 translator_auto.py \"batasi {app} {bps}bps\"")
            print(f"{host}/{app} predicted: {bps}bps → applied")

        # tunggu 1 menit sebelum prediksi berikutnya
        time.sleep(60)
