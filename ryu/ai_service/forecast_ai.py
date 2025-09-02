import psycopg2, pandas as pd, numpy as np, os
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

DB_CONN = "dbname=yourdb user=youruser password=yourpass host=127.0.0.1"
INTERVAL = 5  # seconds
SEQ_LEN = 10

def get_data(host, app):
    conn = psycopg2.connect(DB_CONN)
    df = pd.read_sql(f"""
        SELECT timestamp, bytes_tx FROM traffic.flow_stats
        WHERE host='{host}' AND app='{app}'
        ORDER BY timestamp ASC
    """, conn)
    conn.close()
    return df['bytes_tx'].values

def train_predict(data):
    X,Y=[],[]
    for i in range(len(data)-SEQ_LEN):
        X.append(data[i:i+SEQ_LEN])
        Y.append(data[i+SEQ_LEN])
    X=np.array(X).reshape(-1,SEQ_LEN,1)
    Y=np.array(Y)
    model=Sequential()
    model.add(LSTM(32,input_shape=(SEQ_LEN,1)))
    model.add(Dense(1))
    model.compile(optimizer='adam',loss='mse')
    model.fit(X,Y,epochs=5,batch_size=8,verbose=0)
    pred=model.predict(X[-1].reshape(1,SEQ_LEN,1))
    return pred[0][0]

if __name__=="__main__":
    # Contoh host/app
    host_app_list=[('10.0.0.1','youtube'),('10.0.0.2','netflix'),('10.0.0.3','twitch')]
    for host, app in host_app_list:
        data = get_data(host, app)
        if len(data) > SEQ_LEN:
            pred_bytes = train_predict(data)
            bps = int(pred_bytes*8/INTERVAL)
            os.system(f"python3 translator_auto.py \"batasi {app} {bps}bps\"")
            print(f"{app} predicted: {bps}bps → applied")
