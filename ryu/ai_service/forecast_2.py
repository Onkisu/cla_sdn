#!/usr/bin/env python3
"""
forecast_2.py — Real-time Traffic Burst Forecasting
Optimized untuk traffic pattern dari:
  - bursty.py    : TCP burst h3→h2 dengan pola siklus deterministik
  - spine_leaf   : VoIP UDP + TCP background h1→h2 dengan random silence
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
# CONFIG
# ==============================================================================
DB_URI                 = "postgresql://dev_one:hijack332.@127.0.0.1:5432/development"
TABLE_FORECAST         = "forecast_1h"

TARGET                 = "throughput_bps"
BURST_THRESHOLD_BPS    = 40_000_000   # 40 Mbps
PREDICTION_HORIZON_SEC = 10           # Prediksi 10 detik ke depan
TRAIN_INTERVAL_SEC     = 1800         # Re-train tiap 30 menit
LOOP_INTERVAL_SEC      = 5.0          # Prediksi tiap 1 detik (selaras resolusi data)s

# ── Fitur yang dipakai — harus SAMA antara train & predict ──────────────────
FEATURES = [
    # Kondisi saat ini
    'throughput_bps',
    'is_burst',
    'consecutive_steady_sec',

    # Lag — menangkap history singkat & menengah
    'lag_1', 'lag_5', 'lag_10', 'lag_15', 'lag_30',

    # Delta/slope — seberapa cepat traffic berubah
    'delta_1s', 'delta_5s', 'delta_10s',

    # Rolling stats window CEPAT (10s) — sensitif terhadap burst pendek
    'roll_mean_10s', 'roll_std_10s', 'roll_max_10s',

    # Rolling stats window LAMBAT (30s) — baseline traffic normal
    'roll_mean_30s', 'roll_std_30s',

    # EMA — lebih reaktif dari rolling biasa
    'ema_5s', 'ema_15s',

    # Momentum EMA: EMA cepat − EMA lambat
    # Positif → traffic sedang naik = tanda awal burst
    'ema_cross',
]

# ==============================================================================
# GLOBAL STATE
# ==============================================================================
TRAINED_MODEL   = None
LAST_TRAIN_TIME = 0

engine = create_engine(DB_URI, pool_pre_ping=True)


# ==============================================================================
# 1. FETCH DATA
# ==============================================================================
def get_data(hours: float = 1.5) -> pd.DataFrame | None:
    """
    Ambil data dari DB, resample ke 1 detik.

    FIX #1 — Filter bytes_tx=0 di SQL agar baris kosong tidak masuk training.
    FIX #2 — Setelah resample, gap diisi ffill (bukan fillna(0)) sehingga
             VoIP silence / jeda sesi tidak merusak rolling & lag.
    """
    query = f"""
        WITH x AS (
            SELECT
                date_trunc('second', timestamp) AS detik,
                dpid,
                MAX(bytes_tx)                   AS total_bytes
            FROM  traffic.flow_stats_
            WHERE timestamp >= NOW() - INTERVAL '{hours} hours'
              AND bytes_tx   > 0
            GROUP BY detik, dpid
        )
        SELECT
            detik            AS ts,
            total_bytes * 8  AS throughput_bps
        FROM  x
        WHERE dpid = 5
        ORDER BY ts ASC
    """
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"\n❌ DB Error: {e}")
        return None

    if df.empty:
        return None

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    # Resample ke grid 1 detik; gap di-ffill, bukan fillna(0)
    df = df.resample('1s').max()
    df[TARGET] = df[TARGET].ffill().bfill()

    return df


# ==============================================================================
# 2. FEATURE ENGINEERING
# ==============================================================================
def create_features(df: pd.DataFrame, for_training: bool = False) -> pd.DataFrame:
    """
    Bangun semua fitur dari kolom throughput_bps.

    Fitur BARU vs versi lama:
      delta_Xs       — slope (arah & kecepatan perubahan traffic)
      roll_*_10s     — window kecil untuk deteksi burst pendek (≥15 detik di bursty.py)
      ema_5s/15s     — moving avg yang lebih reaktif
      ema_cross      — momentum: sinyal dini burst akan datang
      lag_15, lag_30 — pola siklus bursty.py (~445 detik per cycle)
    """
    data = df.copy()

    # ── State label ─────────────────────────────────────────────────────────
    data['is_burst'] = (data[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    # ── Consecutive steady seconds ───────────────────────────────────────────
    group_id = data['is_burst'].cumsum()
    data['consecutive_steady_sec'] = data.groupby(group_id).cumcount()
    data.loc[data['is_burst'] == 1, 'consecutive_steady_sec'] = 0

    # ── Lag features ─────────────────────────────────────────────────────────
    for l in [1, 5, 10, 15, 30]:
        data[f'lag_{l}'] = data[TARGET].shift(l)

    # ── Delta / slope ────────────────────────────────────────────────────────
    data['delta_1s']  = data[TARGET].diff(1)
    data['delta_5s']  = data[TARGET].diff(5)
    data['delta_10s'] = data[TARGET].diff(10)

    # ── Rolling window CEPAT (10s) ───────────────────────────────────────────
    data['roll_mean_10s'] = data[TARGET].rolling(window=10, min_periods=5).mean()
    data['roll_std_10s']  = data[TARGET].rolling(window=10, min_periods=5).std()
    data['roll_max_10s']  = data[TARGET].rolling(window=10, min_periods=5).max()

    # ── Rolling window LAMBAT (30s) ──────────────────────────────────────────
    data['roll_mean_30s'] = data[TARGET].rolling(window=30, min_periods=15).mean()
    data['roll_std_30s']  = data[TARGET].rolling(window=30, min_periods=15).std()

    # ── EMA & Momentum ───────────────────────────────────────────────────────
    data['ema_5s']    = data[TARGET].ewm(span=5,  adjust=False).mean()
    data['ema_15s']   = data[TARGET].ewm(span=15, adjust=False).mean()
    data['ema_cross'] = data['ema_5s'] - data['ema_15s']

    # ── Target (hanya saat training) ─────────────────────────────────────────
    if for_training:
        data['target_future'] = data[TARGET].shift(-PREDICTION_HORIZON_SEC)
        data = data.dropna()
    else:
        # Saat prediksi, cukup pastikan lag & rolling terpanjang sudah tersedia
        data = data.dropna(subset=['lag_30', 'roll_mean_30s'])

    return data


# ==============================================================================
# 3. TRAINING
# ==============================================================================
def train_model() -> None:
    global TRAINED_MODEL, LAST_TRAIN_TIME

    print("\n🧠 Training dimulai — harap tunggu...")
    t0 = time.time()

    # 6 jam agar minimal 1 siklus penuh bursty.py (~445 detik) terlihat
    df = get_data(hours=6)
    if df is None or len(df) < 1000:
        print("⚠️  Data tidak cukup (butuh ≥ 1000 baris). Training dilewati.")
        return

    df_train = create_features(df, for_training=True)
    if len(df_train) < 500:
        print("⚠️  Setelah feature engineering data terlalu sedikit. Training dilewati.")
        return

    X = df_train[FEATURES]
    y = df_train['target_future']

    # ── XGBoost dengan regularisasi ──────────────────────────────────────────
    # subsample=0.8        : tiap tree pakai 80% data → kurangi overfit
    # colsample_bytree=0.8 : tiap tree pakai 80% fitur secara random
    # min_child_weight=10  : jangan belajar dari kluster ≤10 sampel
    #                        → mencegah fit ke VoIP silence yang frekuensinya rendah
    # gamma=1              : minimum gain untuk split → pohon lebih konservatif
    model = xgb.XGBRegressor(
        n_estimators     = 1000,
        max_depth        = 6,
        learning_rate    = 0.02,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        min_child_weight = 10,
        gamma            = 1,
        n_jobs           = -1,
        random_state     = 42,
        verbosity        = 0,
    )
    model.fit(X, y)

    TRAINED_MODEL   = model
    LAST_TRAIN_TIME = time.time()

    elapsed = time.time() - t0
    print(f"✅ Training selesai dalam {elapsed:.1f}s | Sampel: {len(X):,} | Fitur: {len(FEATURES)}")
    print(f"   Next re-train dalam {TRAIN_INTERVAL_SEC // 60} menit.\n")

    # ── Top-10 Feature Importance ─────────────────────────────────────────────
    importance = dict(zip(FEATURES, model.feature_importances_))
    top10 = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
    print("   📊 Top-10 Feature Importance:")
    for fname, fscore in top10:
        bar = '█' * max(1, int(fscore * 300))
        print(f"      {fname:<22} {fscore:.4f}  {bar}")
    print()


# ==============================================================================
# 4. PREDICTION
# ==============================================================================
def run_prediction() -> None:
    global TRAINED_MODEL

    if TRAINED_MODEL is None:
        return

    df = get_data(hours=1.5)
    if df is None:
        return

    df_feat = create_features(df, for_training=False)
    if df_feat.empty:
        return

    last_row = df_feat.iloc[[-1]]

    # Guard: pastikan tidak ada fitur yang hilang
    missing = [f for f in FEATURES if f not in last_row.columns]
    if missing:
        print(f"\n⚠️  Fitur hilang: {missing}")
        return

    # ── Prediksi ─────────────────────────────────────────────────────────────
    pred_val = float(TRAINED_MODEL.predict(last_row[FEATURES])[0])

    # FIX #3 — Clamp negatif: traffic tidak bisa < 0
    pred_val = max(0.0, pred_val)

    # ── Timestamp ────────────────────────────────────────────────────────────
    # FIX #4 — Gunakan timestamp INDEX DATA, bukan Timestamp.now()
    # → prediksi ter-plot sejajar dengan actual di Grafana (sumbu X = ts_created)
    ts_data    = last_row.index[-1]
    ts_created = ts_data
    ts_target  = ts_data + timedelta(seconds=PREDICTION_HORIZON_SEC)

    # ── Terminal log ─────────────────────────────────────────────────────────
    actual_bps  = last_row['throughput_bps'].values[0]
    steady_sec  = last_row['consecutive_steady_sec'].values[0]
    delta_1s    = last_row['delta_1s'].values[0]
    ema_cross   = last_row['ema_cross'].values[0]
    trend_arrow = '↑' if delta_1s > 500_000 else ('↓' if delta_1s < -500_000 else '→')
    status      = "⚠️  DANGER" if pred_val > BURST_THRESHOLD_BPS else "✅ SAFE "

    print(
        f"[{ts_created.strftime('%H:%M:%S')}] "
        f"Now: {actual_bps/1e6:6.2f}M {trend_arrow} | "
        f"EMA✕: {ema_cross/1e6:+5.2f}M | "
        f"Steady: {steady_sec:3.0f}s | "
        f"Pred+{PREDICTION_HORIZON_SEC}s: {pred_val/1e6:6.2f}M [{status}]",
        end='\r'
    )

    # ── Simpan ke DB ─────────────────────────────────────────────────────────
    # PENTING untuk Grafana:
    #   ts_created = timestamp data yg dipakai  → pakai ini sebagai Time field
    #   ts         = timestamp prediksi (+10s)  → untuk referensi saja
    #   y_pred     = nilai prediksi (bps)
    try:
        res_df = pd.DataFrame([{
            'ts_created': ts_created,
            'ts'        : ts_target,
            'y_pred'    : pred_val,
        }])
        res_df.to_sql(TABLE_FORECAST, engine, if_exists='append', index=False)
    except Exception:
        pass


# ==============================================================================
# MAIN LOOP
# ==============================================================================
if __name__ == "__main__":
    print("=" * 65)
    print("🚀  Real-time Traffic Burst Forecaster")
    print("=" * 65)
    print(f"   Threshold  : {BURST_THRESHOLD_BPS / 1e6:.0f} Mbps")
    print(f"   Horizon    : +{PREDICTION_HORIZON_SEC} detik")
    print(f"   Loop       : tiap {LOOP_INTERVAL_SEC}s")
    print(f"   Re-train   : tiap {TRAIN_INTERVAL_SEC // 60} menit")
    print(f"   Fitur      : {len(FEATURES)}")
    print(f"   DB Table   : {TABLE_FORECAST}")
    print("=" * 65)
    print()

    try:
        while True:
            loop_start = time.time()

            # 1. Training (pertama kali atau tiap TRAIN_INTERVAL_SEC)
            if TRAINED_MODEL is None or (time.time() - LAST_TRAIN_TIME > TRAIN_INTERVAL_SEC):
                train_model()

            # 2. Prediksi
            run_prediction()

            # 3. Sleep cerdas — jaga agar loop ≈ LOOP_INTERVAL_SEC
            elapsed    = time.time() - loop_start
            sleep_time = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\n🛑 Dihentikan oleh user.")