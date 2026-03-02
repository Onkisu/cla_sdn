#!/usr/bin/env python3


import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine
from datetime import timedelta
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# ==============================================================================
# CONFIG
# ==============================================================================
DB_URI                 = "postgresql://dev_one:hijack332.@103.181.142.165:5432/development"

TARGET                 = "throughput_bps"
BURST_THRESHOLD_BPS    = 15_000_000
PREDICTION_HORIZON_SEC = 10

TRAIN_HOURS            = 24  # jam untuk training
TEST_HOURS             = 4   # jam terakhir untuk testing

FEATURES = [
    'throughput_bps', 'is_burst', 'consecutive_steady_sec',
    'lag_1','lag_5','lag_10','lag_15','lag_30',
    'lag_60','lag_120','lag_300','lag_450','lag_600',
    'delta_1s','delta_5s','delta_10s',
    'roll_mean_10s','roll_std_10s','roll_max_10s',
    'roll_mean_30s','roll_std_30s',
    'roll_mean_60s','roll_std_60s',
    'ema_5s','ema_15s','ema_cross',
    'hour_sin','hour_cos',
    'min_sin','min_cos',
    'dow_sin','dow_cos',
]

engine = create_engine(DB_URI, pool_pre_ping=True)


def get_data(hours):
    query = f"""
        WITH base AS (
            SELECT
                date_trunc('second', timestamp) AS ts,
                MAX(bytes_tx) AS total_bytes
            FROM traffic.flow_stats_
            WHERE timestamp >= NOW() - INTERVAL '{hours} hours'
              AND dpid = 5
              AND bytes_tx > 0
            GROUP BY ts
        )
        SELECT ts, total_bytes * 8 AS throughput_bps
        FROM base ORDER BY ts
    """
    df = pd.read_sql(query, engine)
    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts').resample('1s').last()
    df[TARGET] = df[TARGET].interpolate()
    return df.dropna()



def create_features(df, for_training=False):
    data = df.copy()
    data['is_burst'] = (data[TARGET] > BURST_THRESHOLD_BPS).astype(int)

    group_id = data['is_burst'].cumsum()
    data['consecutive_steady_sec'] = data.groupby(group_id).cumcount()
    data.loc[data['is_burst'] == 1, 'consecutive_steady_sec'] = 0

    for l in [1, 5, 10, 15, 30, 60, 120, 300, 450]:
        data[f'lag_{l}'] = data[TARGET].shift(l)

    data['delta_1s']  = data[TARGET].diff(1)
    data['delta_5s']  = data[TARGET].diff(5)
    data['delta_10s'] = data[TARGET].diff(10)

    data['roll_mean_10s'] = data[TARGET].rolling(10).mean()
    data['roll_std_10s']  = data[TARGET].rolling(10).std()
    data['roll_max_10s']  = data[TARGET].rolling(10).max()
    data['roll_mean_30s'] = data[TARGET].rolling(30).mean()
    data['roll_std_30s']  = data[TARGET].rolling(30).std()


    data['roll_mean_60s'] = data[TARGET].rolling(60).mean()
    data['roll_std_60s']  = data[TARGET].rolling(60).std()
    data['lag_600']       = data[TARGET].shift(600)   # 10 menit lalu


    data['ema_5s']    = data[TARGET].ewm(span=5).mean()
    data['ema_15s']   = data[TARGET].ewm(span=15).mean()
    data['ema_cross'] = data['ema_5s'] - data['ema_15s']


    data['hour_sin'] = np.sin(2 * np.pi * data.index.hour / 24)
    data['hour_cos'] = np.cos(2 * np.pi * data.index.hour / 24)
    data['min_sin']  = np.sin(2 * np.pi * data.index.minute / 60)
    data['min_cos']  = np.cos(2 * np.pi * data.index.minute / 60)
    data['dow_sin']  = np.sin(2 * np.pi * data.index.dayofweek / 7)
    data['dow_cos']  = np.cos(2 * np.pi * data.index.dayofweek / 7)


    if for_training:
        data['target_future'] = data[TARGET].shift(-PREDICTION_HORIZON_SEC)
        data = data.dropna()
    else:
        data = data.dropna(subset=['lag_600'])

    return data





def train_model(df_raw):
    df = create_features(df_raw, for_training=True)

    #10% akhir untuk validasi
    val_size = int(len(df) * 0.1)
    df_tr  = df.iloc[:-val_size]
    df_val = df.iloc[-val_size:]

    model = xgb.XGBRegressor(
        n_estimators=1000,
        max_depth=5, learning_rate=0.04,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, gamma=0.5,
        reg_alpha=0.05, reg_lambda=1.0,
        early_stopping_rounds=40,
        objective="reg:squarederror", n_jobs=-1, random_state=42
    )
    model.fit(
        df_tr[FEATURES], df_tr['target_future'],
        eval_set=[(df_val[FEATURES], df_val['target_future'])],
        verbose=False
    )
    print(f"  Training selesai: {len(df_tr):,} sampel | Best round: {model.best_iteration}")
    return model


def evaluate(model, df_raw):
    df = create_features(df_raw, for_training=True)
    y_pred = np.maximum(0, model.predict(df[FEATURES]))
    y_true = df['target_future'].values

    res = pd.DataFrame({'y_true': y_true, 'y_pred': y_pred}, index=df.index)
    res['error']           = res['y_pred'] - res['y_true']
    res['is_burst_actual'] = (res['y_true'] > BURST_THRESHOLD_BPS).astype(int)
    res['is_burst_pred']   = (res['y_pred'] > BURST_THRESHOLD_BPS).astype(int)
    return res


def compute_metrics(res):
    yt, yp = res['y_true'], res['y_pred']
    mae  = mean_absolute_error(yt, yp)
    rmse = np.sqrt(mean_squared_error(yt, yp))
    r2   = r2_score(yt, yp)
    mask = yt > 15_000_000
    mape = np.mean(np.abs((yt[mask] - yp[mask]) / yt[mask])) * 100 if mask.sum() > 0 else float('nan')

    tp = ((res['is_burst_actual']==1) & (res['is_burst_pred']==1)).sum()
    tn = ((res['is_burst_actual']==0) & (res['is_burst_pred']==0)).sum()
    fp = ((res['is_burst_actual']==0) & (res['is_burst_pred']==1)).sum()
    fn = ((res['is_burst_actual']==1) & (res['is_burst_pred']==0)).sum()
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    return dict(
        mae=mae, rmse=rmse, r2=r2, mape=mape,
        precision=precision*100, recall=recall*100, f1=f1*100,
        acc=(tp+tn)/len(res)*100,
        tp=tp, tn=tn, fp=fp, fn=fn
    )


def plot_results(res, m, train_period, test_period):
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    thresh = BURST_THRESHOLD_BPS / 1e6
    idx = res.index
    yt  = res['y_true'] / 1e6
    yp  = res['y_pred'] / 1e6

    fig.suptitle(
        f"Evaluasi Forecast — Horizon {PREDICTION_HORIZON_SEC}s\n"
        f"Train: {train_period[0].strftime('%H:%M')}–{train_period[1].strftime('%H:%M')}  |  "
        f"Test: {test_period[0].strftime('%H:%M')}–{test_period[1].strftime('%H:%M')}",
        fontsize=12, fontweight='bold'
    )

    # --- Plot 1: Actual vs Predicted ---
    ax = axes[0]
    ax.plot(idx, yt, label='Aktual', color='steelblue', lw=0.9, alpha=0.9)
    ax.plot(idx, yp, label='Prediksi', color='tomato', lw=0.9, alpha=0.8)
    ax.axhline(thresh, color='orange', ls='--', lw=1, label=f'Threshold ({thresh:.0f} Mbps)')
    ax.set_ylabel('Throughput (Mbps)')
    ax.set_title(
        f'Aktual vs Prediksi  |  MAE={m["mae"]/1e6:.3f} Mbps  |  RMSE={m["rmse"]/1e6:.3f} Mbps  |  '
        f'MAPE={m["mape"]:.1f}%  |  R²={m["r2"]:.4f}'
    )
    ax.legend(loc='upper right', fontsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.grid(True, alpha=0.3)

    # --- Plot 2: Error ---
    ax = axes[1]
    err = res['error'] / 1e6
    ax.fill_between(idx, err, 0, where=(err >= 0), color='tomato',    alpha=0.5, label='Overprediksi')
    ax.fill_between(idx, err, 0, where=(err <  0), color='steelblue', alpha=0.5, label='Underprediksi')
    ax.axhline(0, color='black', lw=0.8)
    ax.set_ylabel('Error (Mbps)')
    ax.set_title('Error = Prediksi − Aktual')
    ax.legend(loc='upper right', fontsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.grid(True, alpha=0.3)

    # --- Plot 3: Confusion Matrix Burst ---
    ax = axes[2]
    labels = ['TN\n(Steady Benar)', 'FP\n(False Alarm)', 'FN\n(Miss Burst)', 'TP\n(Burst Benar)']
    values = [m['tn'], m['fp'], m['fn'], m['tp']]
    colors = ['#2ecc71', '#e74c3c', '#e67e22', '#3498db']
    bars = ax.bar(labels, values, color=colors, edgecolor='white', lw=1.5)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                f'{val:,}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    ax.set_ylabel('Jumlah Sampel')
    ax.set_title(
        f'Burst Detection  |  Acc={m["acc"]:.1f}%  |  '
        f'Precision={m["precision"]:.1f}%  |  Recall={m["recall"]:.1f}%  |  F1={m["f1"]:.1f}%'
    )
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.show()

def main():
    print("=" * 55)
    print("  FORECAST MODEL EVALUATION")
    print("=" * 55)

    print(f"\n[1] Ambil data {TRAIN_HOURS + TEST_HOURS} jam terakhir...")
    df_all = get_data(TRAIN_HOURS + TEST_HOURS)
    print(f"  Total: {len(df_all):,} detik")

    cutoff   = df_all.index[-1] - timedelta(hours=TEST_HOURS)
    df_train = df_all[df_all.index <= cutoff]
    df_test  = df_all[df_all.index >  cutoff]
    print(f"  Train: {len(df_train):,} detik  |  Test: {len(df_test):,} detik")

    # Setelah baris cutoff di main():
    burst_train = (df_train[TARGET] > BURST_THRESHOLD_BPS).mean() * 100
    burst_test  = (df_test[TARGET]  > BURST_THRESHOLD_BPS).mean() * 100
    print(f"  Burst ratio — Train: {burst_train:.1f}%  |  Test: {burst_test:.1f}%")
    print(f"  Mean throughput — Train: {df_train[TARGET].mean()/1e6:.1f} Mbps  |  Test: {df_test[TARGET].mean()/1e6:.1f} Mbps")

    print(f"\n[2] Training...")
    model = train_model(df_train)

    print(f"\n[3] Evaluasi...")
    res = evaluate(model, df_test)
    m   = compute_metrics(res)

    print(f"\n  MAE            : {m['mae']/1e6:.3f} Mbps")
    print(f"  RMSE             : {m['rmse']/1e6:.3f} Mbps")
    print(f"  MAPE             : {m['mape']:.2f}%")
    print(f"  R²               : {m['r2']:.4f}")
    print(f"  Burst Accuracy   : {m['acc']:.1f}%")
    print(f"  Burst Precision  : {m['precision']:.1f}%")
    print(f"  Burst Recall     : {m['recall']:.1f}%")
    print(f"  Burst F1         : {m['f1']:.1f}%")


    print(f"\n[5] Threshold sweep burst detection...")
    for thresh_pct in [0.7, 0.8, 0.9, 1.0, 1.2, 1.4 ,1.6 ,1.8 , 2]:
        thresh = BURST_THRESHOLD_BPS * thresh_pct
        res_t = evaluate(model, df_test)
        res_t['is_burst_pred'] = (res_t['y_pred'] > thresh).astype(int)
        m_t = compute_metrics(res_t)
        print(f"  thresh={thresh/1e6:.0f}Mbps | Prec={m_t['precision']:.1f}% | Recall={m_t['recall']:.1f}% | F1={m_t['f1']:.1f}%")

    print(f"\n[4] Menampilkan plot...")
    plot_results(res, m,
                 train_period=(df_train.index[0], df_train.index[-1]),
                 test_period =(df_test.index[0],  df_test.index[-1]))


if __name__ == "__main__":
    main()