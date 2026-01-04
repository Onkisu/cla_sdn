import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
from sklearn.metrics import mean_squared_error

color_pal = sns.color_palette()
plt.style.use('fivethirtyeight')
df = pd.read_csv('drive/MyDrive/datasets/beta_dataset_v4.csv')
df = df.set_index('ts')
df.index=pd.to_datetime(df.index)
df.head()

df.plot(style = '-' ,
        figsize=(15,5), color = color_pal[0],
        title='VoIP Traffic Facebook Voice')

TARGET = 'throughput_bps'

# time features (boleh ada, tapi bukan utama)
df['hour'] = df.index.hour
df['minute'] = df.index.minute
df['second'] = df.index.second

# lag features (WAJIB)
LAGS = [1, 2, 3, 5, 10]

for lag in LAGS:
    df[f'lag_{lag}'] = df[TARGET].shift(lag)

# delta / micro-movement
df['delta_1'] = df[TARGET] - df['lag_1']
df['abs_delta_1'] = df['delta_1'].abs()

# rolling statistics (window kecil)
df['roll_mean_5'] = df[TARGET].rolling(5).mean()
df['roll_std_5']  = df[TARGET].rolling(5).std()

# buang NaN akibat lag & rolling
df = df.dropna()


cutoff = pd.Timestamp('2026-01-04 05:10:00+07:00')

train = df.loc[df.index < cutoff]
test  = df.loc[df.index >= cutoff]


FEATURES = (
    [f'lag_{l}' for l in LAGS] +
    ['roll_mean_5', 'roll_std_5'] +
    ['second']     # optional
)

FEATURES += ['delta_1', 'abs_delta_1']


X_train = train[FEATURES]
y_train = train[TARGET]

X_test  = test[FEATURES]
y_test  = test[TARGET]

split = int(len(X_train) * 0.8)

X_tr = X_train.iloc[:split]
y_tr = y_train.iloc[:split]

X_val = X_train.iloc[split:]
y_val = y_train.iloc[split:]



reg = xgb.XGBRegressor(
    n_estimators=10000,
    learning_rate=0.01,

    max_depth=3,
    min_child_weight=10,
    gamma=1,

    subsample=0.8,
    colsample_bytree=0.8,

    objective='reg:squarederror',
    random_state=42,
    early_stopping_rounds=50
)

reg.fit(
    X_tr, y_tr,
    eval_set=[(X_val, y_val)],
    verbose=100
)

pred = reg.predict(X_test)
pred_series = pd.Series(pred, index=y_test.index)
start = '2026-01-04 05:10:00+07:00'
end   = '2026-01-04 05:13:00+07:00'
fig, ax = plt.subplots(figsize=(15,5))

y_test.loc[start:end].sort_index().plot(
    ax=ax,
    label='Truth Data'
)

pred_series.loc[start:end].sort_index().plot(
    ax=ax,
    style='-',
    label='Predictions'
)

ax.set_title('3 minute of data: Actual vs Prediction')
ax.legend()
plt.show()



start = '2026-01-04 05:10:00+07:00'
end   = '2026-01-04 05:30:00+07:00'

start2 = '2026-01-04 05:10:00+07:00'
end2   = '2026-01-04 05:13:00+07:00'

# --- Plot 1: Truth Data ---
fig, ax = plt.subplots(figsize=(15,5))
y_test.loc[start:end].sort_index().plot(
    ax=ax,
    label='Truth Data',
    color='tab:blue'
)
ax.set_title('30 minutes of data: Truth')
ax.legend()
plt.show()


# --- Plot 2: Predictions ---
fig, ax = plt.subplots(figsize=(15,5))
pred_series.loc[start:end].sort_index().plot(
    ax=ax,
    style='-',
    label='Predictions',
    color='tab:red'
)
ax.set_title('30 minutes of data: Prediction')
ax.legend()
plt.show()

