import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
from sklearn.metrics import mean_squared_error

color_pal = sns.color_palette()
plt.style.use('fivethirtyeight')
df = pd.read_csv('drive/MyDrive/datasets/beta_dataset_v1.csv')
df = df.set_index('ts')
df.index=pd.to_datetime(df.index)
df.head()

#####



TARGET = 'throughput_bps'

# time features (boleh ada, tapi bukan utama)
df['hour'] = df.index.hour
df['minute'] = df.index.minute
df['second'] = df.index.second

# lag features (WAJIB)
LAGS = [1, 2, 3, 5, 10]

for lag in LAGS:
    df[f'lag_{lag}'] = df[TARGET].shift(lag)

# rolling statistics (window kecil)
df['roll_mean_5'] = df[TARGET].rolling(5).mean()
df['roll_std_5']  = df[TARGET].rolling(5).std()

# buang NaN akibat lag & rolling
df = df.dropna()

####

cutoff = pd.Timestamp('2015-04-15 00:15:00')

train = df.loc[df.index < cutoff]
test  = df.loc[df.index >= cutoff]


####

FEATURES = (
    [f'lag_{l}' for l in LAGS] +
    ['roll_mean_5', 'roll_std_5'] +
    ['second']     # optional
)

X_train = train[FEATURES]
y_train = train[TARGET]

X_test  = test[FEATURES]
y_test  = test[TARGET]

split = int(len(X_train) * 0.8)

X_tr = X_train.iloc[:split]
y_tr = y_train.iloc[:split]

X_val = X_train.iloc[split:]
y_val = y_train.iloc[split:]



###


reg = xgb.XGBRegressor(
    n_estimators=2000,
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


###

pred = reg.predict(X_test)
pred_series = pd.Series(pred, index=y_test.index)
start = '2015-04-15 00:15:00'
end   = '2015-04-15 00:30:00'
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

ax.set_title('1 minute of data: Actual vs Prediction')
ax.legend()
plt.show()


###

mse = mean_squared_error(y_test, pred)
rmse = np.sqrt(mse)

print(f"RMSE: {rmse:.2f}")

###

smape = np.mean(
    2 * np.abs(pred_series - y_test) /
    (np.abs(y_test) + np.abs(pred_series) + 1e-6)
) * 100

print(f"sMAPE: {smape:.2f}%")

###

reg.save_model('drive/MyDrive/datasets/beta_model_voip_v1.json')

### FUTURE DF ###

n_future = 5  # 5 detik ke depan

last_ts = df.index.max()
future_index = pd.date_range(
    start=last_ts + pd.Timedelta(seconds=1),
    periods=n_future,
    freq='1s'
)

future_df = pd.DataFrame(index=future_index)


###
def recursive_forecast(
    model,
    df_hist,
    target,
    features,
    lags,
    n_steps
):
    df = df_hist.copy()
    preds = []

    for _ in range(n_steps):
        ts = df.index.max() + pd.Timedelta(seconds=1)

        row = pd.DataFrame(index=[ts])

        # time features
        row['second'] = ts.second

        # lag features
        for lag in lags:
            row[f'lag_{lag}'] = df[target].iloc[-lag]

        # rolling features
        row['roll_mean_5'] = df[target].iloc[-5:].mean()
        row['roll_std_5']  = df[target].iloc[-5:].std()

        # predict
        y_pred = model.predict(row[features])[0]

        # append
        row[target] = y_pred
        df = pd.concat([df, row])
        preds.append(y_pred)

    return pd.Series(preds, index=future_index)
###

future_pred = recursive_forecast(
    model=reg,
    df_hist=df,
    target='throughput_bps',
    features=FEATURES,
    lags=LAGS,
    n_steps=5
)

future_pred.plot(title='Future 5-second Forecast')
plt.show()

### USE THE MODEL LATER ###

reg_loaded = xgb.XGBRegressor()
reg_loaded.load_model('drive/MyDrive/datasets/beta_model_voip_v2.json')

def recursive_forecast(model, df_hist, target, features, lags, n_steps):
    df = df_hist.copy()
    preds = []

    for _ in range(n_steps):
        ts = df.index.max() + pd.Timedelta(seconds=1)
        row = pd.DataFrame(index=[ts])

        row['second'] = ts.second

        for lag in lags:
            row[f'lag_{lag}'] = df[target].iloc[-lag]

        row['roll_mean_5'] = df[target].iloc[-5:].mean()
        row['roll_std_5']  = df[target].iloc[-5:].std()

        y_hat = model.predict(row[features])[0]
        row[target] = y_hat

        df = pd.concat([df, row])
        preds.append(y_hat)

    return pd.Series(
        preds,
        index=pd.date_range(
            df_hist.index.max() + pd.Timedelta(seconds=1),
            periods=n_steps,
            freq='1s'
        )
    )

###

df_seed = df.iloc[-50:].copy()  # seed terakhir

future_pred = recursive_forecast(
    model=reg_loaded,
    df_hist=df_seed,
    target=TARGET,
    features=FEATURES,
    lags=LAGS,
    n_steps=5
)

future_pred.plot(title='Future 5-Second Forecast')
plt.show()


### TRY ANOTHER DATA SOURCE ###

df_sim = pd.read_csv("drive/MyDrive/datasets/beta_dataset_v2.csv")
df_sim = df_sim.set_index('ts')
df_sim.index=pd.to_datetime(df_sim.index)
df_sim = df_sim[['throughput_bps']]

df_seed = df_sim.iloc[-50:]
future_pred = recursive_forecast(
    model=reg_loaded,
    df_hist=df_seed,
    target=TARGET,
    features=FEATURES,
    lags=LAGS,
    n_steps=5
)

future_pred.plot(title='Future 5-Second Forecast')
plt.show()

