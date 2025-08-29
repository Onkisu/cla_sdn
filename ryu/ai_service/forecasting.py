# ai_service/forecasting.py
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import json

def forecast_from_csv(csv_path, steps=24):
    """
    csv with columns: timestamp, traffic
    timestamp parseable by pandas
    """
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df = df.set_index("timestamp").asfreq("H").fillna(method="ffill")
    model = ARIMA(df["traffic"], order=(2,1,2))
    fitted = model.fit()
    pred = fitted.forecast(steps=steps)
    out = [{"ds": str(idx), "yhat": float(x)} for idx,x in pred.items()]
    return out

if __name__=="__main__":
    import sys
    res = forecast_from_csv(sys.argv[1], steps=24)
    print(json.dumps(res, indent=2))
