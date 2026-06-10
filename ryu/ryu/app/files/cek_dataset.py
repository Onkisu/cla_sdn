import pandas as pd

CSV_PATH               = "dataset_ta_3.csv"

df = pd.read_csv(CSV_PATH)
print(df.info())