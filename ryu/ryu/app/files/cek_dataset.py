import pandas as pd

CSV_PATH               = "dataset_ta_2.csv"

df = pd.read_csv(CSV_PATH)
print(df.info())