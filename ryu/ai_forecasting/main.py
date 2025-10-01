import pandas as pd
from db import fetch_flow_stats, insert_forecast_results
from forecast import forecast_category
from ai_argument import generate_arguments_batch

def main():
    df = fetch_flow_stats()
    categories = df["category"].dropna().unique()

    results = []
    for cat in categories:
        res = forecast_category(df, cat)
        if res:
            results.append(res)

    if not results:
        print("No categories found for forecasting.")
        return

    # Batch AI reasoning
    arguments = generate_arguments_batch(results)

    # Gabungkan hasil forecast + reasoning
    for r in results:
        r["argument"] = arguments.get(r["category"], "No reasoning generated")

    # Simpan ke DB
    out_df = pd.DataFrame(results)
    insert_forecast_results(out_df)

    # Print ke console
    for r in results:
        print(f"\n=== {r['category']} ===")
        print(r["argument"])

if __name__ == "__main__":
    main()
