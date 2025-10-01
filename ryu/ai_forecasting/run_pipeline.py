# run_pipeline.py
from forecast import forecast_category
from ai_argument import generate_argument
from decision import decide_action
from db import insert_forecast_result

CATEGORIES = ["video","voip","gaming","iot"]

def main():
    results = []
    for cat in CATEGORIES:
        res = forecast_category(cat)
        if not res: 
            continue
        arg = generate_argument(res)
        res["argument"] = arg
        res["decision"] = decide_action(res)
        insert_forecast_result(res)
        results.append(res)
    print("Pipeline finished. Results saved.")

if __name__ == "__main__":
    main()
