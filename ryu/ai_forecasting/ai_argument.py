# ai_argument.py
import google.generativeai as genai
from config import GEMINI_API_KEY

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

def generate_argument(results):
    prompt = f"""
    You are a network AI assistant. 
    Analyze these forecast results and explain in structured arguments:
    - Risk level (low/medium/high)
    - Why (based on anomaly_score, traffic_trend, SLA probs)
    - Suggest flow rule action

    Results:
    {results}
    """
    resp = model.generate_content(prompt)
    return resp.text
