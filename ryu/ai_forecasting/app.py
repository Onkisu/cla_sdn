# app.py
import streamlit as st
import pandas as pd
from db import get_conn

def load_results():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM forecast_results ORDER BY created_at DESC LIMIT 50", conn)
    conn.close()
    return df

st.title("📊 Flow Stats Forecast + AI Argument Dashboard")

df = load_results()
if df.empty:
    st.warning("No forecast results yet. Run pipeline first.")
else:
    st.dataframe(df)

    for _, row in df.iterrows():
        st.subheader(f"Category: {row['category']}")
        st.write("**Argument**:", row['argument'])
        st.write("**Decision**:", row['decision'])
