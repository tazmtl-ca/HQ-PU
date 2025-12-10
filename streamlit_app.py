
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec

st.set_page_config(page_title="Hydro‑Québec Usage Viewer", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage Viewer")

# --- Secrets ---
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")

if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD secrets.")
    st.stop()

@st.cache_data(ttl=1800)
def login_and_get_client(email, password):
    client = HydroQuebec(email, password)
    client.login()
    return client

try:
    client = login_and_get_client(email, password)
    st.success("Authenticated to Hydro‑Québec API.")
except Exception as e:
    st.error(f"Login failed: {e}")
    st.stop()

granularity = st.radio("Granularity", ["Hourly", "Daily", "Monthly"], horizontal=True)

today = date.today()
start_date = st.date_input("Start date", value=today - timedelta(days=30))
end_date = st.date_input("End date", value=today)

@st.cache_data(ttl=600)
def fetch_hourly(client):
    return pd.DataFrame(client.get_hourly_usage())

@st.cache_data(ttl=600)
def fetch_daily(client, start_date, end_date):
    return pd.DataFrame(client.get_daily_usage(start_date.isoformat(), end_date.isoformat()))

@st.cache_data(ttl=600)
def fetch_monthly(client):
    return pd.DataFrame(client.get_monthly_usage())

if granularity == "Hourly":
    df = fetch_hourly(client)
    st.line_chart(df.set_index("timestamp")["kWh"])
elif granularity == "Daily":
    df = fetch_daily(client, start_date, end_date)
    st.bar_chart(df.set_index("date")["kWh"])
else:
    df = fetch_monthly(client)
