
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec

st.set_page_config(page_title="Hydro‑Québec Usage Viewer", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage Viewer")

# ---- Secrets ----
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")
if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD secrets.")
    st.stop()

# ---- Cache the resource (client) ----
@st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec…")
def get_client(_email: str, _password: str) -> HydroQuebec:
    client = HydroQuebec(_email, _password)
    client.login()
    return client

try:
    client = get_client(email, password)
    st.success("Authenticated to Hydro‑Québec.")
except Exception as e:
    st.error(f"Login failed: {e}")
    st.stop()

# ---- UI controls ----
granularity = st.radio("Granularity", ["Hourly", "Daily", "Monthly"], horizontal=True)
today = date.today()
start_date = st.date_input("Start date", value=today - timedelta(days=30))
end_date = st.date_input("End date", value=today)

# ---- Cache DATA (returns are DataFrames / lists) ----
@st.cache_data(ttl=600, show_spinner="Fetching hourly usage…")
def fetch_hourly():
    data = client.get_hourly_usage()          # no client param passed
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching daily usage…")
def fetch_daily(start_iso: str, end_iso: str):
    data = client.get_daily_usage(start_iso, end_iso)
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching monthly usage…")
def fetch_monthly():
    data = client.get_monthly_usage()
    return pd.DataFrame(data)

# ---- Display ----
if granularity == "Hourly":
    df = fetch_hourly()
    st.subheader("Hourly usage (last 24h)")
    st.dataframe(df, use_container_width=True)
    st.line_chart(df.set_index("timestamp")["kWh"])

elif granularity == "Daily":
    df = fetch_daily(start_date.isoformat(), end_date.isoformat())
    st.subheader(f"Daily usage ({start_date} → {end_date})")
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("date")["kWh"])

else:
    df = fetch_monthly()
    st.subheader("Monthly usage (~last 12 months)")
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("month")["kWh"])
