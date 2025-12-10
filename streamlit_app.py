
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests  # for HTTPError handling

st.set_page_config(page_title="Hydro‑Québec Usage Viewer", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage Viewer")

# ---- Secrets ----
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")
if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD secrets in Streamlit Cloud.")
    st.stop()

# ---- Controls ----
st.subheader("Selection")
granularity = st.radio("Granularity", ["Hourly", "Daily", "Monthly"], horizontal=True)
today = date.today()
col_date1, col_date2 = st.columns(2)
with col_date1:
    start_date = st.date_input("Start date (daily)", value=today - timedelta(days=30), format="YYYY-MM-DD")
with col_date2:
    end_date   = st.date_input("End date (daily)",   value=today, format="YYYY-MM-DD")

# ---- Action buttons ----
colA, colB, colC = st.columns(3)
with colA:
    run_clicked = st.button("▶️ RUN")
with colB:
    if st.button("🧹 Clear data cache"):
        st.cache_data.clear()
        st.success("Data cache cleared.")
with colC:
    if st.button("🧹 Clear resource cache"):
        st.cache_resource.clear()
        st.success("Resource cache cleared.")

# ---- Cache the Hydro‑Québec client as a resource ----
@st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec…")
def get_client(_email: str, _password: str) -> HydroQuebec:
    client = HydroQuebec(_email, _password)
    client.login()  # obtains tokens & session
    return client

# ---- Fetch helpers (cached DATA) ----
@st.cache_data(ttl=600, show_spinner="Fetching hourly usage…")
def fetch_hourly_df():
    data = client.get_hourly_usage()
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching daily usage…")
def fetch_daily_df(start_iso: str, end_iso: str):
    data = client.get_daily_usage(start_iso, end_iso)
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching monthly usage…")
def fetch_monthly_df():
    data = client.get_monthly_usage()
    return pd.DataFrame(data)

def show_http_error(prefix: str, err: requests.exceptions.HTTPError):
    resp = getattr(err, "response", None)
    status = getattr(resp, "status_code", None)
    body = None
    try:
        body = resp.text[:500] if resp is not None and resp.text else None
    except Exception:
        pass
    with st.expander("Error details"):
        st.write(f"{prefix} HTTPError")
        st.write("Status code:", status)
        if body:
            st.code(body, language="text")
    st.error(f"{prefix} failed. Status: {status or 'unknown'}. See details above.")

# ---- RUN block: only execute after clicking the button ----
if run_clicked:
    # Validate selection before making calls
    if granularity == "Daily" and start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    # Acquire client (cached resource)
    try:
        client = get_client(email, password)
        st.caption("Logged in to Hydro‑Québec.")
    except requests.exceptions.HTTPError as http_err:
        show_http_error("Login", http_err)
        st.stop()
    except Exception as e:
        st.error(f"Login failed: {e}")
        st.stop()

    # Fetch and render by granularity
    try:
        if granularity == "Hourly":
            df = fetch_hourly_df()
            st.subheader("Hourly usage (last 24h)")
            st.dataframe(df, use_container_width=True)
            # Expect columns like 'timestamp' and 'kWh'
            if "timestamp" in df.columns and "kWh" in df.columns:
                st.line_chart(df.set_index("timestamp")["kWh"])
            else:
                st.info("Hourly data columns differ from expected ('timestamp', 'kWh'). Showing raw table above.")

        elif granularity == "Daily":
            df = fetch_daily_df(start_date.isoformat(), end_date.isoformat())
            st.subheader(f"Daily usage ({start_date} → {end_date})")
            st.dataframe(df, use_container_width=True)
            if "date" in df.columns and "kWh" in df.columns:
                st.bar_chart(df.set_index("date")["kWh"])
            else:
                st.info("Daily data columns differ from expected ('date', 'kWh'). Showing raw table above.")

        else:  # Monthly
            df = fetch_monthly_df()
            st.subheader("Monthly usage (~last 12 months)")
            st.dataframe(df, use_container_width=True)
            if "month" in df.columns and "kWh" in df.columns:
                st.bar_chart(df.set_index("month")["kWh"])
            else:
                st.info("Monthly data columns differ from expected ('month', 'kWh'). Showing raw table above.")

    except requests.exceptions.HTTPError as http_err:
        show_http_error("Data retrieval", http_err)
    except Exception as e:
        # Show full stacktrace for non-HTTP exceptions
        st.exception(e)
else:
    st.info("Select granularity and (for daily) a date range, then click ▶️ RUN.")
