
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests  # for HTTPError handling

st.set_page_config(page_title="Hydro‑Québec Usage Viewer", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage Viewer")

# ===== Normalizer (place near top) =====
def normalize_usage_df(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Normalize Hydro‑Québec usage columns for charting."""
    if df is None or df.empty:
        return df

    df = df.copy()
    df.rename(columns={c: c.lower() for c in df.columns}, inplace=True)

    kwh_candidates = ["kwh", "kw_h", "valuekwh", "consumption", "energy", "valeur", "value"]
    date_candidates_monthly = ["month", "periode", "period", "date", "mois"]
    date_candidates_daily  = ["date", "jour", "day", "periode", "period"]
    ts_candidates_hourly   = ["timestamp", "time", "datetime", "heure", "period", "periode"]

    def first_existing(cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    if granularity == "Monthly":
        date_col = first_existing(date_candidates_monthly)
        val_col  = first_existing(kwh_candidates)
        if date_col and date_col != "month":
            df.rename(columns={date_col: "month"}, inplace=True)
        if val_col and val_col != "kwh":
            df.rename(columns={val_col: "kwh"}, inplace=True)
        if "month" in df.columns:
            try:
                df["month"] = pd.to_datetime(df["month"]).dt.to_period("M").astype(str)
            except Exception:
                pass

    elif granularity == "Daily":
        date_col = first_existing(date_candidates_daily)
        val_col  = first_existing(kwh_candidates)
        if date_col and date_col != "date":
            df.rename(columns={date_col: "date"}, inplace=True)
        if val_col and val_col != "kwh":
            df.rename(columns={val_col: "kwh"}, inplace=True)
        if "date" in df.columns:
            try:
                df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
            except Exception:
                pass

    elif granularity == "Hourly":
        ts_col  = first_existing(ts_candidates_hourly)
        val_col = first_existing(kwh_candidates)
        if ts_col and ts_col != "timestamp":
            df.rename(columns={ts_col: "timestamp"}, inplace=True)
        if val_col and val_col != "kwh":
            df.rename(columns={val_col: "kwh"}, inplace=True)
        if "timestamp" in df.columns:
            try:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            except Exception:
                pass

    return df

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

    try:
        # ==== HOURLY BRANCH ====
        if granularity == "Hourly":
            df = fetch_hourly_df()
            df = normalize_usage_df(df, "Hourly")

            st.subheader("Hourly usage (last 24h)")
            st.dataframe(df, use_container_width=True)
            if df.empty:
                st.warning("No hourly data returned.")
            elif {"timestamp", "kwh"}.issubset(df.columns):
                st.line_chart(df.set_index("timestamp")["kwh"])
            else:
                st.info("Hourly data columns differ; showing normalized table above.")

        # ==== DAILY BRANCH ====
        elif granularity == "Daily":
            df = fetch_daily_df(start_date.isoformat(), end_date.isoformat())
            df = normalize_usage_df(df, "Daily")

            st.subheader(f"Daily usage ({start_date} → {end_date})")
            st.dataframe(df, use_container_width=True)
            if df.empty:
                st.warning("No daily data returned for the selected range.")
            elif {"date", "kwh"}.issubset(df.columns):
                st.bar_chart(df.set_index("date")["kwh"])
            else:
                st.info("Daily data columns differ; showing normalized table above.")

        # ==== MONTHLY BRANCH ====
        else:  # Monthly
            df = fetch_monthly_df()

            # STEP 1: Diagnostics (you can remove later)
            st.write("Monthly DF shape:", df.shape)
            st.write("Monthly DF columns:", list(df.columns))
            st.write("Sample rows:")
            st.dataframe(df.head(10))

            # Normalize and chart
            df = normalize_usage_df(df, "Monthly")

            st.subheader("Monthly usage (~last 12 months)")
            st.dataframe(df, use_container_width=True)
            if df.empty:
                st.warning("No monthly data returned for your account/period.")
            elif {"month", "kwh"}.issubset(df.columns):
                st.bar_chart(df.set_index("month")["kwh"])
            else:
                st.info("Monthly data columns differ; showing normalized table above.")

    except requests.exceptions.HTTPError as http_err:
        show_http_error("Data retrieval", http_err)
    except Exception as e:
        st.exception(e)

else:
    st.info("Select granularity and (for daily) a date range, then click ▶️ RUN.")
