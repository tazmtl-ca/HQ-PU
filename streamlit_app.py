
# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests  # for HTTPError handling
import json
from typing import Tuple

# ------------------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------------------
st.set_page_config(page_title="Hydro‑Québec Usage Viewer", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage Viewer")

# ------------------------------------------------------------------------------
# Helpers: Monthly parsing + column normalization
# ------------------------------------------------------------------------------

def parse_monthly_rows_from_results(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse a DataFrame that has a 'results' column of JSON strings where each row contains:
      {
        "compare": { ... last year's month ... },
        "courant": { ... current month's data ... }
      }

    Returns (df_monthly_current, df_monthly_compare), each with columns:
      - month (YYYY-MM)
      - kwh (float/int)
      - start_date (YYYY-MM-DD)
      - end_date (YYYY-MM-DD)
      - avg_kwh_per_day (optional)
      - avg_temp (optional)
    """
    if df_raw is None or df_raw.empty or "results" not in df_raw.columns:
        return pd.DataFrame(), pd.DataFrame()

    current_rows = []
    compare_rows = []

    # Some payloads may have objects; others may be JSON strings—handle both.
    for _, row in df_raw.iterrows():
        try:
            payload = row["results"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            elif isinstance(payload, dict):
                pass  # already dict
            else:
                continue

            # Extract CURRENT (courant)
            cur = payload.get("courant") or {}
            cur_start = cur.get("dateDebutMois")
            cur_end   = cur.get("dateFinMois")
            cur_total = cur.get("consoTotalMois")
            cur_avg   = cur.get("moyenneKwhJourMois")
            cur_temp  = cur.get("tempMoyenneMois")
            if cur_start and cur_total is not None:
                month_label = pd.to_datetime(cur_start).to_period("M").strftime("%Y-%m")
                current_rows.append({
                    "month": month_label,
                    "kwh": cur_total,
                    "start_date": cur_start,
                    "end_date": cur_end,
                    "avg_kwh_per_day": cur_avg,
                    "avg_temp": cur_temp,
                })

            # Extract COMPARE (prior year)
            cmp = payload.get("compare") or {}
            cmp_start = cmp.get("dateDebutMois")
            cmp_end   = cmp.get("dateFinMois")
            cmp_total = cmp.get("consoTotalMois")
            cmp_avg   = cmp.get("moyenneKwhJourMois")
            cmp_temp  = cmp.get("tempMoyenneMois")
            if cmp_start and cmp_total is not None:
                month_label = pd.to_datetime(cmp_start).to_period("M").strftime("%Y-%m")
                compare_rows.append({
                    "month": month_label,
                    "kwh": cmp_total,
                    "start_date": cmp_start,
                    "end_date": cmp_end,
                    "avg_kwh_per_day": cmp_avg,
                    "avg_temp": cmp_temp,
                })
        except Exception:
            # Skip malformed rows; you could log here.
            continue

    df_current = pd.DataFrame(current_rows).sort_values("month")
    df_compare = pd.DataFrame(compare_rows).sort_values("month")
    return df_current, df_compare


def normalize_usage_df(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Normalize Hydro‑Québec usage columns for charting."""
    if df is None or df.empty:
        return df

    df = df.copy()
    # Standardize columns to lowercase for easy matching
    df.rename(columns={c: c.lower() for c in df.columns}, inplace=True)

    # Candidates for values and time columns
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
        # Normalize month to YYYY‑MM if possible
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

# ------------------------------------------------------------------------------
# Secrets & basic checks
# ------------------------------------------------------------------------------
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")
if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD secrets in Streamlit Cloud.")
    st.stop()

# ------------------------------------------------------------------------------
# UI controls
# ------------------------------------------------------------------------------
st.subheader("Selection")
granularity = st.radio("Granularity", ["Hourly", "Daily", "Monthly"], horizontal=True)
today = date.today()
col_date1, col_date2 = st.columns(2)
with col_date1:
    start_date = st.date_input("Start date (daily)", value=today - timedelta(days=30), format="YYYY-MM-DD")
with col_date2:
    end_date   = st.date_input("End date (daily)",   value=today, format="YYYY-MM-DD")

# Action buttons
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

# ------------------------------------------------------------------------------
# Caching: client as resource; data as cached data
# ------------------------------------------------------------------------------
@st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec…")
def get_client(_email: str, _password: str) -> HydroQuebec:
    client = HydroQuebec(_email, _password)
    client.login()  # obtains tokens & session
    return client

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

# ------------------------------------------------------------------------------
# Error visualization
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# RUN block: only execute after clicking the button
# ------------------------------------------------------------------------------
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
            df_api = fetch_monthly_df()
            st.subheader("Monthly usage")

            # Diagnostics (optional; remove once stable)
            with st.expander("Raw monthly API payload (first 10 rows)"):
                st.write("Shape:", df_api.shape)
                st.dataframe(df_api.head(10))

            # If API returns a 'results' JSON column (like your CSV), parse it.
            if "results" in df_api.columns:
                df_current, df_compare = parse_monthly_rows_from_results(df_api)

                tab1, tab2 = st.tabs(["Current year", "Same month last year"])
                with tab1:
                    st.dataframe(df_current, use_container_width=True)
                with tab2:
                    st.dataframe(df_compare, use_container_width=True)

                if df_current.empty:
                    st.warning("No monthly data (current) parsed from API.")
                else:
                    st.subheader("Monthly usage (current year)")
                    st.bar_chart(df_current.set_index("month")["kwh"])

            else:
                # Fallback: normalize if API already returns flat columns
                df = normalize_usage_df(df_api, "Monthly")
                st.dataframe(df, use_container_width=True)
                if df.empty:
                    st.warning("No monthly data returned.")
                elif {"month", "kwh"}.issubset(df.columns):
                    st.bar_chart(df.set_index("month")["kwh"])
                else:
                    st.info("Monthly data columns differ; showing normalized table above.")

    except requests.exceptions.HTTPError as http_err:
        show_http_error("Data retrieval", http_err)
    except Exception as e:
        # Show full stacktrace for non-HTTP exceptions
        st.exception(e)

else:
    st.info("Select granularity and (for daily) a date range, then click ▶️ RUN.")
