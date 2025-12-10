
# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests  # HTTPError handling
import json
from typing import Tuple, Any, Dict, List

# ------------------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------------------
st.set_page_config(page_title="Hydro‑Québec Usage & Billing", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage & Billing")

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
    Returns (df_monthly_current, df_monthly_compare) with columns:
      month, kwh, start_date, end_date, avg_kwh_per_day, avg_temp
    """
    if df_raw is None or df_raw.empty or "results" not in df_raw.columns:
        return pd.DataFrame(), pd.DataFrame()

    current_rows: List[Dict[str, Any]] = []
    compare_rows: List[Dict[str, Any]] = []

    for _, row in df_raw.iterrows():
        try:
            payload = row["results"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            elif not isinstance(payload, dict):
                continue

            cur = payload.get("courant") or {}
            cmp = payload.get("compare") or {}

            # Current year
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

            # Same month last year (compare)
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
            continue

    df_current = pd.DataFrame(current_rows).sort_values("month")
    df_compare = pd.DataFrame(compare_rows).sort_values("month")
    return df_current, df_compare


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

    def first_existing(cands: List[str]) -> Any:
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

# ------------------------------------------------------------------------------
# Secrets & basic checks
# ------------------------------------------------------------------------------
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")
if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD secrets in Streamlit Cloud.")
    st.stop()

# Optional identifiers that some hydroqc billing flows use (from your invoice)
cust_id = st.secrets.get("HQ_CUSTOMER_ID")
acct_id = st.secrets.get("HQ_ACCOUNT_ID")
ctrt_id = st.secrets.get("HQ_CONTRACT_ID")

# ------------------------------------------------------------------------------
# UI: Tabs
# ------------------------------------------------------------------------------
tabs = st.tabs(["Usage", "Billing"])
usage_tab, billing_tab = tabs

# ------------------------------------------------------------------------------
# Usage tab controls
# ------------------------------------------------------------------------------
with usage_tab:
    st.subheader("Selection")
    granularity = st.radio("Granularity", ["Hourly", "Daily", "Monthly"], horizontal=True)
    today = date.today()
    col_date1, col_date2 = st.columns(2)
    with col_date1:
        start_date = st.date_input("Start date (daily)", value=today - timedelta(days=30), format="YYYY-MM-DD")
    with col_date2:
        end_date   = st.date_input("End date (daily)",   value=today, format="YYYY-MM-DD")

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
# Caching: hydroq-api client (resource) + usage data (data)
# ------------------------------------------------------------------------------
@st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec (usage)…")
def get_hydroqapi_client(_email: str, _password: str) -> HydroQuebec:
    """
    hydroq-api: simple wrapper for usage (hourly/daily/monthly).
    """
    client = HydroQuebec(_email, _password)
    client.login()
    return client

@st.cache_data(ttl=600, show_spinner="Fetching hourly usage…")
def fetch_hourly_df(_email: str, _password: str):
    client = get_hydroqapi_client(_email, _password)
    data = client.get_hourly_usage()
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching daily usage…")
def fetch_daily_df(_email: str, _password: str, start_iso: str, end_iso: str):
    client = get_hydroqapi_client(_email, _password)
    data = client.get_daily_usage(start_iso, end_iso)
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching monthly usage…")
def fetch_monthly_df(_email: str, _password: str):
    client = get_hydroqapi_client(_email, _password)
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

# ------------------------------------------------------------------------------
# Usage RUN block
# ------------------------------------------------------------------------------
with usage_tab:
    if run_clicked:
        if granularity == "Daily" and start_date > end_date:
            st.error("Start date must be before end date.")
            st.stop()

        try:
            # Ensure client is created; also confirms login credentials
            _ = get_hydroqapi_client(email, password)
            st.caption("Logged in (usage).")
        except requests.exceptions.HTTPError as http_err:
            show_http_error("Login (usage)", http_err)
            st.stop()
        except Exception as e:
            st.error(f"Login failed (usage): {e}")
            st.stop()

        try:
            if granularity == "Hourly":
                df = fetch_hourly_df(email, password)
                df = normalize_usage_df(df, "Hourly")

                st.subheader("Hourly usage (last 24h)")
                st.dataframe(df, use_container_width=True)
                if df.empty:
                    st.warning("No hourly data returned.")
                elif {"timestamp", "kwh"}.issubset(df.columns):
                    st.line_chart(df.set_index("timestamp")["kwh"])
                else:
                    st.info("Hourly data columns differ; showing normalized table above.")

            elif granularity == "Daily":
                df = fetch_daily_df(email, password, start_date.isoformat(), end_date.isoformat())
                df = normalize_usage_df(df, "Daily")

                st.subheader(f"Daily usage ({start_date} → {end_date})")
                st.dataframe(df, use_container_width=True)
                if df.empty:
                    st.warning("No daily data returned for the selected range.")
                elif {"date", "kwh"}.issubset(df.columns):
                    st.bar_chart(df.set_index("date")["kwh"])
                else:
                    st.info("Daily data columns differ; showing normalized table above.")

            else:  # Monthly
                df_api = fetch_monthly_df(email, password)

                with st.expander("Raw monthly API payload (first 10 rows)"):
                    st.write("Shape:", df_api.shape)
                    st.dataframe(df_api.head(10))

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
                    df = normalize_usage_df(df_api, "Monthly")
                    st.dataframe(df, use_container_width=True)
                    if df.empty:
                        st.warning("No monthly data returned.")
                    elif {"month", "kwh"}.issubset(df.columns):
                        st.bar_chart(df.set_index("month")["kwh"])
                    else:
                        st.info("Monthly data columns differ; showing normalized table above.")

        except requests.exceptions.HTTPError as http_err:
            show_http_error("Data retrieval (usage)", http_err)
        except Exception as e:
            st.exception(e)
    else:
        st.info("Select granularity and (for daily) a date range, then click ▶️ RUN.")

# ------------------------------------------------------------------------------
# Billing tab (balances & due dates) via hydroqc
# ------------------------------------------------------------------------------
with billing_tab:
    st.subheader("Balances & Due Dates (via hydroqc)")
    st.caption(
        "This tab uses the Hydro‑Quebec API Wrapper (`hydroqc`) for account & billing. "
        "Usage remains powered by `hydroq-api`."
    )

    @st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec (billing)…")
    def get_hydroqc_session(_email: str, _password: str):
        """
        Build a hydroqc session. Try multiple constructor signatures because they vary by version:
        - WebUser(email, password, verify_ssl=True)
        - WebUser(email, password)
        - HydroClient(email, password) or HydroClient() + login
        Attach customer/account/contract identifiers when available.
        """
        try:
            from hydroqc.webuser import WebUser  # type: ignore
            try:
                user = WebUser(_email, _password, True)  # newer builds: verify_ssl required
            except TypeError:
                user = WebUser(_email, _password)        # older builds
            user.login()
            # Attach identifiers if supported
            if cust_id and hasattr(user, "customer"):
                try: setattr(user, "customer", cust_id)
                except Exception: pass
            if acct_id and hasattr(user, "account"):
                try: setattr(user, "account", acct_id)
                except Exception: pass
            if ctrt_id and hasattr(user, "contract"):
                try: setattr(user, "contract", ctrt_id)
                except Exception: pass
            return user
        except Exception as e_webuser:
            try:
                from hydroqc.hydro_api.client import HydroClient  # type: ignore
                try:
                    client = HydroClient(_email, _password)
                except TypeError:
                    client = HydroClient()
                    if hasattr(client, "login"):
                        client.login(_email, _password)
                for name, value in [("customer", cust_id), ("account", acct_id), ("contract", ctrt_id)]:
                    if value and hasattr(client, name):
                        try: setattr(client, name, value)
                        except Exception: pass
                return client
            except Exception as e_client:
                raise RuntimeError(
                    f"Unable to initialize hydroqc session. "
                    f"WebUser error: {e_webuser}; HydroClient error: {e_client}"
                )

    def try_get_billing_summary(hq_obj: Any) -> Dict[str, Any]:
        """
        Attempt to fetch balance & due date across hydroqc versions.
        Probe common methods on root and nested modules (account/customer/contract/contracts).
        """
        candidates = [
            "get_balance", "get_billing_info", "get_current_invoice",
            "get_invoices", "billing_summary",
        ]

        # Direct calls on the root object
        for name in candidates:
            if hasattr(hq_obj, name):
                try:
                    return {"source": f"root.{name}", "raw": getattr(hq_obj, name)()}
                except Exception:
                    pass

        # Look into common containers
        for attr in ["account", "customer", "contract", "contracts"]:
            if hasattr(hq_obj, attr):
                obj = getattr(hq_obj, attr)
                if callable(obj):
                    try:
                        data = obj()
                        if isinstance(data, dict):
                            return {"source": f"{attr}()", "raw": data}
                    except Exception:
                        pass

                if isinstance(obj, (list, tuple)):
                    found = []
                    for i, elem in enumerate(obj):
                        for name in candidates:
                            if hasattr(elem, name):
                                try:
                                    data = getattr(elem, name)()
                                    found.append({"source": f"{attr}[{i}].{name}", "raw": data})
                                except Exception:
                                    pass
                    if found:
                        return {"source": f"{attr}[*]", "raw": found}
                else:
                    for name in candidates:
                        if hasattr(obj, name):
                            try:
                                return {"source": f"{attr}.{name}", "raw": getattr(obj, name)()}
                            except Exception:
                                pass

        return {"error": "No billing method found.", "dir": dir(hq_obj)}

    def normalize_billing_rows(raw: Any) -> pd.DataFrame:
        """
        Convert various shapes to rows with 'amount' and 'due_date' keys.
        """
        rows: List[Dict[str, Any]] = []

        def pick_amount_and_due(d: Dict[str, Any]) -> Dict[str, Any]:
            if not isinstance(d, dict): return {}
            lower = {k.lower(): k for k in d.keys()}
            amt_key = next((lower[k] for k in lower if ("amount" in k or "balance" in k)), None)
            due_key = next((lower[k] for k in lower if ("due" in k and "date" in k)), None)
            return {
                "amount": d.get(amt_key) if amt_key else None,
                "due_date": d.get(due_key) if due_key else None,
            }

        if isinstance(raw, dict):
            rows.append(pick_amount_and_due(raw))
        elif isinstance(raw, list):
            for elem in raw:
                if isinstance(elem, dict) and "raw" in elem:
                    val = elem["raw"]
                    if isinstance(val, dict):
                        rows.append(pick_amount_and_due(val))
                    elif isinstance(val, list):
                        for v in val:
                            if isinstance(v, dict): rows.append(pick_amount_and_due(v))
                elif isinstance(elem, dict):
                    rows.append(pick_amount_and_due(elem))

        return pd.DataFrame(rows)

    # UI: RUN button for billing
    col_b1, col_b2 = st.columns([1, 1])
    with col_b1:
        run_billing = st.button("▶️ RUN (Billing)")
    with col_b2:
        if st.button("🧹 Clear billing resource cache"):
            st.cache_resource.clear()
            st.success("Billing resource cache cleared.")

    if run_billing:
        try:
            hq_session = get_hydroqc_session(email, password)
            st.caption("Logged in (billing).")
        except Exception as e:
            st.error(f"hydroqc login failed: {e}")
            st.stop()

        summary = try_get_billing_summary(hq_session)

        if "error" in summary:
            st.error("Could not find billing methods on this hydroqc version.")
            with st.expander("Diagnostics"):
                st.write("Attributes/methods on session:", summary.get("dir"))
                st.info(
                    "Tip: Ensure the latest Hydro‑Quebec API Wrapper (`Hydro-Quebec-API-Wrapper`) is installed. "
                    "Some versions rely on customer/account/contract IDs (from your invoice)."
                )
        else:
            raw = summary.get("raw")
            st.write("Billing source:", summary.get("source"))
            st.write("Raw billing data:")
            st.json(raw)

            df_billing = normalize_billing_rows(raw)
            st.subheader("Balances & due dates (normalized)")
            if df_billing.empty:
                st.info("Could not normalize billing fields automatically. See raw data above.")
            else:
                st.dataframe(df_billing, use_container_width=True)

    st.markdown(
        """
        **Notes**  
        • `hydroq-api` focuses on consumption retrieval (hourly/daily/monthly).  
        • `hydroqc` provides broader account/customer/contract/webuser flows (used here for billing).  
        """
    )
