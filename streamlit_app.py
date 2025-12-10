
# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests  # for HTTPError handling
import json
from typing import Tuple, Any, Dict, List

# ------------------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------------------
st.set_page_config(page_title="Hydro‑Québec Usage & Billing", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage & Billing")

# ------------------------------------------------------------------------------
# Helpers: Monthly parsing + column normalization (from previous step)
# ------------------------------------------------------------------------------
def parse_monthly_rows_from_results(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse a DataFrame that has a 'results' column of JSON strings where each row contains:
      {
        "compare": { ... last year's month ... },
        "courant": { ... current month's data ... }
      }
    Returns (df_monthly_current, df_monthly_compare)
    """
    if df_raw is None or df_raw.empty or "results" not in df_raw.columns:
        return pd.DataFrame(), pd.DataFrame()

    current_rows = []
    compare_rows = []

    for _, row in df_raw.iterrows():
        try:
            payload = row["results"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            elif isinstance(payload, dict):
                pass
            else:
                continue

            cur = payload.get("courant") or {}
            cmp = payload.get("compare") or {}

            # Current
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

            # Compare (prior year)
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

# ------------------------------------------------------------------------------
# Secrets & basic checks
# ------------------------------------------------------------------------------
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")
if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD secrets in Streamlit Cloud.")
    st.stop()

# ------------------------------------------------------------------------------
# UI: Tabs
# ------------------------------------------------------------------------------
tabs = st.tabs(["Usage", "Billing"])
usage_tab, billing_tab = tabs

# ------------------------------------------------------------------------------
# Usage tab controls (same as before)
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
    This library focuses on consumption data; invoices are not documented here. [1](https://github.com/hydrohub2/Hydroq)[2](https://github.com/homas01123/HydroQ)
    """
    client = HydroQuebec(_email, _password)
    client.login()
    return client

@st.cache_data(ttl=600, show_spinner="Fetching hourly usage…")
def fetch_hourly_df():
    data = usage_client.get_hourly_usage()
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching daily usage…")
def fetch_daily_df(start_iso: str, end_iso: str):
    data = usage_client.get_daily_usage(start_iso, end_iso)
    return pd.DataFrame(data)

@st.cache_data(ttl=600, show_spinner="Fetching monthly usage…")
def fetch_monthly_df():
    data = usage_client.get_monthly_usage()
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
            usage_client = get_hydroqapi_client(email, password)
            st.caption("Logged in (usage).")
        except requests.exceptions.HTTPError as http_err:
            show_http_error("Login (usage)", http_err)
            st.stop()
        except Exception as e:
            st.error(f"Login failed (usage): {e}")
            st.stop()

        try:
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

            else:  # Monthly
                df_api = fetch_monthly_df()

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
        "This tab uses the Hydro‑Quebec API Wrapper (`hydroqc`) which exposes account/customer/web‑session "
        "flows beyond consumption. If your installed version differs, we’ll introspect available methods. "
        "Install: `pip install Hydro-Quebec-API-Wrapper`."
    )

    # Cache hydroqc web user login as a resource (like a session/client)
    @st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec (billing)…")
    def get_hydroqc_user(_email: str, _password: str) -> Any:
        """
        Create a hydroqc WebUser or client, log in, and return the object.

        hydroqc covers account, contract, customer, webuser modules. Exact public methods vary by version and
        are documented across the repo and docs. We use a resilient approach to try common patterns and
        introspect available attributes. 
        """
        try:
            # Most builds expose a WebUser interface for authenticated portal flows:
            from hydroqc.webuser import WebUser  # type: ignore
            user = WebUser(_email, _password)
            user.login()
            return user
        except Exception as e:
            # Fallback: some versions use a HydroClient (hydro_api.client)
            try:
                from hydroqc.hydro_api.client import HydroClient  # type: ignore
                client = HydroClient()
                # Depending on version, there may be async/await; if so, we’d adapt here.
                # For now, try a simple login flow or raise.
                if hasattr(client, "login"):
                    client.login(_email, _password)
                return client
            except Exception as e2:
                raise RuntimeError(
                    f"Unable to initialize hydroqc session. Make sure Hydro-Quebec-API-Wrapper is installed. "
                    f"Primary error: {e}; fallback error: {e2}"
                )

    # Try to extract billing summary using multiple likely methods/attributes
    def try_get_billing_summary(hq_obj: Any) -> Dict[str, Any]:
        """
        Attempt to fetch balance & due date(s) via hydroqc across versions.

        Strategy:
        1) Try common method names directly (get_balance, get_billing_info, get_current_invoice).
        2) Explore user/account/contract attributes and call candidate methods found there.
        3) Return a normalized dict if possible; else return diagnostics.
        """
        candidates = [
            ("get_balance", {}),
            ("get_billing_info", {}),
            ("get_current_invoice", {}),
            ("get_invoices", {}),
            ("billing_summary", {}),
        ]

        # Direct calls on the root object
        for name, kwargs in candidates:
            if hasattr(hq_obj, name):
                try:
                    data = getattr(hq_obj, name)(**kwargs)
                    return {"source": f"root.{name}", "raw": data}
                except Exception:
                    pass

        # Inspect attributes where billing commonly lives
        nested_attrs = ["account", "customer", "contract", "contracts"]
        for attr in nested_attrs:
            if hasattr(hq_obj, attr):
                obj = getattr(hq_obj, attr)
                # If it's callable (method returning data), call it
                if callable(obj):
                    try:
                        data = obj()
                        # If data itself has balance/amount/dueDate, return that
                        if isinstance(data, dict):
                            return {"source": f"{attr}()", "raw": data}
                    except Exception:
                        pass

                # Try methods inside the nested object
                for name, kwargs in candidates:
                    if hasattr(obj, name):
                        try:
                            data = getattr(obj, name)(**kwargs)
                            return {"source": f"{attr}.{name}", "raw": data}
                        except Exception:
                            pass

                # If iterable (e.g., contracts), iterate and try typical getters
                try:
                    if isinstance(obj, (list, tuple)):
                        summary_list: List[Dict[str, Any]] = []
                        for i, cobj in enumerate(obj):
                            for name, kwargs in candidates:
                                if hasattr(cobj, name):
                                    try:
                                        data = getattr(cobj, name)(**kwargs)
                                        summary_list.append({"contract_index": i, "source": f"{attr}[{i}].{name}", "raw": data})
                                    except Exception:
                                        pass
                        if summary_list:
                            return {"source": f"{attr}[*]", "raw": summary_list}
                except Exception:
                    pass

        # If nothing matched, provide diagnostics
        return {"error": "No billing method found on hydroqc object.", "dir": dir(hq_obj)}

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
            hq_user = get_hydroqc_user(email, password)
            st.caption("Logged in (billing).")
        except Exception as e:
            st.error(f"hydroqc login failed: {e}")
            st.stop()

        # Fetch summary in a resilient way
        summary = try_get_billing_summary(hq_user)
        if "error" in summary:
            st.error("Could not find billing methods on this hydroqc version.")
            with st.expander("Diagnostics"):
                st.write("Available attributes/methods:", summary.get("dir"))
                st.info(
                    "The hydroqc library exposes account/customer/contract/webuser modules and is the "
                    "recommended solution for account & billing data. If your installed version differs, "
                    "upgrading may expose helpers like account balance or invoice endpoints."
                )
        else:
            raw = summary.get("raw")
            st.write("Billing source:", summary.get("source"))
            st.write("Raw billing data:")
            st.json(raw)

            # Attempt to normalize common keys (amount & due date)
            # This covers typical schemas like: {'amount': 123.45, 'dueDate': '2025-12-28', ...}
            def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
                keys = {k.lower(): k for k in item.keys()}
                amount_key = next((item[k] for k in item.keys() if "amount" in k.lower() or "balance" in k.lower()), None)
                due_key    = next((k for k in item.keys() if "due" in k.lower() and "date" in k.lower()), None)
                return {
                    "amount": item.get(amount_key) if isinstance(amount_key, str) else amount_key,
                    "due_date": item.get(due_key),
                }

            rows: List[Dict[str, Any]] = []
            if isinstance(raw, dict):
                rows.append(normalize_item(raw))
            elif isinstance(raw, list):
                for elem in raw:
                    if isinstance(elem, dict) and "raw" in elem:
                        val = elem["raw"]
                        if isinstance(val, dict):
                            rows.append(normalize_item(val))
                        elif isinstance(val, list):
                            for v in val:
                                if isinstance(v, dict):
                                    rows.append(normalize_item(v))
                    elif isinstance(elem, dict):
                        rows.append(normalize_item(elem))

            df_billing = pd.DataFrame(rows)
            st.subheader("Balances & due dates (normalized)")
            if df_billing.empty:
                st.info("Could not normalize billing fields automatically. See raw data above.")
            else:
                st.dataframe(df_billing, use_container_width=True)

    # Info & references
    st.markdown(
        """
        **Notes**  
        • `hydroq-api` is designed for consumption retrieval and does not document invoice/billing endpoints.  
        • `hydroqc` (Hydro‑Quebec API Wrapper) exposes broader capabilities (account, customer, contract, webuser).  
        """,
    )
    st.caption(
        "References: hydroq-api on PyPI/GitHub; hydroqc on PyPI and docs."
    )
