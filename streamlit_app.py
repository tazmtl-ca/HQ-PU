
# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests  # HTTPError handling
import json
from typing import Tuple, Any, Dict, List, Optional

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
    Returns (df_monthly_current, df_monthly_compare).
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

    def first_existing(cands: List[str]) -> Optional[str]:
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

# Optional identifiers used by some hydroqc billing flows (from your invoice)
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
# Billing tab (balances & due dates) via hydroqc — robust JSON coercion & flexible calls
# ------------------------------------------------------------------------------
with billing_tab:
    st.subheader("Balances & Due Dates (via hydroqc)")
    st.caption(
        "This tab uses the Hydro‑Quebec API Wrapper (`hydroqc`) for account & billing. "
        "Usage remains powered by `hydroq-api` (consumption only)."
    )

    @st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec (billing)…")
    def get_hydroqc_session(_email: str, _password: str):
        """
        Try:
          WebUser(email, password, True) → WebUser(email, password) → HydroClient(email, password) → HydroClient() + login.
        Attach customer/account/contract identifiers when supported by the object.
        """
        try:
            from hydroqc.webuser import WebUser  # type: ignore
            try:
                user = WebUser(_email, _password, True)  # some builds require verify_ssl
            except TypeError:
                user = WebUser(_email, _password)
            user.login()
            for name, value in [("customer", cust_id), ("account", acct_id), ("contract", ctrt_id)]:
                if value and hasattr(user, name):
                    try: setattr(user, name, value)
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
                    f"Unable to initialize hydroqc session. WebUser error: {e_webuser}; HydroClient error: {e_client}"
                )

    # --- Utilities: coercion & flexible method caller ---
    import inspect
    def coerce_to_jsonlike(x: Any) -> Any:
        """Best effort: return dict/list if possible; else return the original for st.write()."""
        if x is None:
            return None
        # If it's already a dict/list/tuple, OK
        if isinstance(x, (dict, list, tuple)):
            return x
        # String that might be JSON
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                return x  # not JSON; show raw string
        # If object has a __dict__, use it
        if hasattr(x, "__dict__"):
            try:
                d = vars(x).copy()
                # Avoid recursive issues; drop callables
                for k, v in list(d.items()):
                    if callable(v):
                        d.pop(k, None)
                return d
            except Exception:
                pass
        # Fallback: return original; st.write will show type/value
        return x

    def flexible_call(obj: Any, method_name: str) -> Any:
        """
        Call obj.method_name with invoice identifiers if the signature supports them.
        Tries (no args) first, then kwargs from known names.
        """
        if not hasattr(obj, method_name):
            return None
        m = getattr(obj, method_name)
        try:
            return m()  # no-arg call
        except TypeError:
            try:
                sig = inspect.signature(m)
                kwargs = {}
                # Map our secrets to common parameter names
                for pname in sig.parameters.keys():
                    p = pname.lower()
                    if p in {"customer", "customer_id"} and cust_id: kwargs[pname] = cust_id
                    if p in {"account", "account_id"} and acct_id: kwargs[pname] = acct_id
                    if p in {"contract", "contract_id"} and ctrt_id: kwargs[pname] = ctrt_id
                    if p in {"verify_ssl"}: kwargs[pname] = True
                return m(**kwargs)
            except Exception:
                return None

    def deep_find_items(obj) -> List[Dict[str, Any]]:
        """
        Recursively walk dict/list and extract candidate billing dicts that contain
        amount/balance and due date. Supports English & French keys commonly seen.
        """
        found: List[Dict[str, Any]] = []
        amount_keys = {"amount", "balance", "solde", "montant", "montantfacture", "montantsolde", "prochainmontant", "total", "totalfacture"}
        due_keys    = {"duedate", "dateecheance", "echeance", "prochaineecheance", "date_due", "date_limite"}

        def normkey(k: str) -> str:
            return k.replace("_", "").replace("-", "").lower()

        def walk(x):
            if isinstance(x, dict):
                lower = {normkey(k): k for k in x.keys()}
                amt_k = next((lower[k] for k in lower if k in amount_keys), None)
                due_k = next((lower[k] for k in lower if k in due_keys), None)
                if amt_k or due_k:
                    rec = {
                        "amount": x.get(amt_k) if amt_k else None,
                        "due_date": x.get(due_k) if due_k else None,
                    }
                    # capture identifiers if present
                    for id_key in ["contract", "numeroContrat", "contractId", "account", "compte", "accountId", "customer", "client", "customerId"]:
                        if id_key in x:
                            rec[id_key] = x.get(id_key)
                    found.append(rec)
                for v in x.values(): walk(v)
            elif isinstance(x, (list, tuple)):
                for v in x: walk(v)

        walk(obj)
        return found

    def normalize_billing(raws: List[Any]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for raw in raws:
            j = coerce_to_jsonlike(raw)
            rows.extend(deep_find_items(j))
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).drop_duplicates()
        # numeric amount
        if "amount" in df.columns:
            try: df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            except Exception: pass
        # sort by due_date
        if "due_date" in df.columns:
            try:
                df["due_date"] = pd.to_datetime(df["due_date"], errors="coerce")
                df = df.sort_values("due_date", na_position="last")
            except Exception: pass
        return df

    # --- Billing Run UI ---
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

        # Prefer methods your session exposes
        # (based on your attribute list: check_hq_portal_status, get_info, fetch_customers_info, get_customer)
        portal_status = flexible_call(hq_session, "check_hq_portal_status")
        raw_info      = flexible_call(hq_session, "get_info")
        raw_customers = flexible_call(hq_session, "fetch_customers_info")
        raw_customer1 = flexible_call(hq_session, "get_customer")  # sometimes singular record

        # Show raw values safely (write, not json)
        with st.expander("Portal status"):
            st.write(type(portal_status).__name__, portal_status)
        with st.expander("get_info (raw)"):
            st.write(type(raw_info).__name__, raw_info)
        with st.expander("fetch_customers_info (raw)"):
            st.write(type(raw_customers).__name__, raw_customers)
        with st.expander("get_customer (raw)"):
            st.write(type(raw_customer1).__name__, raw_customer1)
        with st.expander("customers property (raw)"):
            st.write(type(getattr(hq_session, "customers", None)).__name__, getattr(hq_session, "customers", None))

        df_billing = normalize_billing([portal_status, raw_info, raw_customers, raw_customer1, getattr(hq_session, "customers", None)])

        st.subheader("Balances & due dates (normalized)")
        if df_billing.empty:
            st.info("Could not normalize billing fields automatically. Try clicking each expander above to inspect the raw values. "
                    "If you see exact keys for amount/due date (e.g., 'solde', 'montantFacture', 'dateEcheance'), tell me and I'll add them.")
        else:
            st.dataframe(df_billing, use_container_width=True)

    st.markdown(
        """
        **Notes**  
        • `st.json` renders valid JSON (dict/list or JSON strings). If a method returns a non‑JSON object,
          prefer `st.write` or coerce to a dict first. [1](https://docs.streamlit.io/develop/api-reference/data/st.json)  
        • `hydroq-api` focuses on consumption (hourly/daily/monthly). [2](https://docs.streamlit.io/develop/api-reference/status/st.error)  
        • `hydroqc` can return general account info via the portal (balance, billing period projection); shapes vary by version. [3](https://github.com/homas01123/HydroQ)[4](https://github.com/hydrohub2/Hydroq)
        """
    )
