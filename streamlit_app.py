
# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from hydroq_api import HydroQuebec
import requests
import json
from typing import Tuple, Any, Dict, List, Optional
import asyncio
import inspect
import threading

# ------------------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------------------
st.set_page_config(page_title="Hydro‑Québec Usage & Billing", page_icon="⚡", layout="wide")
st.title("⚡ Hydro‑Québec Usage & Billing")

# ------------------------------------------------------------------------------
# Helpers: Monthly parsing + column normalization (usage)
# ------------------------------------------------------------------------------
def parse_monthly_rows_from_results(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse DataFrame with 'results' JSON strings where each row contains:
      {
        "compare": {...},
        "courant": {...}
      }
    Returns (df_monthly_current, df_monthly_compare).
    """
    if df_raw is None or df_raw.empty or "results" not in df_raw.columns:
        return pd.DataFrame(), pd.DataFrame()

    curr_rows, cmp_rows = [], []
    for _, row in df_raw.iterrows():
        try:
            payload = row["results"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            elif not isinstance(payload, dict):
                continue

            cur = payload.get("courant") or {}
            cmp = payload.get("compare") or {}

            # current
            cur_start = cur.get("dateDebutMois")
            cur_end   = cur.get("dateFinMois")
            cur_total = cur.get("consoTotalMois")
            cur_avg   = cur.get("moyenneKwhJourMois")
            cur_temp  = cur.get("tempMoyenneMois")
            if cur_start and cur_total is not None:
                m = pd.to_datetime(cur_start).to_period("M").strftime("%Y-%m")
                curr_rows.append({
                    "month": m, "kwh": cur_total, "start_date": cur_start, "end_date": cur_end,
                    "avg_kwh_per_day": cur_avg, "avg_temp": cur_temp
                })

            # compare
            cmp_start = cmp.get("dateDebutMois")
            cmp_end   = cmp.get("dateFinMois")
            cmp_total = cmp.get("consoTotalMois")
            cmp_avg   = cmp.get("moyenneKwhJourMois")
            cmp_temp  = cmp.get("tempMoyenneMois")
            if cmp_start and cmp_total is not None:
                m = pd.to_datetime(cmp_start).to_period("M").strftime("%Y-%m")
                cmp_rows.append({
                    "month": m, "kwh": cmp_total, "start_date": cmp_start, "end_date": cmp_end,
                    "avg_kwh_per_day": cmp_avg, "avg_temp": cmp_temp
                })
        except Exception:
            continue

    return (pd.DataFrame(curr_rows).sort_values("month"),
            pd.DataFrame(cmp_rows).sort_values("month"))


def normalize_usage_df(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Normalize Hydro‑Québec usage columns for charting."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df.rename(columns={c: c.lower() for c in df.columns}, inplace=True)

    kwh_candidates = ["kwh", "kw_h", "valuekwh", "consumption", "energy", "valeur", "value"]
    date_candidates_monthly = ["month", "periode", "period", "date", "mois"]
    date_candidates_daily   = ["date", "jour", "day", "periode", "period"]
    ts_candidates_hourly    = ["timestamp", "time", "datetime", "heure", "period", "periode"]

    def pick(cols: List[str]) -> Optional[str]:
        for c in cols:
            if c in df.columns:
                return c
        return None

    if granularity == "Monthly":
        d = pick(date_candidates_monthly); v = pick(kwh_candidates)
        if d and d != "month": df.rename(columns={d: "month"}, inplace=True)
        if v and v != "kwh":   df.rename(columns={v: "kwh"},   inplace=True)
        if "month" in df.columns:
            try: df["month"] = pd.to_datetime(df["month"]).dt.to_period("M").astype(str)
            except Exception: pass

    elif granularity == "Daily":
        d = pick(date_candidates_daily); v = pick(kwh_candidates)
        if d and d != "date": df.rename(columns={d: "date"}, inplace=True)
        if v and v != "kwh":  df.rename(columns={v: "kwh"},  inplace=True)
        if "date" in df.columns:
            try: df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
            except Exception: pass

    elif granularity == "Hourly":
        t = pick(ts_candidates_hourly); v = pick(kwh_candidates)
        if t and t != "timestamp": df.rename(columns={t: "timestamp"}, inplace=True)
        if v and v != "kwh":       df.rename(columns={v: "kwh"},       inplace=True)
        if "timestamp" in df.columns:
            try: df["timestamp"] = pd.to_datetime(df["timestamp"])
            except Exception: pass

    return df

# ------------------------------------------------------------------------------
# Secrets
# ------------------------------------------------------------------------------
email = st.secrets.get("HQ_EMAIL")
password = st.secrets.get("HQ_PASSWORD")
if not email or not password:
    st.error("Missing HQ_EMAIL / HQ_PASSWORD in Secrets.")
    st.stop()

# Optional IDs (digits only, no spaces); some hydroqc calls use them
cust_id = st.secrets.get("HQ_CUSTOMER_ID")
acct_id = st.secrets.get("HQ_ACCOUNT_ID")
ctrt_id = st.secrets.get("HQ_CONTRACT_ID")

# ------------------------------------------------------------------------------
# UI: Tabs
# ------------------------------------------------------------------------------
usage_tab, billing_tab = st.tabs(["Usage", "Billing"])

# ------------------------------------------------------------------------------
# Usage tab (unchanged, via hydroq-api)
# ------------------------------------------------------------------------------
with usage_tab:
    st.subheader("Selection")
    granularity = st.radio("Granularity", ["Hourly", "Daily", "Monthly"], horizontal=True)
    today = date.today()
    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Start date (daily)", value=today - timedelta(days=30), format="YYYY-MM-DD")
    with c2:
        end_date   = st.date_input("End date (daily)",   value=today, format="YYYY-MM-DD")

    ca, cb, cc = st.columns(3)
    with ca:
        run_clicked = st.button("▶️ RUN")
    with cb:
        if st.button("🧹 Clear data cache"):
            st.cache_data.clear()
            st.success("Data cache cleared.")
    with cc:
        if st.button("🧹 Clear resource cache"):
            st.cache_resource.clear()
            st.success("Resource cache cleared.")

@st.cache_resource(ttl="1h", show_spinner="Connecting to Hydro‑Québec (usage)…")
def get_hydroqapi_client(_email: str, _password: str) -> HydroQuebec:
    client = HydroQuebec(_email, _password)
    client.login()
    return client

@st.cache_data(ttl=600, show_spinner="Fetching hourly usage…")
def fetch_hourly_df(_email: str, _password: str):
    client = get_hydroqapi_client(_email, _password)
    return pd.DataFrame(client.get_hourly_usage())

@st.cache_data(ttl=600, show_spinner="Fetching daily usage…")
def fetch_daily_df(_email: str, _password: str, start_iso: str, end_iso: str):
    client = get_hydroqapi_client(_email, _password)
    return pd.DataFrame(client.get_daily_usage(start_iso, end_iso))

@st.cache_data(ttl=600, show_spinner="Fetching monthly usage…")
def fetch_monthly_df(_email: str, _password: str):
    client = get_hydroqapi_client(_email, _password)
    return pd.DataFrame(client.get_monthly_usage())

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
    st.error(f"{prefix} failed. Status: {status or 'unknown'}.")

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
                df = normalize_usage_df(fetch_hourly_df(email, password), "Hourly")
                st.subheader("Hourly usage (last 24h)")
                st.dataframe(df, use_container_width=True)
                if df.empty:
                    st.warning("No hourly data returned.")
                elif {"timestamp", "kwh"}.issubset(df.columns):
                    st.line_chart(df.set_index("timestamp")["kwh"])
                else:
                    st.info("Hourly data columns differ; showing normalized table above.")

            elif granularity == "Daily":
                df = normalize_usage_df(fetch_daily_df(email, password, start_date.isoformat(), end_date.isoformat()), "Daily")
                st.subheader(f"Daily usage ({start_date} → {end_date})")
                st.dataframe(df, use_container_width=True)
                if df.empty:
                    st.warning("No daily data for the selected range.")
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
                    t1, t2 = st.tabs(["Current year", "Same month last year"])
                    with t1:
                        st.dataframe(df_current, use_container_width=True)
                    with t2:
                        st.dataframe(df_compare, use_container_width=True)
                    if df_current.empty:
                        st.warning("No monthly data parsed.")
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
# Billing tab — fresh hydroqc session per RUN + customer ID selection
# ------------------------------------------------------------------------------
with billing_tab:
    st.subheader("Balances & Due Dates (via hydroqc)")
    st.caption(
        "A fresh `hydroqc` session is created on each RUN because Streamlit does not support caching async objects. "
        "If you update Secrets or dependencies, simply click RUN again."
    )

    # Async helpers
    def run_coro(coro):
        """Run a coroutine safely and propagate exceptions."""
        try:
            return asyncio.run(coro)
        except RuntimeError:
            result_container, error_container = {}, {}
            def runner():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result_container["result"] = loop.run_until_complete(coro)
                except Exception as e:
                    error_container["error"] = e
                finally:
                    loop.close()
            t = threading.Thread(target=runner)
            t.start(); t.join()
            if "error" in error_container:
                raise error_container["error"]
            return result_container.get("result")

    def call_hydroqc_once(obj: Any, method_name: str) -> Any:
        """Inspect signature, pass IDs if present, and await exactly once if needed."""
        if not hasattr(obj, method_name):
            raise AttributeError(f"{method_name} not found on session.")
        m = getattr(obj, method_name)
        sig = inspect.signature(m); kwargs = {}
        for pname in sig.parameters.keys():
            p = pname.lower()
            if p in {"customer", "customer_id"} and cust_id: kwargs[pname] = cust_id
            if p in {"account", "account_id"} and acct_id: kwargs[pname] = acct_id
            if p in {"contract", "contract_id"} and ctrt_id: kwargs[pname] = ctrt_id
            if p in {"verify_ssl"}: kwargs[pname] = True
        res = m(**kwargs) if sig.parameters else m()
        return run_coro(res) if inspect.iscoroutine(res) else res

    # Build a fresh hydroqc session each RUN (no cache)
    def new_hydroqc_session(_email: str, _password: str):
        try:
            from hydroqc.webuser import WebUser  # type: ignore
            try:
                user = WebUser(_email, _password, True)  # some builds require verify_ssl
            except TypeError:
                user = WebUser(_email, _password)
            login_res = user.login()
            if inspect.iscoroutine(login_res):
                run_coro(login_res)
            # Attach IDs if attributes exist
            for name, value in [("customer", cust_id), ("account", acct_id), ("contract", ctrt_id)]:
                if value and hasattr(user, name):
                    try:
                        setattr(user, name, value)
                    except Exception:
                        pass
            return user
        except Exception as e_webuser:
            from hydroqc.hydro_api.client import HydroClient  # type: ignore
            try:
                try:
                    client = HydroClient(_email, _password)
                except TypeError:
                    client = HydroClient()
                    if hasattr(client, "login"):
                        login_res = client.login(_email, _password)
                        if inspect.iscoroutine(login_res):
                            run_coro(login_res)
                for name, value in [("customer", cust_id), ("account", acct_id), ("contract", ctrt_id)]:
                    if value and hasattr(client, name):
                        try:
                            setattr(client, name, value)
                        except Exception:
                            pass
                return client
            except Exception as e_client:
                raise RuntimeError(f"hydroqc session init failed: WebUser error: {e_webuser}; HydroClient error: {e_client}")

    # UI: RUN (Billing)
    cba, cbb = st.columns([1, 1])
    with cba:
        run_billing = st.button("▶️ RUN (Billing)")
    with cbb:
        if st.button("ℹ️ Runtime info"):
            import sys, importlib
            st.write("Python:", sys.version)
            try:
                import hydroqc
                st.write("hydroqc version:", getattr(hydroqc, "__version__", "unknown"))
                st.write("has WebUser:", bool(importlib.util.find_spec("hydroqc.webuser")))
            except Exception as e:
                st.write("hydroqc import error:", e)

    if run_billing:
        # 1) New session (fresh each run)
        try:
            hq_session = new_hydroqc_session(email, password)
            st.caption("Logged in (billing). Fresh session created.")
        except Exception as e:
            st.error(f"hydroqc login failed: {e}")
            st.stop()

        # 2) Fetch customers from the portal (async-aware)
        try:
            customers_raw = call_hydroqc_once(hq_session, "fetch_customers_info")
        except Exception as e:
            st.error(f"fetch_customers_info error: {e}")
            customers_raw = None

        # Show raw customers payload (verify fields)
        with st.expander("fetch_customers_info (raw)"):
            if isinstance(customers_raw, (dict, list)):
                st.json(customers_raw)
            else:
                st.write(type(customers_raw).__name__, customers_raw)

        # 3) Extract candidate customer IDs from the portal payload
        def find_customer_ids(obj) -> List[Dict[str, str]]:
            """
            Search dict/list for likely customer identifiers and labels.
            Returns [{'id': '...', 'label': '...'}].
            """
            pool: List[Dict[str, str]] = []

            id_keys = {
                "customerid", "idcustomer", "idclient", "customer", "client",
                "numeroclient", "numclient", "noclient", "numero", "id"
            }
            name_keys = {"name", "firstname", "first_name", "lastname", "last_name"}

            def norm(s: str) -> str:
                return s.replace("_", "").replace("-", "").lower()

            def walk(x):
                if isinstance(x, dict):
                    lk = {norm(k): k for k in x.keys()}
                    id_key = next((lk[k] for k in id_keys if k in lk), None)
                    if id_key:
                        cid = str(x.get(id_key, "")).strip()
                        parts = []
                        for k in name_keys:
                            if k in lk and x.get(lk[k]):
                                parts.append(str(x.get(lk[k])))
                        label = " ".join(parts) if parts else cid
                        if cid:
                            pool.append({"id": cid, "label": label})
                    # continue walking
                    for v in x.values():
                        walk(v)
                elif isinstance(x, (list, tuple)):
                    for v in x:
                        walk(v)

            walk(obj)
            # de-duplicate
            uniq = {}
            for item in pool:
                uniq[item["id"]] = item["label"]
            return [{"id": k, "label": v} for k, v in uniq.items()]

        candidates = find_customer_ids(customers_raw) if customers_raw else []
        if not candidates:
            st.warning("No customer IDs returned by the portal. If this persists, verify your login and try again later.")
            st.stop()

        # 4) Choose the portal-provided customer ID
        st.subheader("Choose customer ID from the portal")
        chosen_label = st.selectbox(
            "Customer",
            options=[c["label"] for c in candidates],
            index=0
        )
        chosen_id = next(c["id"] for c in candidates if c["label"] == chosen_label)

        # 5) Call get_customer(customer_id=chosen_id)
        try:
            m = getattr(hq_session, "get_customer")
            res = m(customer_id=chosen_id)
            raw_customer = run_coro(res) if inspect.iscoroutine(res) else res
        except Exception as e:
            st.error(f"get_customer({chosen_id}) error: {e}")
            raw_customer = None

        with st.expander("get_customer (raw)"):
            if isinstance(raw_customer, (dict, list)):
                st.json(raw_customer)
            else:
                st.write(type(raw_customer).__name__, raw_customer)

        # 6) Normalize common billing keys into Amount/Due date table
        def deep_find_items(obj) -> List[Dict[str, Any]]:
            found: List[Dict[str, Any]] = []
            amount_keys = {
                "amount", "balance", "solde", "montant", "montantfacture", "montantsolde",
                "prochainmontant", "total", "totalfacture"
            }
            due_keys    = {"duedate", "dateecheance", "echeance", "prochaineecheance", "date_due", "date_limite"}
            def norm(k: str) -> str: return k.replace("_", "").replace("-", "").lower()
            def walk(x):
                if isinstance(x, dict):
                    lower = {norm(k): k for k in x}
                    amt_k = next((lower[k] for k in lower if k in amount_keys), None)
                    due_k = next((lower[k] for k in lower if k in due_keys), None)
                    if amt_k or due_k:
                        rec = {"amount": x.get(amt_k) if amt_k else None,
                               "due_date": x.get(due_k) if due_k else None}
                        for id_key in ["contract","numeroContrat","contractId","account","compte","accountId","customer","client","customerId"]:
                            if id_key in x: rec[id_key] = x.get(id_key)
                        found.append(rec)
                    for v in x.values(): walk(v)
                elif isinstance(x, (list, tuple)):
                    for v in x: walk(v)
            walk(obj)
            return found

        rows = []
        for raw in [customers_raw, raw_customer]:
            if isinstance(raw, (dict, list)):
                rows += deep_find_items(raw)
            elif isinstance(raw, str):
                try:
                    rows += deep_find_items(json.loads(raw))
                except Exception:
                    pass

        df_billing = pd.DataFrame(rows).drop_duplicates() if rows else pd.DataFrame()
        if "amount" in df_billing.columns:
            try:
                df_billing["amount"] = pd.to_numeric(df_billing["amount"], errors="coerce")
            except Exception:
                pass
        if "due_date" in df_billing.columns:
            try:
                df_billing["due_date"] = pd.to_datetime(df_billing["due_date"], errors="coerce")
                df_billing = df_billing.sort_values("due_date", na_position="last")
            except Exception:
                pass

        st.subheader("Balances & due dates (normalized)")
        if df_billing.empty:
            st.info("No billing fields found yet. If you selected a customer ID and still see no details, try another customer entry or retry later.")
        else:
            st.dataframe(df_billing, use_container_width=True)
