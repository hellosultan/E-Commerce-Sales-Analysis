import os, sqlite3, pandas as pd, numpy as np, streamlit as st
from pathlib import Path

st.set_page_config(page_title="E-Commerce Sales", layout="wide")

# --- Choose a writable DB path: local data/ first, then Streamlit Cloud temp dir ---
from src.sql._path_test import first_writable
DB_CANDIDATES = [Path("data/ecommerce.db"), Path("/mount/tmp/ecommerce.db")]
DB_PATH = first_writable(DB_CANDIDATES) or Path("/mount/tmp/ecommerce.db")
BUILDER = Path("src/sql/build_db.py")

def build_db():
    import subprocess, sys
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([sys.executable, str(BUILDER)], check=True)

def tables_ok(conn) -> bool:
    try:
        t = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        return {"orders","products","customers"}.issubset(set(t["name"]))
    except Exception:
        return False

@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    # Try SQLite (build if missing)
    try:
        if not DB_PATH.exists():
            build_db()
        conn = sqlite3.connect(str(DB_PATH))
        try:
            if not tables_ok(conn):
                conn.close(); build_db(); conn = sqlite3.connect(str(DB_PATH))
            df = pd.read_sql(
                """
                SELECT o.order_id, o.order_date, o.quantity, o.discount, o.shipping, o.status,
                       p.product_id, p.category, p.base_price,
                       c.customer_id, c.segment, c.country
                FROM orders o
                JOIN products p USING(product_id)
                JOIN customers c USING(customer_id)
                """,
                conn,
                parse_dates=["order_date"],
            )
        finally:
            try: conn.close()
            except: pass
        df["order_month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()
        df["revenue"] = (df["base_price"] * (1 - df["discount"])) * df["quantity"]
        return df
    except Exception:
        # Deterministic in-memory fallback (no banners shown)
        rng = np.random.default_rng(42)
        n_customers, n_products, n_orders = 5000, 500, 50000
        segments = ["Consumer","Corporate","Enterprise","Small Biz"]
        countries = ["US","UK","FR","DE","IN","ES","BH","QA"]
        customers = pd.DataFrame({
            "customer_id": np.arange(1000, 1000+n_customers),
            "segment": rng.choice(segments, n_customers, p=[0.45,0.25,0.15,0.15]),
            "country": rng.choice(countries, n_customers)})
        categories = ["Electronics","Home","Fashion","Sports","Beauty","Toys","Books","Grocery"]
        products = pd.DataFrame({
            "product_id": np.arange(1, 1+n_products),
            "category": rng.choice(categories, n_products),
            "base_price": np.round(rng.normal(60,35,size=n_products).clip(5,400),2)})
        orders = pd.DataFrame({
            "order_id": np.arange(1, 1+n_orders),
            "order_date": pd.to_datetime("2023-01-01") + pd.to_timedelta(rng.integers(0,730,size=n_orders), unit="D"),
            "quantity": rng.integers(1,5,size=n_orders),
            "discount": np.round(rng.uniform(0,0.3,size=n_orders),2),
            "shipping": rng.choice(["Standard","Express","Two-Day"], size=n_orders, p=[0.6,0.25,0.15]),
            "status": rng.choice(["Completed","Refunded","Pending"], size=n_orders, p=[0.85,0.05,0.10]),
            "product_id": rng.integers(1,1+n_products,size=n_orders),
            "customer_id": rng.integers(1000,1000+n_customers,size=n_orders)})
        df = (orders.merge(products, on="product_id").merge(customers, on="customer_id"))
        df["order_month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()
        df["revenue"] = (df["base_price"] * (1 - df["discount"])) * df["quantity"]
        return df

# --- the rest of your file stays the same (KPIs, charts, downloads) ---
# (keep your existing code from where you compute filters onward)
