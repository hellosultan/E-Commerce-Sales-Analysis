import sqlite3, pandas as pd, numpy as np, streamlit as st
from pathlib import Path

st.set_page_config(page_title="E-Commerce Sales", layout="wide")

# ---------- Pick a writable DB path (local data/ first, then Streamlit Cloud temp) ----------
def first_writable(candidates):
    for p in candidates:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            test = p.parent / ".write_test"
            test.write_text("ok")
            test.unlink(missing_ok=True)
            return p
        except Exception:
            continue
    return candidates[-1]

DB_PATH = first_writable([Path("data/ecommerce.db"), Path("/mount/tmp/ecommerce.db")])
BUILDER = Path("src/sql/build_db.py")

# -------- CSV Upload Helpers --------
def _normalize_cols(df):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def load_uploaded(file) -> pd.DataFrame:
    df = pd.read_csv(file)
    df = _normalize_cols(df)

    required = {"order_id", "order_date", "quantity", "unit_price"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns: {required - set(df.columns)}")

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["revenue"] = df["quantity"] * df["unit_price"]
    return df
# ---------- Helpers ----------
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

def generate_synthetic_df(n_customers=5000, n_products=500, n_orders=50000) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    segments = ["Consumer","Corporate","Enterprise","Small Biz"]
    countries = ["US","UK","FR","DE","IN","ES","BH","QA"]
    customers = pd.DataFrame({
        "customer_id": np.arange(1000, 1000+n_customers),
        "segment": rng.choice(segments, n_customers, p=[0.45,0.25,0.15,0.15]),
        "country": rng.choice(countries, n_customers)
    })
    categories = ["Electronics","Home","Fashion","Sports","Beauty","Toys","Books","Grocery"]
    products = pd.DataFrame({
        "product_id": np.arange(1, 1+n_products),
        "category": rng.choice(categories, n_products),
        "base_price": np.round(rng.normal(60,35,size=n_products).clip(5,400), 2)
    })
    orders = pd.DataFrame({
        "order_id": np.arange(1, 1+n_orders),
        "order_date": pd.to_datetime("2023-01-01") + pd.to_timedelta(rng.integers(0,730,size=n_orders), unit="D"),
        "quantity": rng.integers(1,5,size=n_orders),
        "discount": np.round(rng.uniform(0,0.3,size=n_orders),2),
        "shipping": rng.choice(["Standard","Express","Two-Day"], size=n_orders, p=[0.6,0.25,0.15]),
        "status": rng.choice(["Completed","Refunded","Pending"], size=n_orders, p=[0.85,0.05,0.10]),
        "product_id": rng.integers(1,1+n_products,size=n_orders),
        "customer_id": rng.integers(1000,1000+n_customers,size=n_orders)
    })
    df = orders.merge(products, on="product_id").merge(customers, on="customer_id")
    df["order_month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()
    df["revenue"] = (df["base_price"] * (1 - df["discount"])) * df["quantity"]
    return df

@st.cache_data(show_spinner=True)
def load_data() -> pd.DataFrame:
    # Try SQLite build + read; if anything fails, fall back to in-memory synthetic data
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
        return generate_synthetic_df()

df = load_data()

# =================== UI ===================
with st.sidebar:
    st.header("Filters")

    # ---- CSV Upload ----
    uploaded_file = st.file_uploader("Upload your sales CSV", type=["csv"])
    if uploaded_file:
        try:
            df = load_uploaded(uploaded_file)
            st.success("✅ CSV uploaded and loaded successfully!")
        except Exception as e:
            st.error(f"⚠️ Error processing file: {e}")
            df = load_data()  # fallback
    else:
        df = load_data()  # default dataset (SQLite/synthetic)

    # ---- Filters ----
    min_d, max_d = df["order_date"].min().date(), df["order_date"].max().date()
    dr = st.date_input("Date range", value=(min_d, max_d))
    if isinstance(dr, tuple) and len(dr) == 2:
        df = df[(df["order_date"].dt.date >= dr[0]) & (df["order_date"].dt.date <= dr[1])]

    status    = st.multiselect("Status",   df["status"].unique().tolist(),   default=["Completed"])
    segments  = st.multiselect("Segment",  df["segment"].unique().tolist(),  default=df["segment"].unique().tolist())
    categories= st.multiselect("Category", df["category"].unique().tolist(), default=df["category"].unique().tolist())
    countries = st.multiselect("Country",  df["country"].unique().tolist(),  default=df["country"].unique().tolist())

# (outside the sidebar)
mask = (
    df["status"].isin(status)
    & df["segment"].isin(segments)
    & df["category"].isin(categories)
    & df["country"].isin(countries)
)
d = df[mask].copy()
completed = d["status"].eq("Completed")
refunded  = d["status"].eq("Refunded")

# Allow user to download filtered dataset
st.download_button(
    label="⬇️ Download filtered data as CSV",
    data=d.to_csv(index=False).encode("utf-8"),
    file_name="filtered_sales.csv",
    mime="text/csv",
)

# KPIs
revenue = d.loc[completed, "revenue"].sum()
orders  = len(d)
aov     = d.loc[completed, "revenue"].mean() if completed.any() else 0.0
completion_rate = float(completed.mean() * 100)
refund_rate     = float(refunded.mean() * 100)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Revenue", f"${revenue:,.0f}")
c2.metric("Orders",  f"{orders:,}")
c3.metric("AOV",     f"${aov:,.2f}")
c4.metric("Completion Rate", f"{completion_rate:.1f}%")
c5.metric("Refund Rate",     f"{refund_rate:.1f}%")

# Charts
st.subheader("Revenue Over Time")
st.line_chart(d.loc[completed].groupby("order_month")["revenue"].sum())

st.subheader("Top Categories")
st.bar_chart(d.loc[completed].groupby("category")["revenue"].sum().sort_values(ascending=False).head(10))

st.subheader("Segments by Revenue")
st.bar_chart(d.loc[completed].groupby("segment")["revenue"].sum().sort_values(ascending=False))

# Table
st.subheader("Filtered Orders (sample)")
st.dataframe(d.head(500), use_container_width=True)

# Downloads
@st.cache_data
def to_csv_bytes(df_in: pd.DataFrame) -> bytes:
    return df_in.to_csv(index=False).encode("utf-8")

completed_d = d[d["status"] == "Completed"]
monthly_kpis = completed_d.groupby("order_month").agg(
    revenue=("revenue","sum"),
    orders=("order_id","count"),
    aov=("revenue","mean"),
).reset_index()

rates = d.groupby("order_month")["status"].value_counts(normalize=True).unstack(fill_value=0.0)
for col in ["Completed", "Refunded"]:
    if col not in rates.columns:
        rates[col] = 0.0
rates = rates[["Completed", "Refunded"]]

monthly_kpis = monthly_kpis.merge(
    rates.mul(100).reset_index()[["order_month", "Completed", "Refunded"]],
    on="order_month", how="left"
).rename(columns={"Completed": "completion_rate", "Refunded": "refund_rate"}).fillna(0.0)

dl1, dl2 = st.columns(2)
with dl1:
    st.download_button("Download filtered orders (CSV)",
                       data=to_csv_bytes(d), file_name="filtered_orders.csv", mime="text/csv")
with dl2:
    st.download_button("Download monthly KPIs (CSV)",
                       data=to_csv_bytes(monthly_kpis), file_name="monthly_kpis.csv", mime="text/csv")
