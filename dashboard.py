import sqlite3, pandas as pd, streamlit as st
from pathlib import Path

st.set_page_config(page_title="E-Commerce Sales", layout="wide")

DB_PATH = Path("data/ecommerce.db")
BUILDER = Path("src/sql/build_db.py")

def build_db():
    """Create data dir and build the SQLite DB."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    import subprocess, sys
    st.info("Building SQLite database (first run)…")
    subprocess.run([sys.executable, str(BUILDER)], check=True)

def tables_ok(conn) -> bool:
    """Return True if required tables exist."""
    try:
        t = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        need = {"orders", "products", "customers"}
        return need.issubset(set(t["name"]))
    except Exception:
        return False

@st.cache_data(show_spinner=True)
def load_data():
    # Ensure DB exists
    if not DB_PATH.exists():
        build_db()

    # Try read; if it fails or tables missing, rebuild once and retry.
    def _read():
        conn = sqlite3.connect(str(DB_PATH))
        try:
            if not tables_ok(conn):
                conn.close()
                build_db()
                conn = sqlite3.connect(str(DB_PATH))
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
            return df
        except Exception as e:
            # Rebuild once more if anything went wrong; then raise if it still fails
            conn.close()
            build_db()
            conn = sqlite3.connect(str(DB_PATH))
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
            return df
        finally:
            try:
                conn.close()
            except Exception:
                pass

    df = _read()
    df["order_month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()
    df["revenue"] = (df["base_price"] * (1 - df["discount"])) * df["quantity"]
    return df

df = load_data()

# --------------- Sidebar filters ---------------
with st.sidebar:
    st.header("Filters")
    min_d, max_d = df["order_date"].min().date(), df["order_date"].max().date()
    dr = st.date_input("Date range", value=(min_d, max_d))
    if isinstance(dr, tuple) and len(dr) == 2:
        df = df[(df["order_date"].dt.date >= dr[0]) & (df["order_date"].dt.date <= dr[1])]

    status = st.multiselect("Status", df["status"].unique().tolist(), default=["Completed"])
    segments = st.multiselect("Segment", df["segment"].unique().tolist(), default=df["segment"].unique().tolist())
    categories = st.multiselect("Category", df["category"].unique().tolist(), default=df["category"].unique().tolist())
    countries = st.multiselect("Country", df["country"].unique().tolist(), default=df["country"].unique().tolist())

mask = (
    df["status"].isin(status)
    & df["segment"].isin(segments)
    & df["category"].isin(categories)
    & df["country"].isin(countries)
)
d = df[mask].copy()
completed = d["status"].eq("Completed")
refunded = d["status"].eq("Refunded")

# --------------- KPIs ---------------
revenue = d.loc[completed, "revenue"].sum()
orders = len(d)
aov = d.loc[completed, "revenue"].mean() if completed.any() else 0.0
completion_rate = float(completed.mean() * 100)
refund_rate = float(refunded.mean() * 100)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Revenue", f"${revenue:,.0f}")
c2.metric("Orders", f"{orders:,}")
c3.metric("AOV", f"${aov:,.2f}")
c4.metric("Completion Rate", f"{completion_rate:.1f}%")
c5.metric("Refund Rate", f"{refund_rate:.1f}%")

# --------------- Charts ---------------
st.subheader("Revenue Over Time")
st.line_chart(d.loc[completed].groupby("order_month")["revenue"].sum())

st.subheader("Top Categories")
st.bar_chart(d.loc[completed].groupby("category")["revenue"].sum().sort_values(ascending=False).head(10))

st.subheader("Segments by Revenue")
st.bar_chart(d.loc[completed].groupby("segment")["revenue"].sum().sort_values(ascending=False))

# --------------- Data & Downloads ---------------
st.subheader("Filtered Orders (sample)")
st.dataframe(d.head(500), use_container_width=True)

@st.cache_data
def to_csv_bytes(df_in: pd.DataFrame) -> bytes:
    return df_in.to_csv(index=False).encode("utf-8")

completed_d = d[d["status"] == "Completed"]
monthly_kpis = completed_d.groupby("order_month").agg(
    revenue=("revenue", "sum"),
    orders=("order_id", "count"),
    aov=("revenue", "mean"),
).reset_index()

rates = d.groupby("order_month")["status"].value_counts(normalize=True).unstack(fill_value=0.0)
for col in ["Completed", "Refunded"]:
    if col not in rates.columns:
        rates[col] = 0.0
rates = rates[["Completed", "Refunded"]]

monthly_kpis = monthly_kpis.merge(
    rates.mul(100).reset_index()[["order_month", "Completed", "Refunded"]],
    on="order_month",
    how="left",
).rename(columns={"Completed": "completion_rate", "Refunded": "refund_rate"}).fillna(0.0)

dl_col1, dl_col2 = st.columns(2)
with dl_col1:
    st.download_button("Download filtered orders (CSV)", data=to_csv_bytes(d),
                       file_name="filtered_orders.csv", mime="text/csv")
with dl_col2:
    st.download_button("Download monthly KPIs (CSV)", data=to_csv_bytes(monthly_kpis),
                       file_name="monthly_kpis.csv", mime="text/csv")
