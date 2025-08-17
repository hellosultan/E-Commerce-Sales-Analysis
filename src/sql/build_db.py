#!/usr/bin/env python3
"""
Builds data/ecommerce.db with synthetic, realistic e-commerce tables.
Usage:
  python src/sql/build_db.py
"""
from pathlib import Path
import sqlite3, numpy as np, pandas as pd

RNG = np.random.default_rng(42)

def ensure_dirs():
    Path("data").mkdir(parents=True, exist_ok=True)
    Path("reports/figures").mkdir(parents=True, exist_ok=True)

def gen_customers(n=5000):
    segments = ["Consumer","Corporate","Small Biz","Enterprise"]
    countries = ["US","UK","FR","DE","ES","IT","AE","SA","BH","QA","IN"]
    return pd.DataFrame({
        "customer_id": np.arange(1, n+1),
        "segment": RNG.choice(segments, n, p=[0.55,0.2,0.2,0.05]),
        "country": RNG.choice(countries, n),
        "signup_date": pd.to_datetime("2022-01-01") + pd.to_timedelta(RNG.integers(0, 900, n), unit="D"),
    })

def gen_products(n=500):
    cats = ["Electronics","Home","Beauty","Grocery","Fashion","Sports","Toys"]
    return pd.DataFrame({
        "product_id": np.arange(1, n+1),
        "category": RNG.choice(cats, n),
        "base_price": RNG.uniform(5, 400, n).round(2),
    })

def gen_orders(n=50000, customers=5000, products=500):
    start = pd.to_datetime("2023-01-01")
    order_dates = start + pd.to_timedelta(RNG.integers(0, 550, n), unit="D")
    qty = RNG.integers(1, 5, n)
    discount = np.clip(RNG.normal(0.08, 0.07, n), 0, 0.4).round(2)
    shipping = RNG.choice(["Standard","Express","Two-Day"], n, p=[0.7,0.2,0.1])
    status = RNG.choice(["Completed","Refunded","Cancelled"], n, p=[0.93,0.04,0.03])
    return pd.DataFrame({
        "order_id": np.arange(1, n+1),
        "order_date": order_dates,
        "customer_id": RNG.integers(1, customers+1, n),
        "product_id": RNG.integers(1, products+1, n),
        "quantity": qty,
        "discount": discount,
        "shipping": shipping,
        "status": status,
    })

def main(db_path="data/ecommerce.db"):
    ensure_dirs()
    customers = gen_customers()
    products  = gen_products()
    orders    = gen_orders()

    conn = sqlite3.connect(db_path)
    customers.to_sql("customers", conn, if_exists="replace", index=False)
    products.to_sql("products", conn, if_exists="replace", index=False)
    orders.to_sql("orders", conn, if_exists="replace", index=False)

    conn.executescript("""
    CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_orders_cust ON orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_prod ON orders(product_id);
    CREATE INDEX IF NOT EXISTS idx_products_cat ON products(category);
    CREATE INDEX IF NOT EXISTS idx_customers_seg ON customers(segment);
    """)
    conn.commit()
    conn.close()

    print("[OK] Wrote", db_path)
    print(f"[STATS] customers={len(customers):,} products={len(products):,} orders={len(orders):,}")

if __name__ == "__main__":
    main()
