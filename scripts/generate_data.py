import os
import argparse
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def generate_customers(n=50, seed=42):
    np.random.seed(seed)
    return pd.DataFrame({
        "customer_id": range(1, n + 1),
        "natural_key": [f"CUST_{i:04d}" for i in range(1, n + 1)],
        "first_name": [f"First_{i}" for i in range(1, n + 1)],
        "last_name": [f"Last_{i}" for i in range(1, n + 1)],
        "email": [f"user{i}@example.com" for i in range(1, n + 1)],
        "phone": [f"+63{np.random.randint(900, 999)}{np.random.randint(1000000, 9999999)}" for _ in range(n)],
        "address_line1": [f"{i} Main St" for i in range(1, n + 1)],
        "address_line2": [f"Apt {i}" for i in range(1, n + 1)],
        "city": np.random.choice(["Manila", "Cebu", "Davao", "Baguio"], n),
        "state_region": np.random.choice(["NCR", "Visayas", "Mindanao"], n),
        "postcode": [f"{np.random.randint(1000,9999)}" for _ in range(n)],
        "country_code": ["PH"]*n,
        "latitude": np.random.uniform(10.0, 14.0, n),
        "longitude": np.random.uniform(120.0, 125.0, n),
        "birth_date": pd.date_range("1970-01-01", periods=n).date,
        "join_ts": pd.date_range("2023-01-01", periods=n, freq="D"),
        "is_vip": np.random.choice([True, False], n),
        "gdpr_consent": np.random.choice([True, False], n),
    })

def generate_products(n=20, seed=42):
    np.random.seed(seed)
    return pd.DataFrame({
        "product_id": range(1, n + 1),
        "sku": [f"SKU_{i:04d}" for i in range(1, n + 1)],
        "name": [f"Product_{i}" for i in range(1, n + 1)],
        "category": np.random.choice(["Electronics", "Clothing", "Food", "Home"], n),
        "subcategory": np.random.choice(["Sub1", "Sub2", "Sub3"], n),
        "current_price": np.round(np.random.uniform(100, 1000, n), 2),
        "currency": ["PHP"]*n,
        "is_discontinued": np.random.choice([True, False], n),
        "introduced_dt": pd.date_range("2020-01-01", periods=n).date,
        "discontinued_dt": [None]*n,
    })

def generate_stores(n=10):
    return pd.DataFrame({
        "store_id": range(1, n + 1),
        "store_code": [f"S{i:03d}" for i in range(1, n + 1)],
        "name": [f"Store_{i}" for i in range(1, n + 1)],
        "channel": np.random.choice(["Online", "Offline"], n),
        "region": np.random.choice(["North", "South", "East", "West"], n),
        "state": np.random.choice(["NCR", "Visayas", "Mindanao"], n),
        "latitude": np.random.uniform(10.0, 14.0, n),
        "longitude": np.random.uniform(120.0, 125.0, n),
        "open_dt": pd.date_range("2020-01-01", periods=n).date,
        "close_dt": [None]*n,
    })

def generate_suppliers(n=10):
    return pd.DataFrame({
        "supplier_id": range(1, n + 1),
        "supplier_code": [f"SUP{i:03d}" for i in range(1, n + 1)],
        "name": [f"Supplier_{i}" for i in range(1, n + 1)],
        "country_code": np.random.choice(["PH", "SG", "US", "JP"], n),
        "lead_time_days": np.random.randint(1, 30, n),
        "preferred": np.random.choice([True, False], n),
    })

def generate_orders(customers, stores, seed=42):
    np.random.seed(seed)
    n = 200
    order_ids = range(1, n + 1)
    header = pd.DataFrame({
        "order_id": order_ids,
        "order_ts": pd.date_range("2024-01-01", periods=n, freq="H"),
        "order_dt_local": pd.date_range("2024-01-01", periods=n).date,
        "customer_id": np.random.choice(customers["customer_id"], n),
        "store_id": np.random.choice(stores["store_id"], n),
        "channel": np.random.choice(["Online", "Offline"], n),
        "payment_method": np.random.choice(["Cash", "Card", "E-Wallet"], n),
        "coupon_code": [None]*n,
        "shipping_fee": np.round(np.random.uniform(50, 200, n), 2),
        "currency": ["PHP"]*n,
    })
    lines = pd.DataFrame({
        "order_id": np.repeat(order_ids, 2),
        "line_number": np.tile([1,2], n),
        "product_id": np.random.randint(1, 21, n*2),
        "qty": np.random.randint(1, 10, n*2),
        "unit_price": np.round(np.random.uniform(100, 1000, n*2), 2),
        "line_discount_pct": np.round(np.random.uniform(0, 0.2, n*2), 4),
        "tax_pct": np.round(np.random.uniform(0, 0.15, n*2), 4),
    })
    return header, lines

def generate_shipments(orders):
    n = len(orders)
    return pd.DataFrame({
        "shipment_id": range(1, n + 1),
        "order_id": orders["order_id"],
        "carrier": np.random.choice(["J&T", "LBC", "2GO"], n),
        "shipped_at": orders["order_ts"] + pd.to_timedelta(np.random.randint(1,5,n), "h"),
        "delivered_at": orders["order_ts"] + pd.to_timedelta(np.random.randint(5,72,n), "h"),
        "ship_cost": np.round(np.random.uniform(50, 200, n), 2),
    })

def generate_returns(orders_lines):
    n = len(orders_lines)//4
    return pd.DataFrame({
        "return_id": range(1, n+1),
        "order_id": np.random.choice(orders_lines["order_id"], n),
        "product_id": np.random.choice(orders_lines["product_id"], n),
        "return_ts": pd.date_range("2024-07-01", periods=n, freq="H"),
        "qty": np.random.randint(1, 5, n),
        "reason": np.random.choice(["Defective", "Wrong Item", "Late Delivery"], n),
    })

def generate_exchange_rates():
    dates = pd.date_range("2024-01-01", periods=365)
    return pd.DataFrame({
        "date": dates,
        "currency": ["USD"]*len(dates),
        "rate_to_aud": np.round(np.random.uniform(0.5, 2.0, len(dates)), 8),
    })

def generate_sensors():
    times = pd.date_range("2024-01-01", periods=100, freq="h")
    return pd.DataFrame({
        "sensor_ts": times,
        "store_id": np.random.randint(1, 11, len(times)),
        "shelf_id": [f"SHELF_{i}" for i in range(len(times))],
        "temperature_c": np.round(np.random.uniform(20,35,len(times)),2),
        "humidity_pct": np.round(np.random.uniform(40,80,len(times)),2),
        "battery_mv": np.random.randint(3000,4200,len(times)),
    })

def generate_events():
    events = []
    event_types = ["click", "view", "purchase", "add_to_cart"]
    for i in range(100):
        events.append({
            "json": json.dumps({
                "event_id": i + 1,
                "timestamp": (datetime.now() - timedelta(minutes=np.random.randint(0, 10000))).isoformat(),
                "event_type": np.random.choice(event_types),
                "customer_id": np.random.randint(1, 51)
            })
        })
    return pd.DataFrame(events)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="scripts/data_raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out_dir = Path(args.out)
    ensure_dir(out_dir)

    # --- Generate datasets ---
    customers = generate_customers(seed=args.seed)
    products = generate_products(seed=args.seed)
    stores = generate_stores()
    suppliers = generate_suppliers()
    orders_header, orders_lines = generate_orders(customers, stores, seed=args.seed)
    shipments = generate_shipments(orders_header)
    returns = generate_returns(orders_lines)
    rates = generate_exchange_rates()
    sensors = generate_sensors()
    events = generate_events()

    # --- Write files ---
    customers.to_csv(out_dir / "customers.csv", index=False)
    products.to_csv(out_dir / "products.csv", index=False)
    stores.to_csv(out_dir / "stores.csv", index=False)
    suppliers.to_csv(out_dir / "suppliers.csv", index=False)
    orders_header.to_csv(out_dir / "orders_header.csv", index=False)
    orders_lines.to_csv(out_dir / "orders_lines.csv", index=False)
    shipments.to_parquet(out_dir / "shipments.parquet", index=False)
    returns.to_parquet(out_dir / "returns_day1.parquet", index=False)
    rates.to_parquet(out_dir / "exchange_rates.parquet", index=False)
    sensors.to_parquet(out_dir / "sensors.parquet", index=False)
    events.to_csv(out_dir / "events.csv", index=False)

    print(f"✅ Sample raw data written to {out_dir}")
    print("Files written:")
    for f in sorted(os.listdir(out_dir)):
        print(f" - {f}")

if __name__ == "__main__":
    main()
