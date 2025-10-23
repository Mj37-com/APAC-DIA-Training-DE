#!/usr/bin/env python3
"""
generate_data.py

Generates synthetic datasets according to the spec:
- Dimensions (CSV): customers, products, stores, suppliers
- Facts: orders_header (partitioned CSV), orders_lines (partitioned CSV)
- Events: JSONL partitioned by date
- Sensors: CSV partitioned by store_id/month
- Exchange rates: XLSX (3 years, includes weekends)
- Shipments: Parquet (~1,000,000)
- Returns: returns_v1.parquet, returns_v2.parquet + upsert/delete CSVs

Defaults create full-scale datasets from the spec. Use flags to scale down for local testing.
"""
import os
import shutil
import argparse
import json
import math
import random
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# Recommended external libs:
# pip install pandas numpy pyarrow openpyxl faker

try:
    import pyarrow as pa  # used for parquet write fallback
except Exception:
    pa = None

# For nicer fake addresses/names (optional)
try:
    from faker import Faker
    FAKE = Faker()
except Exception:
    FAKE = None

pd.options.mode.chained_assignment = None

# --------------------
# Helpers
# --------------------
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def rand_alphanum(rng, length):
    chars = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    return "".join(rng.choice(chars, size=length))

def rand_alphanum_vec(rng, length, size):
    chars = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
    arr = rng.choice(chars, size=(size, length))
    return ["".join(row) for row in arr]

def write_parquet(df: pd.DataFrame, path: Path):
    # Use pandas .to_parquet (pyarrow)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def write_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def write_jsonl_from_iter(path: Path, iter_of_json_strings):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for line in iter_of_json_strings:
            fh.write(line + "\n")

# --------------------
# Data generators
# --------------------

# 1) Customers ~80,000
def generate_customers(out: Path, n=80000, seed=42):
    rng = np.random.default_rng(seed)
    ids = np.arange(1, n+1, dtype=np.int64)

    natural_keys = np.array([f"CUST-{s}" for s in rand_alphanum_vec(rng, 8, n)])
    first = np.array([ (FAKE.first_name() if FAKE else f"First_{i}") for i in ids ])
    last = np.array([ (FAKE.last_name() if FAKE else f"Last_{i}") for i in ids ])
    emails = np.array([f"user{i}@example.com" for i in ids], dtype=object)

    # malformed emails 0.5-1% → midpoint 0.75%
    n_malformed = max(1, int(round(n * 0.0075)))
    malformed_idx = rng.choice(n, size=n_malformed, replace=False)
    for idx in malformed_idx:
        emails[idx] = f"user{idx}example[dot]com"

    phones = np.array([f"+63{rng.integers(900,999)}{rng.integers(1_000_000,9_999_999)}" for _ in ids], dtype=object)
    # null phones (~2%)
    phones[rng.choice(n, size=int(round(n*0.02)), replace=False)] = None

    address_line1 = np.array([ (FAKE.street_address() if FAKE else f"{rng.integers(1,9999)} Main St") for _ in ids ], dtype=object)
    address_line2 = np.array([ (FAKE.secondary_address() if FAKE else f"Apt {rng.integers(1,999)}") for _ in ids ], dtype=object)
    address_line1[rng.choice(n, size=int(round(n*0.01)), replace=False)] = None
    address_line2[rng.choice(n, size=int(round(n*0.01)), replace=False)] = None

    cities = rng.choice(["Manila","Cebu","Davao","Baguio","Iloilo"], size=n)
    regions = rng.choice(["NCR","Visayas","Mindanao"], size=n)
    postcodes = np.array([f"{rng.integers(1000,9999)}" for _ in ids])
    country_codes = np.array(["PH"] * n)

    latitude = rng.uniform(5.0, 18.0, size=n)
    longitude = rng.uniform(117.0, 127.0, size=n)
    # impossible coords ~0.01%
    n_bad = max(1, int(round(n * 0.0001)))
    if n_bad > 0:
        bad_idx = rng.choice(n, size=n_bad, replace=False)
        latitude[bad_idx] = 999.0
        longitude[bad_idx] = 999.0

    birth_dates = pd.to_datetime(rng.integers(int(pd.Timestamp("1940-01-01").timestamp()),
                                              int(pd.Timestamp("2005-12-31").timestamp()), size=n), unit='s').date
    join_ts = pd.to_datetime("2023-01-01") + pd.to_timedelta(rng.integers(0, 365*2, size=n), 'D')
    is_vip = rng.choice([True, False], size=n, p=[0.05, 0.95])
    gdpr_consent = rng.choice([True, False], size=n, p=[0.9, 0.1])

    df = pd.DataFrame({
        "customer_id": ids,
        "natural_key": natural_keys,
        "first_name": first,
        "last_name": last,
        "email": emails,
        "phone": phones,
        "address_line1": address_line1,
        "address_line2": address_line2,
        "city": cities,
        "state_region": regions,
        "postcode": postcodes,
        "country_code": country_codes,
        "latitude": latitude,
        "longitude": longitude,
        "birth_date": birth_dates,
        "join_ts": join_ts,
        "is_vip": is_vip,
        "gdpr_consent": gdpr_consent
    })

    # duplicate natural_key ~0.2%
    n_dup = max(1, int(round(n * 0.002)))
    dup_src = rng.choice(n, size=n_dup, replace=False)
    dup_tgt = rng.choice(n, size=n_dup, replace=False)
    df.loc[dup_tgt, "natural_key"] = df.loc[dup_src, "natural_key"].values

    write_csv(df, out / "customers.csv")
    print(f"Generated customers.csv → {len(df):,} rows")
    return df

# 2) Products ~25,000
def generate_products(out: Path, n=25000, seed=43):
    rng = np.random.default_rng(seed)
    ids = np.arange(1, n+1, dtype=np.int64)
    skus = np.array([f"SKU-{s}" for s in rand_alphanum_vec(rng, 6, n)])
    names = np.array([f"Product_{i}" for i in ids])
    categories = rng.choice(["Electronics","Clothing","Food","Home","Toys"], size=n)
    subcats = rng.choice(["Sub1","Sub2","Sub3"], size=n)
    current_price = np.round(rng.uniform(1.0, 10000.0, size=n), 4)  # DECIMAL(12,4)

    # invalid/missing prices 0.1-0.5% -> use 0.3%
    n_bad = max(1, int(round(n * 0.003)))
    bad_idx = rng.choice(n, size=n_bad, replace=False)
    half = len(bad_idx)//2
    if half > 0:
        current_price[bad_idx[:half]] = -1.0
    for i in bad_idx[half:]:
        current_price[i] = np.nan

    currency = np.array(["PHP"] * n)
    is_discontinued = rng.choice([True, False], size=n, p=[0.05, 0.95])
    introduced_dt = pd.to_datetime("2020-01-01") + pd.to_timedelta(rng.integers(0, 365*5, size=n), 'D')
    discontinued_dt = np.array([None]*n, dtype=object)
    for i in range(n):
        if is_discontinued[i]:
            if rng.random() < 0.6:
                discontinued_dt[i] = (pd.to_datetime(introduced_dt[i]) + pd.to_timedelta(rng.integers(30, 365*2), 'D')).date()
            else:
                discontinued_dt[i] = None

    df = pd.DataFrame({
        "product_id": ids,
        "sku": skus,
        "name": names,
        "category": categories,
        "subcategory": subcats,
        "current_price": current_price,
        "currency": currency,
        "is_discontinued": is_discontinued,
        "introduced_dt": introduced_dt.date,
        "discontinued_dt": discontinued_dt
    })

    write_csv(df, out / "products.csv")
    print(f"Generated products.csv → {len(df):,} rows")
    return df

# 3) Stores ~5,000
def generate_stores(out: Path, n=5000, seed=44):
    rng = np.random.default_rng(seed)
    ids = np.arange(1, n+1, dtype=np.int64)
    store_code = np.array([f"S{str(i).zfill(6)}" for i in ids])
    name = np.array([f"Store_{i}" for i in ids])
    channel = rng.choice(["web","pos"], size=n)
    region = rng.choice(["North","South","East","West"], size=n)
    state = rng.choice(["NCR","Visayas","Mindanao"], size=n)
    latitude = rng.uniform(5.0,18.0,size=n)
    longitude = rng.uniform(117.0,127.0,size=n)

    # some impossible lat/lon ~0.05%
    n_bad = max(1, int(round(n * 0.0005)))
    if n_bad > 0:
        bad_idx = rng.choice(n, size=n_bad, replace=False)
        latitude[bad_idx] = -999.0
        longitude[bad_idx] = 9999.0

    # occasional duplicate store_codes ~0.1%
    n_dup = max(1, int(round(n * 0.001)))
    if n_dup > 0:
        src = rng.choice(n, size=n_dup, replace=False)
        tgt = rng.choice(n, size=n_dup, replace=False)
        for s,t in zip(src,tgt):
            store_code[t] = store_code[s]

    open_dt = pd.to_datetime("2015-01-01") + pd.to_timedelta(rng.integers(0, 365*10, size=n), 'D')
    close_dt = np.array([None]*n, dtype=object)

    df = pd.DataFrame({
        "store_id": ids,
        "store_code": store_code,
        "name": name,
        "channel": channel,
        "region": region,
        "state": state,
        "latitude": latitude,
        "longitude": longitude,
        "open_dt": open_dt.date,
        "close_dt": close_dt
    })

    write_csv(df, out / "stores.csv")
    print(f"Generated stores.csv → {len(df):,} rows")
    return df

# 4) Suppliers ~8,000
def generate_suppliers(out: Path, n=8000, seed=45):
    rng = np.random.default_rng(seed)
    ids = np.arange(1, n+1, dtype=np.int64)
    supplier_code = np.array([f"SUP-{s}" for s in rand_alphanum_vec(rng, 6, n)])
    name = np.array([f"Supplier_{i}" for i in ids])
    country_code = rng.choice(["PH","SG","US","JP","CN"], size=n)
    lead_time_days = rng.integers(1, 90, size=n)
    preferred = rng.choice([True, False], size=n, p=[0.2, 0.8])

    df = pd.DataFrame({
        "supplier_id": ids,
        "supplier_code": supplier_code,
        "name": name,
        "country_code": country_code,
        "lead_time_days": lead_time_days,
        "preferred": preferred
    })

    write_csv(df, out / "suppliers.csv")
    print(f"Generated suppliers.csv → {len(df):,} rows")
    return df

# 5 & 6) Orders header (>=1,000,000) & lines (~3-4M) partitioned
def generate_orders_partitioned(out: Path, total_orders=1_000_000, avg_lines=3.5, seed=46, chunk_orders=100_000):
    """
    Stream/generate orders in chunks and write partitions:
    - orders/order_dt=YYYY-MM-DD/orders_header_YYYY-MM-DD.csv
    - orders/orders_lines/order_dt=YYYY-MM-DD/orders_lines_YYYY-MM-DD.csv
    """
    rng = np.random.default_rng(seed)
    orders_root = out / "orders"
    orders_lines_root = orders_root / "orders_lines"
    ensure_dir(orders_root)
    ensure_dir(orders_lines_root)

    # load masters from out (they must exist)
    customers = pd.read_csv(out / "customers.csv")
    stores = pd.read_csv(out / "stores.csv")
    products = pd.read_csv(out / "products.csv")
    cust_ids = customers['customer_id'].values
    store_ids = stores['store_id'].values
    product_ids = products['product_id'].values

    start_ts = pd.Timestamp("2024-01-01")
    remaining = total_orders
    next_order_id = 1

    print(f"Generating {total_orders:,} orders in chunks of {chunk_orders:,}...")

    while remaining > 0:
        n = min(chunk_orders, remaining)
        # timestamps hourly for the chunk
        order_ts_chunk = pd.date_range(start_ts, periods=n, freq='H')
        start_ts = order_ts_chunk[-1] + pd.Timedelta(hours=1)

        order_ids = np.arange(next_order_id, next_order_id + n, dtype=np.int64)
        next_order_id += n
        remaining -= n

        customer_id = rng.choice(cust_ids, size=n)
        store_id = rng.choice(store_ids, size=n)

        # ~1% FK violations
        n_fk = max(1, int(round(n * 0.01)))
        if n_fk > 0:
            fk_idx = rng.choice(n, size=n_fk, replace=False)
            customer_id[fk_idx] = cust_ids.max() + rng.integers(1,1000, size=n_fk)
            fk_idx2 = rng.choice(n, size=n_fk, replace=False)
            store_id[fk_idx2] = store_ids.max() + rng.integers(1,500, size=n_fk)

        channel = rng.choice(["web","pos"], size=n)
        payment_method = rng.choice(["Cash","Card","E-Wallet"], size=n)
        coupon_code = [None]*n
        shipping_fee = np.round(rng.uniform(20.0,500.0,size=n),2)
        currency = np.array(["PHP"]*n)

        header_df = pd.DataFrame({
            "order_id": order_ids,
            "order_ts": order_ts_chunk,
            "order_dt": pd.to_datetime(order_ts_chunk).date,
            "order_dt_local": pd.to_datetime(order_ts_chunk).date,
            "customer_id": customer_id,
            "store_id": store_id,
            "channel": channel,
            "payment_method": payment_method,
            "coupon_code": coupon_code,
            "shipping_fee": shipping_fee,
            "currency": currency
        })

        # create lines for chunk
        line_counts = rng.integers(1, 6, size=n)  # 1..5
        total_lines = int(line_counts.sum())
        order_ids_rep = np.repeat(header_df['order_id'].values, line_counts)
        line_numbers = np.concatenate([np.arange(1, c+1) for c in line_counts])

        product_id = rng.choice(product_ids, size=total_lines)
        # 1% invalid product ids
        n_invalid = max(1, int(round(total_lines * 0.01)))
        if n_invalid > 0:
            invalid_idx = rng.choice(total_lines, size=n_invalid, replace=False)
            product_id[invalid_idx] = product_ids.max() + rng.integers(1, 100, size=n_invalid)

        qty = rng.integers(1, 10, size=total_lines)
        # rare negative qty
        n_neg = int(round(total_lines * 0.0005))
        if n_neg > 0:
            qty[rng.choice(total_lines, size=n_neg, replace=False)] = -1

        unit_price = np.round(rng.uniform(1.0, 5000.0, size=total_lines), 4)
        # rare zero prices
        n_zero = int(round(total_lines * 0.0005))
        if n_zero > 0:
            unit_price[rng.choice(total_lines, size=n_zero, replace=False)] = 0.0

        line_discount_pct = np.round(rng.uniform(0, 0.5, size=total_lines), 4)
        tax_pct = np.round(rng.uniform(0, 0.2, size=total_lines), 4)

        lines_df = pd.DataFrame({
            "order_id": order_ids_rep,
            "line_number": line_numbers,
            "product_id": product_id,
            "qty": qty,
            "unit_price": unit_price,
            "line_discount_pct": line_discount_pct,
            "tax_pct": tax_pct
        })

        # duplicate order_ids across files 0.05%: make tiny duplicate set
        n_dup = max(1, int(round(n * 0.0005)))
        if n_dup > 0:
            dup_oids = rng.choice(order_ids, size=n_dup, replace=False)
            dup_rows = header_df[header_df['order_id'].isin(dup_oids)].copy()
            dup_rows['order_dt'] = (pd.to_datetime(dup_rows['order_dt']) + pd.Timedelta(days=1)).dt.date
            header_df = pd.concat([header_df, dup_rows], ignore_index=True).sort_values('order_id').reset_index(drop=True)
            dup_lines = lines_df[lines_df['order_id'].isin(dup_oids)].copy()
            lines_df = pd.concat([lines_df, dup_lines], ignore_index=True)

        # write header and lines partitioned by order_dt
        for d, grp in header_df.groupby('order_dt'):
            dstr = pd.to_datetime(d).strftime("%Y-%m-%d")
            hdr_part_dir = orders_root / f"order_dt={dstr}"
            lines_part_dir = orders_lines_root / f"order_dt={dstr}"
            ensure_dir(hdr_part_dir)
            ensure_dir(lines_part_dir)
            hdr = grp.copy()
            hdr.to_csv(hdr_part_dir / f"orders_header_{dstr}.csv", index=False)
            ids_in_hdr = hdr['order_id'].unique()
            lines_match = lines_df[lines_df['order_id'].isin(ids_in_hdr)]
            lines_match.to_csv(lines_part_dir / f"orders_lines_{dstr}.csv", index=False)

        print(f"Chunk written: headers={len(header_df):,}, lines={len(lines_df):,}")

    print(f"Completed orders: target headers={total_orders:,} (partitioned under {orders_root})")
    return True

# 7) Events JSONL partitioned (~2,000,000)
def generate_events(out: Path, total_events=2_000_000, seed=47, days=30):
    rng = np.random.default_rng(seed)
    out_events = out / "events"
    ensure_dir(out_events)
    per_day = total_events // days
    event_types = ["click","view","purchase","add_to_cart","checkout"]

    for day in range(days):
        date = (pd.Timestamp("2024-01-01") + pd.Timedelta(days=day)).date()
        file_path = out_events / f"events_{date.strftime('%Y-%m-%d')}.jsonl"
        with open(file_path, "w", encoding="utf-8") as fh:
            for i in range(per_day):
                event_id = f"E{day}_{i}_{rand_alphanum(rng, 6)}"
                event_ts = (pd.Timestamp("2024-01-01") + pd.Timedelta(days=day) +
                            pd.to_timedelta(int(rng.integers(0, 86400)), 's')).isoformat()
                envelope = {
                    "event_id": event_id,
                    "event_ts": event_ts,
                    "event_type": str(rng.choice(event_types)),
                    "user_id": int(rng.integers(1, 80000)),
                    "session_id": rand_alphanum(rng, 12)
                }
                payload = {"value": int(rng.integers(0,1000))}
                obj = {"envelope": envelope, "payload": payload}
                s = json.dumps(obj)

                # malformed JSON lines 0.05%
                if rng.random() < 0.0005:
                    s = s[:-5]
                # missing envelope fields sometimes 0.1%
                if rng.random() < 0.001:
                    obj2 = {"envelope": {"event_id": envelope["event_id"]}, "payload": payload}
                    s = json.dumps(obj2)

                fh.write(s + "\n")
        print(f"Wrote events partition {file_path} ({per_day:,} lines)")
    print(f"Completed events generation (~{per_day*days:,} lines)")

# 8) Sensors CSV partitioned by store_id/month (target 5-10M)
def generate_sensors_partitioned(out: Path, stores_count=5000, months=12, rows_per_store_month=100, seed=48):
    """
    Default produces: stores_count * months * rows_per_store_month rows (e.g. 5000 * 12 * 100 = 6,000,000)
    Partition path: sensors/store_id={id}/month=YYYY-MM/sensors_{id}_{YYYY-MM}.csv
    """
    rng = np.random.default_rng(seed)
    sensors_root = out / "sensors"
    ensure_dir(sensors_root)
    start = pd.Timestamp("2024-01-01")

    total = stores_count * months * rows_per_store_month
    print(f"Generating sensors ~{total:,} rows (stores={stores_count}, months={months}, rows_per_store_month={rows_per_store_month})")

    for store in range(1, stores_count+1):
        for m in range(months):
            month_dt = start + pd.DateOffset(months=m)
            p = sensors_root / f"store_id={store}" / f"month={month_dt.strftime('%Y-%m')}"
            ensure_dir(p)
            n = rows_per_store_month
            times = pd.date_range(month_dt, periods=n, freq='H')
            df = pd.DataFrame({
                "sensor_ts": times,
                "store_id": [store]*n,
                "shelf_id": [f"SHELF-{int(x)}" for x in rng.integers(1, 1000, size=n)],
                "temperature_c": np.round(rng.uniform(-10, 50, size=n), 2),
                "humidity_pct": np.round(rng.uniform(0, 100, size=n), 2),
                "battery_mv": rng.integers(2500, 4200, size=n)
            })
            # out-of-range anomalies 0.1-0.5% -> use 0.2%
            n_bad = max(1, int(round(n * 0.002)))
            bad_idx = rng.choice(n, size=n_bad, replace=False)
            df.loc[bad_idx, "temperature_c"] = 999.0
            # missing sensor_ts occasional
            miss_idx = rng.choice(n, size=max(1,int(round(n*0.002))), replace=False)
            df.loc[miss_idx, "sensor_ts"] = None
            df.to_csv(p / f"sensors_{store}_{month_dt.strftime('%Y-%m')}.csv", index=False)
        if store % 500 == 0:
            print(f"Generated sensors for store {store}/{stores_count}")
    print("Completed sensors generation.")

# 9) Exchange rates XLSX (~1,100 rows for 3 years daily * currencies)
def generate_exchange_rates_xlsx(out: Path, years=3, seed=49):
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2023-01-01")
    days = 365 * years + (years // 4)  # approximate include leap-ish
    dates = pd.date_range(start, periods=days, freq='D')
    currencies = ["USD","EUR","JPY","SGD","GBP"]
    rows = []
    for d in dates:
        for cur in currencies:
            rows.append({"date": d.date(), "currency": cur, "rate_to_aud": round(float(rng.uniform(0.1, 2.5)), 8)})
    df = pd.DataFrame(rows)
    # write xlsx
    out_file = out / "exchange_rates.xlsx"
    df.to_excel(out_file, index=False, engine="openpyxl")
    print(f"Generated exchange_rates.xlsx → {len(df):,} rows")
    return df

# 10) Shipments Parquet (~1,000,000)
def generate_shipments_parquet(out: Path, n=1_000_000, seed=50):
    rng = np.random.default_rng(seed)
    shipment_id = np.arange(1, n+1, dtype=np.int64)
    order_id = rng.integers(1, 1_000_000, size=n)
    shipped_at = pd.to_datetime("2024-01-01") + pd.to_timedelta(rng.integers(0, 365*24, size=n), 'h')
    delivered_mask = rng.random(n) >= 0.05  # 5% in-transit (null delivered_at)
    delivered_at = np.array([None]*n, dtype=object)
    delivered_at[delivered_mask] = (shipped_at[delivered_mask] + pd.to_timedelta(rng.integers(1,200,size=delivered_mask.sum()), 'h')).astype('datetime64[ns]')
    # late deliveries 2% of delivered -> add 5 days
    late_mask = (rng.random(n) < 0.02) & delivered_mask
    if late_mask.any():
        delivered_at[late_mask] = (pd.to_datetime(delivered_at[late_mask]) + pd.to_timedelta(5*24, 'h')).astype('datetime64[ns]')
    ship_cost = np.round(rng.uniform(20.0, 1000.0, size=n), 2)

    df = pd.DataFrame({
        "shipment_id": shipment_id,
        "order_id": order_id,
        "carrier": rng.choice(["J&T","LBC","2GO","Grab"], size=n),
        "shipped_at": shipped_at,
        "delivered_at": delivered_at,
        "ship_cost": ship_cost
    })

    out_parquet = out / "shipments.parquet"
    df.to_parquet(out_parquet, index=False)
    print(f"Generated shipments.parquet → {len(df):,} rows")
    return df

# 11) Returns (v1 + v2 + upsert/delete)
def generate_returns(out: Path, n=100000, seed=51):
    rng = np.random.default_rng(seed)
    ids = np.arange(1, n+1, dtype=np.int64)
    order_id = rng.integers(1, 1_000_000, size=n)
    product_id = rng.integers(1, 25000, size=n)
    return_ts = pd.date_range("2024-07-01", periods=n, freq='H')
    qty = rng.integers(1,5,size=n)
    reason = rng.choice(["Defective","Wrong Item","Late Delivery","Changed Mind"], size=n)
    df_v1 = pd.DataFrame({"return_id": ids, "order_id": order_id, "product_id": product_id,
                         "return_ts": return_ts, "qty": qty, "reason": reason})
    df_v1.to_parquet(out / "returns_v1.parquet", index=False)

    # v2 adds return_reason_code
    codes = rng.choice(["R01","R02","R03","R04","R05"], size=n)
    df_v2 = df_v1.copy()
    df_v2['return_reason_code'] = codes
    df_v2.to_parquet(out / "returns_v2.parquet", index=False)

    # upsert CSV (5% sample updated)
    upsert_n = max(1, int(round(n * 0.05)))
    upsert = df_v2.sample(upsert_n, random_state=seed).copy()
    upsert['qty'] = upsert['qty'] + 1
    upsert.to_csv(out / "returns_upsert.csv", index=False)

    # delete CSV (2% delete)
    delete_n = max(1, int(round(n * 0.02)))
    delete_ids = rng.choice(df_v2['return_id'], size=delete_n, replace=False)
    pd.DataFrame({"return_id": delete_ids}).to_csv(out / "returns_delete.csv", index=False)

    print(f"Generated returns_v1/v2 parquets → {n:,} rows each, plus upsert/delete CSVs")
    return df_v1, df_v2

# --------------------
# CLI / Runner
# --------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=str, default="scripts/data_raw", help="Output folder")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    p.add_argument("--orders", type=int, default=1_000_000, help="Total orders headers to generate (default 1,000,000)")
    p.add_argument("--sensors_target", type=int, default=6_000_000, help="Approx sensors rows target (default 6M)")
    p.add_argument("--events", type=int, default=2_000_000, help="Total events (default 2,000,000)")
    p.add_argument("--scale", choices=["full","small"], default="full", help="full = spec volumes, small = quicker dev volumes")
    return p.parse_args()

def main():
    args = parse_args()
    out = Path(args.out)
    seed = args.seed

    # Clean up previous output
    if out.exists():
        print(f"🧹 Cleaning up existing output at {out} ...")
        shutil.rmtree(out)
    ensure_dir(out)

    # scale settings
    if args.scale == "small":
        orders_target = min(10000, args.orders)
        sensors_target = min(100000, args.sensors_target)
        events_target = min(200000, args.events)
        products_target = 2000
        stores_target = 200
        suppliers_target = 500
    else:
        orders_target = args.orders
        sensors_target = args.sensors_target
        events_target = args.events
        products_target = 25000
        stores_target = 5000
        suppliers_target = 8000

    print("Generating master tables...")
    generate_customers(out, n=80000, seed=seed)
    generate_products(out, n=products_target, seed=seed+1)
    generate_stores(out, n=stores_target, seed=seed+2)
    generate_suppliers(out, n=suppliers_target, seed=seed+3)

    print("Generating orders & order_lines (partitioned). This may take a while...")
    # Choose chunk_orders to trade memory vs speed (100k recommended)
    generate_orders_partitioned(out=out, total_orders=orders_target, avg_lines=3.5, seed=seed+4, chunk_orders=100_000 if args.scale=="full" else 10_000)

    print("Generating shipments...")
    generate_shipments_parquet(out, n=1_000_000 if args.scale=="full" else 100_000, seed=seed+5)

    print("Generating exchange rates (XLSX)...")
    generate_exchange_rates_xlsx(out, years=3, seed=seed+6)

    print("Generating sensors (partitioned)...")
    # derive sensible partition parameters from sensors_target
    stores_count = stores_target
    months = 12
    rows_per_store_month = max(1, sensors_target // (stores_count * months))
    generate_sensors_partitioned(out, stores_count=stores_count, months=months, rows_per_store_month=rows_per_store_month, seed=seed+7)

    print("Generating events (JSONL partitions)...")
    generate_events(out, total_events=events_target, seed=seed+8, days=30)

    print("Generating returns (v1/v2 + upsert/delete)...")
    generate_returns(out, n=100000 if args.scale=="full" else 10000, seed=seed+9)

    print("\n✅ All data generation complete. Files written under:", out.resolve())

if __name__ == "__main__":
    main()
