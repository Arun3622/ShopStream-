"""
Data Source Simulator - E-Commerce Event Stream
================================================
Simulates a real-time e-commerce event stream (like Kafka topics would emit)
and lands raw events as JSON Lines files in data/raw/.

Events: order_created, payment_processed, shipment_dispatched
Entities: customers.csv, products.csv (dimension-style reference data)
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

random.seed(42)
np.random.seed(42)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 1200
N_PRODUCTS = 180
N_ORDERS = 30000
DAYS = 90

CATEGORIES = {
    "Electronics": {"price": (40, 1600), "weight": 0.22},
    "Fashion": {"price": (12, 260), "weight": 0.30},
    "Home & Kitchen": {"price": (15, 480), "weight": 0.20},
    "Beauty": {"price": (8, 140), "weight": 0.13},
    "Sports": {"price": (10, 320), "weight": 0.09},
    "Books": {"price": (5, 60), "weight": 0.06},
}

CITIES = [
    ("New York", "US"), ("Los Angeles", "US"), ("Chicago", "US"), ("Houston", "US"),
    ("London", "UK"), ("Manchester", "UK"), ("Berlin", "DE"), ("Munich", "DE"),
    ("Paris", "FR"), ("Mumbai", "IN"), ("Delhi", "IN"), ("Bengaluru", "IN"),
    ("Sydney", "AU"), ("Toronto", "CA"), ("Dubai", "AE"), ("Singapore", "SG"),
]

FIRST_NAMES = ["Aarav", "Emma", "Liam", "Olivia", "Noah", "Sofia", "Ethan", "Mia",
               "Lucas", "Zara", "Kai", "Nina", "Omar", "Ivy", "Leo", "Priya"]
LAST_NAMES = ["Sharma", "Johnson", "Smith", "Garcia", "Miller", "Patel", "Kim",
              "Wong", "Brown", "Silva", "Khan", "Rossi", "Chen", "Dubois"]


def build_customers():
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        city, country = random.choice(CITIES)
        joined = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
        rows.append({
            "customer_id": f"C{i:05d}",
            "name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "city": city,
            "country": country,
            "segment": random.choice(["Standard"] * 6 + ["Premium"] * 3 + ["VIP"]),
            "joined_date": joined.strftime("%Y-%m-%d"),
        })
    return rows


def build_products():
    rows = []
    cat_names = list(CATEGORIES.keys())
    cat_weights = [CATEGORIES[c]["weight"] for c in cat_names]
    for i in range(1, N_PRODUCTS + 1):
        cat = np.random.choice(cat_names, p=cat_weights)
        lo, hi = CATEGORIES[cat]["price"]
        base_price = round(random.uniform(lo, hi), 2)
        rows.append({
            "product_id": f"P{i:05d}",
            "product_name": f"{cat.split(' ')[0].replace('&', '')} Item #{i:03d}",
            "category": cat,
            "base_price": base_price,
            "cost": round(base_price * random.uniform(0.45, 0.72), 2),
        })
    return rows


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def simulate_orders(customers, products):
    """Simulate a streaming event log. Some records are intentionally dirty
    to make the pipeline's validation layer meaningful."""
    now = datetime.now().replace(microsecond=0)
    start = now - timedelta(days=DAYS)

    # Intraday traffic curve: peaks around lunch and evening (realistic seasonality)
    hour_weights = np.array([2, 1.4, 1, 0.7, 0.6, 0.9, 1.6, 3.2, 5.0, 5.6,
                             6.4, 8.2, 9.5, 8.0, 6.8, 7.4, 8.8, 9.6, 8.4, 6.6,
                             5.0, 3.8, 3.0, 2.4])
    hour_weights /= hour_weights.sum()

    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]
    pmap = {p["product_id"]: p for p in products}
    events = []

    for _ in range(N_ORDERS):
        ts = start + timedelta(
            days=int(np.random.choice(DAYS)),
            hours=int(np.random.choice(24, p=hour_weights)),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        if ts > now:
            ts = now - timedelta(seconds=random.randint(60, 86400))

        cust = random.choice(customer_ids)
        prod = pmap[random.choice(product_ids)]
        qty = int(np.random.choice([1, 1, 1, 2, 2, 3, 4], p=[.42, .18, .08, .16, .07, .06, .03]))
        discount = random.choice([0, 0, 0, 5, 10, 15, 25])
        unit_price = round(prod["base_price"] * random.uniform(0.92, 1.08), 2)
        total = round(unit_price * qty * (1 - discount / 100), 2)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order_created",
            "timestamp": ts.isoformat() + "Z",
            "order_id": f"ORD-{ts.strftime('%Y%m%d')}-{random.randint(100000, 999999)}",
            "customer_id": cust,
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": unit_price,
            "discount_pct": discount,
            "total_amount": total,
            "payment_method": random.choices(
                ["credit_card", "debit_card", "UPI", "wallet", "paypal"],
                weights=[38, 20, 18, 12, 12])[0],
            "status": random.choices(["completed", "completed", "completed", "returned", "cancelled"],
                                     weights=[80, 8, 6, 4, 2])[0],
            "device": random.choices(["mobile", "desktop", "tablet"], weights=[62, 30, 8])[0],
        }

        roll = random.random()
        if roll < 0.008:                      # dirty: missing amount
            del event["total_amount"]
        elif roll < 0.014:                    # dirty: negative quantity
            event["quantity"] = -event["quantity"]
        elif roll < 0.02:                     # dirty: null customer
            event["customer_id"] = None

        events.append(event)

    events.sort(key=lambda e: e["timestamp"])
    return events


def main():
    print("[generate] building dimensions...")
    customers = build_customers()
    products = build_products()
    write_csv(RAW_DIR / "customers.csv", list(customers[0].keys()), customers)
    write_csv(RAW_DIR / "products.csv", list(products[0].keys()), products)

    print("[generate] simulating event stream (JSON Lines)...")
    events = simulate_orders(customers, products)
    with open(RAW_DIR / "events.jsonl", "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    print(f"[done] customers={len(customers)}, products={len(products)}, events={len(events)}")


if __name__ == "__main__":
    main()
