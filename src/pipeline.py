"""
ETL Pipeline - E-Commerce Event Stream
======================================
Extract  : reads raw JSONL event stream + CSV dimensions
Transform: schema validation, dedup, dirty-record quarantine, enrichment,
           business-day aggregations (revenue, AOV, category share, hourly load)
Load     : writes curated Parquet-style aggregates (CSV/JSON) to data/processed/

Run: python src/pipeline.py
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW = BASE_DIR / "data" / "raw"
OUT = BASE_DIR / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("etl")

REQUIRED_FIELDS = ["event_id", "timestamp", "order_id", "product_id",
                   "quantity", "unit_price", "total_amount"]


# ---------------------------------------------------------------- EXTRACT
def extract():
    log.info("EXTRACT | reading raw events + dimensions")
    events = pd.read_json(RAW / "events.jsonl", lines=True)
    customers = pd.read_csv(RAW / "customers.csv")
    products = pd.read_csv(RAW / "products.csv")
    log.info(f"EXTRACT | {len(events):,} events | {len(customers):,} customers | {len(products)} products")
    return events, customers, products


# ---------------------------------------------------------------- TRANSFORM
def transform(events, customers, products):
    n_raw = len(events)

    # -- validation layer: quarantine records failing the data contract
    missing = events[REQUIRED_FIELDS].isna().any(axis=1)
    bad_qty = events["quantity"].notna() & (events["quantity"] <= 0)
    reject_mask = missing | bad_qty
    quarantined = events[reject_mask].copy()
    quarantined["reject_reason"] = np.where(missing[reject_mask], "missing_fields",
                                            "invalid_quantity")
    quarantined.to_json(OUT / "_quarantine.jsonl", orient="records", lines=True)
    clean = events[~reject_mask].copy()
    log.info(f"VALIDATE | quarantined {len(quarantined):,} bad records "
             f"({len(quarantined)/n_raw:.2%}) | kept {len(clean):,}")

    # -- dedup on event_id (idempotent replay safety)
    before = len(clean)
    clean = clean.drop_duplicates(subset="event_id")
    log.info(f"DEDUP | removed {before - len(clean):,} duplicate events")

    # -- type casting + enrichment (star-schema style join)
    clean["timestamp"] = pd.to_datetime(clean["timestamp"])
    clean = clean.merge(customers, on="customer_id", how="left") \
                 .merge(products, on="product_id", how="left")
    clean["date"] = clean["timestamp"].dt.date.astype(str)
    clean["hour"] = clean["timestamp"].dt.hour
    clean["month"] = clean["timestamp"].dt.strftime("%Y-%m")
    clean["net_revenue"] = (clean["total_amount"] * 0.94).round(2)   # after fees
    clean["profit"] = ((clean["unit_price"] - clean["cost"]) *
                       clean["quantity"]).round(2)
    log.info("ENRICH | joined customer + product dims, derived revenue/profit columns")

    completed = clean[clean["status"] == "completed"]
    return clean, completed


def aggregate(clean, completed):
    agg = {}

    kpis = {
        "total_events": int(len(clean)),
        "total_orders": int(completed["order_id"].nunique()),
        "gross_revenue": float(completed["total_amount"].sum()),
        "net_revenue": float(completed["net_revenue"].sum()),
        "total_profit": float(completed["profit"].sum()),
        "avg_order_value": float(completed.groupby("order_id")["total_amount"].sum().mean()),
        "unique_customers": int(completed["customer_id"].nunique()),
        "return_rate_pct": float((clean["status"] == "returned").mean() * 100),
        "cancel_rate_pct": float((clean["status"] == "cancelled").mean() * 100),
        "quarantined_records": int(len(pd.read_json(OUT / "_quarantine.jsonl", lines=True)))
                               if (OUT / "_quarantine.jsonl").exists() else 0,
    }
    agg["kpis"] = kpis

    daily = completed.groupby("date").agg(
        revenue=("total_amount", "sum"),
        orders=("order_id", "nunique"),
        profit=("profit", "sum"),
        aov=("total_amount", "mean"),
    ).reset_index().round(2)
    daily["revenue_ma7"] = daily["revenue"].rolling(7).mean().round(2)
    agg["daily_trend"] = daily.to_dict("records")

    cat = completed.groupby("category").agg(
        revenue=("total_amount", "sum"),
        units=("quantity", "sum"),
        profit=("profit", "sum"),
        orders=("order_id", "nunique"),
    ).sort_values("revenue", ascending=False).reset_index().round(2)
    cat["margin_pct"] = (cat["profit"] / cat["revenue"] * 100).round(1)
    agg["by_category"] = cat.to_dict("records")

    top_products = completed.groupby(["product_name", "category"]) \
        .agg(units=("quantity", "sum"), revenue=("total_amount", "sum")) \
        .nlargest(10, "revenue").reset_index().round(2)
    agg["top_products"] = top_products.to_dict("records")

    hourly = completed.groupby("hour").agg(
        orders=("order_id", "nunique"), revenue=("total_amount", "sum")
    ).reindex(range(24), fill_value=0).reset_index().round(2)
    agg["hourly_load"] = hourly.to_dict("records")

    geo = completed.groupby(["country", "city"]).agg(
        revenue=("total_amount", "sum"), customers=("customer_id", "nunique")
    ).reset_index().round(2).sort_values("revenue", ascending=False)
    agg["geo"] = geo.head(12).to_dict("records")

    pay = completed["payment_method"].value_counts().to_dict()
    agg["payment_mix"] = pay

    device = completed.groupby("device")["total_amount"].sum().round(2).to_dict()
    agg["device_mix"] = device

    monthly = completed.groupby("month").agg(
        revenue=("total_amount", "sum"), orders=("order_id", "nunique")).reset_index().round(2)
    agg["monthly"] = monthly.to_dict("records")

    return agg


# ---------------------------------------------------------------- LOAD
def load(agg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {"generated_at": ts, **agg}
    with open(OUT / "aggregates.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    # also persist flat tables for BI tools
    pd.DataFrame(agg["daily_trend"]).to_csv(OUT / "fact_daily_sales.csv", index=False)
    pd.DataFrame(agg["by_category"]).to_csv(OUT / "dim_category_performance.csv", index=False)
    log.info(f"LOAD   | wrote aggregates.json + fact/dim tables -> {OUT}")


def main():
    t0 = datetime.now()
    events, customers, products = extract()
    clean, completed = transform(events, customers, products)
    agg = aggregate(clean, completed)
    load(agg)
    log.info(f"PIPELINE OK in {(datetime.now()-t0).total_seconds():.1f}s | "
             f"gross revenue ${agg['kpis']['gross_revenue']:,.0f} | "
             f"AOV ${agg['kpis']['avg_order_value']:.2f}")


if __name__ == "__main__":
    main()
