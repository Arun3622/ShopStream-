# 🛒 ShopStream — Real-Time E-Commerce Analytics Pipeline

End-to-end **data engineering project**: a simulated streaming e-commerce event pipeline with data-quality enforcement, dimensional enrichment, and an animated real-time analytics dashboard.

![status](https://img.shields.io/badge/status-production--style-brightgreen) ![python](https://img.shields.io/badge/Python-3.10%2B-blue) ![pandas](https://img.shields.io/badge/pandas-ETL-informational)

## 🏗️ Architecture

```
┌──────────────────┐   ┌───────────────────┐   ┌────────────────────┐   ┌──────────────┐
│  Event Simulator │──▶│  Validation Layer │──▶│  Enrichment (dims) │──▶│ Aggregations │
│  (Kafka-style    │   │  • schema check   │   │  • customer dim    │   │  • daily rev │
│   JSONL stream)  │   │  • dedup          │   │  • product dim     │   │  • category  │
│  30k events      │   │  • quarantine DQ  │   │  • derived metrics │   │  • hourly    │
└──────────────────┘   └───────────────────┘   └────────────────────┘   └──────┬───────┘
                                                                               ▼
                                                                      ┌────────────────┐
                                                                      │ Live Dashboard │
                                                                      │ (Chart.js +    │
                                                                      │  animations)   │
                                                                      └────────────────┘
```

## 📦 Pipeline Stages

| Stage | What happens | Output |
|---|---|---|
| **Extract** | Read JSONL event stream + CSV dimension tables | `events.jsonl` |
| **Validate** | Data contract check, dedup on `event_id`, quarantine dirty records (~2% injected dirt) | `_quarantine.jsonl` |
| **Transform** | Join customer/product dims, derive net revenue & profit, time dimensions | wide fact table |
| **Aggregate** | Daily revenue + 7-day MA, category share, top products, hourly traffic, geo, payment mix | `aggregates.json` |
| **Load** | Persist aggregates + fact/dim CSVs for BI tools | `data/processed/` |

## 🚀 Run It

```bash
pip install -r requirements.txt
python run_all.py
# then open dashboard/index.html in your browser 🎉
```

## 📊 Dashboard Features

- Animated count-up KPI cards (revenue, orders, profit, AOV, DQ stats)
- Revenue trend line chart with **7-day moving average**
- Category revenue donut with hover explode
- Top-products horizontal bar chart
- Hourly traffic load chart with staggered bounce animation
- Scrolling **live-orders ticker tape**
- Glassmorphism UI + gradient background animation

## 🧠 Data Engineering Concepts Demonstrated

- Streaming event simulation (append-only log, idempotent replay)
- **Data quality gates** — validation layer + quarantine pattern
- Deduplication / exactly-once semantics
- Star-schema style enrichment (fact + dimensions)
- Rolling-window analytics (7-day MA)
- Medallion-style layout: raw → validated → curated

## 📁 Structure

```
project-1-ecommerce-realtime-analytics/
├── run_all.py              # one-command orchestration
├── requirements.txt
├── src/
│   ├── generate_data.py    # event simulator (30k events, 90 days)
│   ├── pipeline.py         # ETL: validate → transform → aggregate → load
│   └── build_dashboard.py  # injects aggregates into HTML template
├── dashboard/
│   ├── template.html       # animated dashboard template
│   └── index.html          # generated (open this!)
└── data/
    ├── raw/                # events.jsonl, customers.csv, products.csv
    └── processed/          # aggregates.json, quarantine, fact/dim tables
```

## 💬 LinkedIn Caption (copy-paste ready)

> 🚀 Just built a Real-Time E-Commerce Analytics Pipeline from scratch!
>
> What's inside:
> ✅ Simulated streaming event source — 30,000 orders across 90 days
> ✅ Data quality layer that quarantines bad records automatically
> ✅ Star-schema style enrichment with customer & product dimensions
> ✅ Rolling-window revenue analytics (7-day moving average)
> ✅ Fully animated live dashboard built with Chart.js
>
> Key numbers from the last run: 💰 $XXM gross revenue · XX,XXX orders · X.XX% data rejection rate caught by the DQ layer
>
> Tech stack: Python, Pandas, NumPy, JSONL streaming, Chart.js
>
> #DataEngineering #ETL #Analytics #Python #DataQuality #BuildInPublic

## 👤 Author

**Arun Prajapati** — Data Engineer
- GitHub: [github.com/Arun3622](https://github.com/Arun3622)
- Repository: [github.com/Arun3622/ShopStream-](https://github.com/Arun3622/ShopStream-)

