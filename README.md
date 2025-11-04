# 🐝 Yelp Data Warehouse (Postgres + Metabase + Nginx)

## 1️⃣ Project Overview

This project implements a **complete, containerized Yelp data warehouse stack**:

```
Parquet Gold Layer → PostgreSQL (yelp_gold) → Metabase (BI) + Nginx (image proxy)
```

It’s designed for **local analytics, BI visualization, and data export reproducibility**.

### Goals

- Build an analytical gold dataset in **PostgreSQL**.
- Integrate a lightweight **Metabase BI** instance for dashboards and exploration.
- Serve static photo links via **Nginx** (for image previews inside Metabase).
- Provide **portable CSV/SQL exports** for anyone who doesn’t need the full stack.

---

## 2️⃣ Current Status (as of 2025-11-03)

| Module              | Status            | Notes |
| ------------------- | ---------------- | ------ |
| `dim_business`      | ✅ Loaded         | Includes business metadata |
| `dim_photo_files`   | ✅ Loaded         | Linked with image URLs (served via Nginx) |
| `dim_user`          | ✅ Loaded         | 1.98M users indexed |
| `mart_city_month`   | ✅ Loaded + View  | 103K rows (2005–2022) |
| `mart_photo_counts` | ✅ Loaded         | Label frequency summary |
| Views               | ✅ Created        | `vw_city_month_latest`, `vw_city_photo_top10`, `vw_user_review_buckets` |
| Metabase Connection | ✅ Verified       | Schema locked to `yelp_gold` |
| Export Snapshot     | ✅ CSV + SQL OK   | `exports/csv_*/` + `exports/sql_*/` ready |

---

## 3️⃣ Lightweight Replication / Data Access

If you **don’t want to clone or run containers**, you can still use this project’s exported data:

- **CSV exports** (ready to import to Excel / DuckDB / pandas)
- **SQL dumps** (schema + data for direct restore)

These are available in `exports/` and automatically generated via GitHub Actions.  
Each CI run uploads the latest dump as an **artifact** or Release asset.

---

## 4️⃣ Project Directory (Post-Trimmed Layout)

```
Yelp project/
├── clients/pyclient/yelp_data.py       # Data client (read gold layer)
├── conf/metrics.yaml                   # Metric definitions
├── data/gold/*.parquet                 # Parquet gold sources
├── pipelines/03_sync_gold_to_pg.py     # Sync parquet → PostgreSQL
├── scripts/
│   ├── export_pg_yelp_gold.py          # CSV+SQL exporter
│   ├── mb_one_click_login.py           # Local Metabase login helper
│   └── mb_refresh.py                   # Schema limiter + rescan
├── services/
│   ├── pg/init/00_schema.sql           # DB init DDL
│   └── jupyter/Dockerfile              # Notebook runtime
├── nginx/nginx.conf                    # Enables image serving
├── docker-compose.yml                  # Orchestrates all services
├── Makefile                            # Unified task interface
└── .github/workflows/ci.yml            # Minimal CI health + export
```

---

## 5️⃣ Makefile Quick Reference

| Command             | Description |
| ------------------- | ------------ |
| `make mb-refresh`   | Re-sync Metabase schema to `yelp_gold` |
| `make ui`           | One-click local Metabase login (auto-opens browser) |
| `make export`       | Run full CSV + SQL export |
| `make export-csv`   | Only export CSVs |
| `make export-sql`   | Only export SQL dumps |

---

## 6️⃣ BI & Image Integration

| Component | Purpose | Notes |
| ---------- | -------- | ----- |
| **Metabase** | Interactive BI tool | Auto-provisioned, schema locked |
| **Nginx** | Static image reverse proxy | Allows photo previews within BI cards |
| **PostgreSQL** | Data backend | Populated from Parquet gold layer |

This combination gives analysts **end-to-end visual context** — from tabular stats to images — without leaving the BI dashboard.

---

## 7️⃣ Minimal CI Workflow

The repository includes a preconfigured **GitHub Actions CI** (`.github/workflows/ci.yml`) that:

1. Spins up `postgres`, `metabase`, and `jupyter` containers.  
2. Waits for health OK.  
3. Runs `scripts/export_pg_yelp_gold.py --csv`.  
4. Uploads `exports/` as build artifacts.

This guarantees **data export reproducibility** on every commit.

---

## 8️⃣ How to Use (Local Quickstart)

```bash
# Start environment
docker compose up -d

# Refresh Metabase schema (auto login)
make mb-refresh && make ui

# Export CSV/SQL snapshots
make export
```

Then open:
```
http://localhost:3000  → Metabase BI
http://localhost:8080  → Nginx (image links)
```

---

## 9️⃣ Next Steps

* ✅ **Completed:** gold layer ingestion, Metabase configuration, CI health pipeline  
* 💡 **Next:** 
  * Publish dashboards directly in Metabase  
  * Add lightweight dashboard seed for demos  
  * Integrate DuckDB connector for local analysis  

---

**Author:** Yixin Wei  
**Last Updated:** 2025-11-03  
**Notes:** This version provides both **containerized BI integration** and **portable exports** for quick replication.
