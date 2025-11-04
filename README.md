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
| `dim_user`          | ✅ Loaded         | 1.98 M users indexed |
| `mart_city_month`   | ✅ Loaded + View  | 103 K rows (2005–2022) |
| `mart_photo_counts` | ✅ Loaded         | Label frequency summary |
| Views               | ✅ Created        | `vw_city_month_latest`, `vw_city_photo_top10`, `vw_user_review_buckets` |
| Metabase Connection | ✅ Verified       | Schema locked to `yelp_gold` |
| Export Snapshot     | ✅ CSV + SQL OK   | `exports/csv_*/` + `exports/sql_*/` ready |

---

## 3️⃣ Lightweight Replication / Data Access

If you **don’t want to run containers**, you can still use this project’s exported data:

- **CSV exports** → ready to load into Excel / DuckDB / pandas  
- **SQL dumps** → schema + data for direct restore

These are found under `exports/` and also automatically built by GitHub Actions CI.  
Each CI run uploads the latest dump as an **artifact** or **release asset**.

---

## 4️⃣ Project Layout (Post-Trimmed)

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

| Command              | Description |
| -------------------- | ------------ |
| `make up`            | Start all core containers (Postgres, Metabase, Jupyter, Nginx) |
| `make down`          | Stop and remove all running containers (volumes kept) |
| `make restart`       | Restart all core services |
| `make ps`            | Show running container status |
| `make logs`          | View recent logs from all services (last 200 lines) |
| `make logs-metabase` | View logs for a specific service (e.g. Metabase) |
| `make clean`         | Remove cache and compiled files (`__pycache__`, `*.pyc`) |
| `make help`          | Display all available Make targets with short descriptions |
| `make mb-refresh`    | Re-sync Metabase schema to `yelp_gold` |
| `make ui`            | One-click local Metabase login (auto-opens browser) |
| `make export`        | Run full CSV + SQL export (includes schema refresh) |
| `make export-csv`    | Export only CSVs |
| `make export-sql`    | Export only SQL dumps |

---

## 6️⃣ BI & Image Integration

| Component | Purpose | Notes |
| ---------- | -------- | ----- |
| **Metabase** | Interactive BI tool | Auto-provisioned, schema locked |
| **Nginx** | Static image proxy | Enables photo previews inside dashboards |
| **PostgreSQL** | Data backend | Populated from Parquet gold layer |

This combo provides analysts with **end-to-end visual context** — from tabular stats to images — without leaving the BI dashboard.

---

## 7️⃣ Minimal CI Workflow

The repo includes `.github/workflows/ci.yml`, which:

1. Spins up `postgres`, `metabase`, and `jupyter` containers.  
2. Waits for health checks.  
3. Runs `scripts/export_pg_yelp_gold.py --csv`.  
4. Uploads `exports/` as build artifacts.

This ensures **data export reproducibility** on every commit.

---

## 8️⃣ 🧩 How to Use (Full Local Clone Setup)

### 🔹 Step 1 — Clone the Repository
```bash
git clone https://github.com/Rebas9512/yelp_project.git
cd yelp_project
```

### 🔹 Step 2 — Start All Containers
```bash
docker compose up -d
```

### 🔹 Step 3 — Refresh Metabase & Open UI
```bash
make mb-refresh && make ui
```

This command will:
- Log into Metabase (`admin@yelp.local` / `Metabase!2025`)
- Auto-open your browser at `http://localhost:3000`

### 🔹 Step 4 — Export CSV / SQL Snapshots
```bash
make export
```

Then access:
```
http://localhost:3000   →  Metabase BI
http://localhost:8080   →  Nginx (image links)
```

---

## 9️⃣ Next Steps

* ✅ **Completed:** gold-layer ingestion, Metabase config, CI health pipeline  
* 💡 **Next:**
  * Publish dashboards directly in Metabase  
  * Add lightweight dashboard seed for demos  
  * Integrate DuckDB connector for local analysis  

---

**Repository:** [github.com/Rebas9512/yelp_project](https://github.com/Rebas9512/yelp_project)  
**Author:** Yixin Wei  
**Last Updated:** 2025-11-03  
**Notes:** Provides both **containerized BI integration** and **portable exports** for quick replication.
