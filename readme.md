# High-Performance Hybrid ELT Data Pipeline & Unified Analytics Engine

**Phase 1 & Phase 2**

> Enterprise-Grade Distributed Big Data Ingestion (30M+ Orders), 8-Rule Quality Validation (21 Normalization & Repair Patterns), Anomaly Quarantine, Idempotent Upsert, Indexed Operational Queries, Independent Aggregations, Incremental Materialized Views, Scheduled Jobs, and Unified FastAPI Command Center.

**Lead Data Engineer & Architect:** Eng. Nader Alshawki (@Naderalshawki)  
**Course:** Big Data Engineering (Practical) — Scope: Phase 1 (Midterm Core ELT Pipeline) & Phase 2 (Final Analytical Extensions & Unified API)

---

## 📌 1. Project Overview & End-to-End Architecture 🏗️

This repository implements an end-to-end Hybrid Big Data ELT & Real-Time Analytics Platform using Apache PySpark 3.5, Pure Python Batch Streaming, MongoDB 7.0 Aggregation Pipelines, APScheduler, and FastAPI (**100% Native Execution — Zero pandas / numpy dependencies**).

### Phase 1 — Core Hybrid ELT Pipeline & Data Quality Engine

Processes datasets of any scale (20K evaluation datasets, 100K sample files, and 30,000,000+ record production datasets ~12.65 GB) via an intelligent File Router:

- `<= 200 MB` → Python Batch Streaming with 5,000-row live terminal progress.
- `> 200 MB` → PySpark Distributed Mode with offline local JAR support.

The pipeline enforces 100% verbatim raw fidelity in `orders_raw`, applies 8 standardization rules covering 21 error and repair patterns with a full Audit Trail (`corrections`), isolates unrecoverable anomalies in `orders_quarantine`, and performs Idempotent Upserts into `orders_validated` with clean database reset (`count_documents({})`) on fresh evaluation runs.

### Phase 2 — Operational Queries, Indexes, Aggregations, Incremental MVs, Jobs & Unified API

Extends the core pipeline with 5 practical operational queries, 3 custom indexes (including Compound Indexes) verified with before/after `explain("executionStats")`, 5 independent MongoDB Aggregation Reports, 2 Incremental Materialized Views (`daily_sales_summary` & `top_products_summary`), 2 Scheduled Background Jobs with execution audit logging, an interactive CLI Launcher, a Cyber Terminal Showcase, a Web Command Center Dashboard (`/`), and a unified FastAPI Swagger interface (`/docs`).

### End-to-End Processing Architecture

```text
+-----------------------------------------------------------------------------------+
|                                  RAW CSV INPUT                                    |
|    (30M Production Dataset ~12.65 GB | 100K Sample | 20K Evaluation Dataset)      |
+-----------------------------------------+-----------------------------------------+
                                          |
                          [ Smart File Router Engine (6.2) ]
              +---------------------------+---------------------------+
              | (Size <= 200 MB)                                      | (Size > 200 MB)
              v                                                       v
  [ Python Batch Streaming ]                             [ PySpark Distributed Mode ]
  (5,000-Row Live Progress)                              (Offline Local JARs Ready)
              +---------------------------+---------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                     STAGE 1: RAW INGESTION & METADATA ENRICHMENT                  |
|  - Explicit StringType Schema Enforcement (100% Raw Verbatim Fidelity)            |
|  - Audit Metadata: (run_id, source_file, source_row_number, ingested_at)          |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
                              [( MongoDB: orders_raw )]
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|               STAGE 2: QUALITY VALIDATION, CLASSIFICATION & UPSERT (ELT)          |
|  - 8 Quality Rules (21 Normalization & Repair Patterns): Arabic Digits,           |
|    Word-Numbers, Currency (YER), Phone (+967), Email Syntax Repair, ISO-8601      |
|    Dates, Status Mapping, Items JSON Numeric Parsing & Invoice Total Recalculation|
+---------------------+---------------------------------------+---------------------+
                      |                                       |
        (Passed Valid / Repaired)                   (Corrupted / Unrecoverable)
                      |                                       |
                      v                                       v
+-------------------------------------------+   +-----------------------------------+
|        COLLECTION: orders_validated       |   |    COLLECTION: orders_quarantine  |
|  - Valid Records + Corrected (Audit Trail)|   |  - Missing Mandatory Keys         |
|  - Idempotent Upsert (order_id Unique)    |   |  - Impossible Dates / Negatives   |
|  - 3 Custom Indexes (Compound & Single)   |   |  - Non-Parsable Items JSON        |
+---------------------+---------------------+   +-----------------------------------+
                      |
                      v
+-----------------------------------------------------------------------------------+
|            STAGE 3 (PHASE 2): ANALYTICS, INCREMENTAL MVs, JOBS & FASTAPI          |
|  - 5 Operational Queries + Before/After explain("executionStats") Benchmark       |
|  - 5 Independent Aggregation Reports (City, Products, Customers, Period, Status)  |
|  - 2 Incremental Materialized Views (daily_sales_summary, top_products_summary)   |
|  - 2 APScheduler Jobs (Auto & Manual Run + Execution Audit Logs)                  |
|  - Unified FastAPI Server (10 Official Routes + Independent Routes + /docs + /)   |
+-----------------------------------------------------------------------------------+
```

---

## ✅ 2. Full Technical Implementation Checklist (Phase 1 & Phase 2)

### Phase 1: Core Hybrid ELT Pipeline & Data Quality Engine

- **Small & Medium Dataset Automatic Routing:** Selects Python Batch automatically when file size `<= 200 MB` (`chunk_size = 5,000` for real-time terminal progress).
- **Large Dataset Automatic Routing:** Selects PySpark Distributed automatically when file size `> 200 MB` (supports offline local `.ivy2/jars` execution).
- **100% Raw Ingestion First:** All records land first in `orders_raw` with full metadata (`run_id`, `source_file`, `ingested_at`).
- **8 Data Quality & Cleaning Rules (21 Normalization & Repair Patterns in `src/quality_rules.py`).**

#### The 8 Quality Rules

1. **Rule 1 — Arabic-Indic Numerals:** Converts `٥٠٠٠` → `5000` across all fields and nested JSON.
2. **Rule 2 — Arabic Word-Numbers & Currency:** Parses `خمسة آلاف ريال` → `5000.0` & `YER`.
3. **Rule 3 — Thousands Separators & Decimals:** Cleans `15,000.50` → `15000.50`.
4. **Rule 4 — Currency Normalization:** Standardizes `ريال يمني`, `ر.ي`, `YR`, `ريال` → `YER`.
5. **Rule 5 — Yemeni Phone E.164 Format:** Standardizes local variants `771234567` → `+967771234567`.
6. **Rule 6 — Email Syntax Repair:** Fixes `user@@mail..com` → `user@mail.com`.
7. **Rule 7 — ISO-8601 Date Standardization:** Normalizes mixed dates → `YYYY-MM-DDTHH:MM:SSZ` and isolates impossible calendar dates.
8. **Rule 8 — Order Status, City Mapping & Items JSON Recalculation:** Standardizes Arabic/English statuses (`مؤكد` → `confirmed`), converts string numbers inside `items_json`, and recalculates `total_amount` against item subtotals + `delivery_cost`.

- **Audit Trail on Corrected Records:** Every repaired field is logged inside the `corrections` array (`field`, `original_value`, `corrected_value`, `rule_code`).
- **Quarantine Isolation with Reasons:** Unrecoverable records are isolated in `orders_quarantine` with explicit `error_code` and `quarantine_reason`.
- **Section 6.11 Mathematical Consistency Equation Verified:** `Raw == Valid + Corrected + Quarantine` (100% balanced for every `run_id`).
- **Clean Reset & Zero-Duplication Idempotent Upsert:** `_execute_ingestion_gate` performs a clean reset before fresh test file ingestion, enforces a Unique Index on `order_id`, and uses exact `count_documents({})` queries to prevent stale cache reads or cross-batch accumulation.

### Phase 2: Queries, Indexes, Aggregations, Incremental MVs, Scheduled Jobs & Unified API

1. **Queries, Indexes & Explain (`src/queries_and_indexes.py`):** 5 practical dynamic queries, 3 indexes (including 2 Compound Indexes following the ESR rule), and before/after `explain("executionStats")` comparison for 3 queries saved to `reports/index_explain_report.json`.
2. **Independent Aggregation Reports (`src/aggregations.py`):** 5 independent MongoDB Aggregation Reports (`sales_by_city`, `top_products`, `top_customers`, `sales_by_period`, `orders_by_status`) with dedicated Python functions, independent API routes, enum dropdowns, and separate JSON output files in `reports/`.
3. **Materialized Views & Incremental Refresh (`src/materialized_views.py`):** 2 Materialized Views (`daily_sales_summary` keyed by `period_date` & `top_products_summary` sorted by `total_product_revenue`) built on aggregations with true Incremental Refresh via `mv_watermarks` (`last_object_id` & `last_updated_at`) and atomic `$inc` delta merges (`initial_seed` / `incremental_delta` / `incremental_noop` with 0 delta when no new data arrives).
4. **Scheduled Jobs (`src/scheduler.py`):** 2 scheduled jobs managed by APScheduler supporting both periodic execution and immediate manual triggering, logging `start_time`, `end_time`, `duration_seconds`, and `status` (`SUCCESS/FAILED`) to `job_execution_logs` and `reports/jobs_execution_history.json`.
5. **Unified FastAPI Interface (`src/api.py`):** All 10 official JSON endpoints + explicit independent routes for each query, report, view, and job, auto-expanded Swagger UI at `/docs` (`"docExpansion": "list"`), Enum dropdowns, and Web Command Center at `/`.
6. **Dynamic Portability & Clean Repository:** Zero hardcoded test outcomes, UTF-8 `requirements.txt` free of forbidden libraries, and `.env.example` template.

---

## 📁 3. Project Directory Structure 📂

```text
midterm-data-pipeline/
├── .env.example                           # Clean environment variable template (no sensitive secrets)
├── config/
│   └── settings.py                        # Dynamic settings loader (.env + Phase 1 & Phase 2 collections)
├── data/
│   ├── orders_huge_mixed_quality.csv      # Production dataset (30M records ~12.65 GB)
│   ├── orders_sample.csv                  # Standard sample dataset (100,000 rows)
│   └── 01_student_test_small.csv          # Official evaluation test dataset (20,000 rows)
├── reports/
│   ├── results.json                       # Stage 1 & 2 ELT execution metrics & consistency report
│   ├── results.md                         # Executive Markdown summary (Phase 1 & Phase 2)
│   ├── index_explain_report.json          # Before vs. After explain("executionStats") benchmark
│   ├── aggregations_report.json           # Combined 5 MongoDB analytical aggregation reports
│   ├── aggregation_sales_by_city.json     # [Independent Report 1] Sales by City
│   ├── aggregation_top_products.json      # [Independent Report 2] Top Products
│   ├── aggregation_top_customers.json     # [Independent Report 3] Top Customers
│   ├── aggregation_sales_by_period.json   # [Independent Report 4] Sales by Period
│   ├── aggregation_orders_by_status.json  # [Independent Report 5] Orders by Status
│   ├── materialized_views_report.json     # Incremental Materialized Views & Watermarks state
│   ├── periodic_analytics_report.json     # Output generated by periodic scheduled report job
│   └── jobs_execution_history.json        # Execution audit trail for scheduled & manual jobs
├── src/
│   ├── main.py                            # Master End-to-End ELT Pipeline & Server entrypoint
│   ├── file_router.py                     # [Phase 1] Automatic workload router (Python Batch vs. PySpark)
│   ├── batch_loader.py                    # [Phase 1] High-speed Python Streaming Batch loader (<= 200MB)
│   ├── spark_loader.py                    # [Phase 1] Distributed PySpark Raw Ingestion loader (> 200MB)
│   ├── quality_rules.py                   # [Phase 1] 8 Data Quality Rules (21 Normalization Patterns)
│   ├── elt_pipeline.py                    # [Phase 1] Stage 2 validation, classification, quarantine & Upsert
│   ├── queries_and_indexes.py             # [Phase 2] 5 Operational Queries + 3 Indexes + Explain
│   ├── aggregations.py                    # [Phase 2] 5 Independent MongoDB Aggregation Pipelines
│   ├── materialized_views.py              # [Phase 2] 2 Materialized Views with Incremental Refresh
│   ├── scheduler.py                       # [Phase 2] 2 Scheduled Jobs + Execution Audit Logger
│   ├── api.py                             # [Phase 2] Unified FastAPI Server (/docs & / Dashboard)
│   ├── showcase.py                        # 3D Cyber Terminal Visual Telemetry Showcase
│   └── cli_launcher.py                    # Interactive Rich Terminal Command Center (Phase 1 + 2)
├── tests/
│   ├── test_cleaning_rules.py             # Unit tests for all 8 data cleaning rules
│   ├── test_classification.py             # Tests for Valid, Corrected, and Quarantine routing
│   ├── test_idempotency.py                # Idempotent Upsert & zero-duplicate verification
│   └── test_phase2_api.py                 # Automated test suite for all Phase 2 API endpoints (6/6 PASSED)
├── requirements.txt                       # Pure UTF-8 Python dependencies (Zero Pandas/NumPy)
└── README.md                              # Comprehensive installation, operation & architecture guide
```

---

## ⚙️ 4. Installation & Environment Setup 💻

### Prerequisites

- **Python:** 3.10 – 3.12
- **MongoDB Server:** 6.0+ or 7.0+ running on `mongodb://localhost:27017`
- **Java (OpenJDK 17/21):** Required only when running PySpark mode on files `> 200 MB`

### Step-by-Step Setup (Windows PowerShell)

```powershell
# 1. Navigate to the project directory
cd C:\lec\midterm-data-pipeline

# 2. Create virtual environment (if not already created)
python -m venv .venv

# 3. Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# 4. Install all required dependencies
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 5. Copy environment template to .env
Copy-Item .env.example .env
```

---

## 🌐 5. Running the Unified FastAPI Server & Web Dashboard (`src/api.py`)

Start the unified API server using any of the commands below:

### Method 1 — Fast Startup with 3D Terminal Showcase & Auto-Browser Launch

```powershell
.\.venv\Scripts\python.exe -m src.main --server-only
```

### Method 2 — Direct Module Execution

```powershell
.\.venv\Scripts\python.exe -m src.api
```

### Method 3 — Using Uvicorn Directly

```powershell
.\.venv\Scripts\python.exe -m uvicorn src.api:app --host 0.0.0.0 --port 8000
```

Once running, open your browser at:

- **Swagger UI (Official Automated Evaluation Interface):** <http://localhost:8000/docs>
- **Interactive Web Command Center Dashboard:** <http://localhost:8000/>
- **System Health & Exact Live Counts:** <http://localhost:8000/health>

### Unified API Endpoints Reference (10 Official Routes + Independent Routes)

| Method & Endpoint | Description & Functionality | Sample cURL / Request |
|---|---|---|
| `GET /health` | Verifies MongoDB connectivity and returns exact `count_documents({})` across all Phase 1 & Phase 2 collections. | `curl http://localhost:8000/health` |
| `POST /ingest` | Triggers the Phase 1 Midterm ingestion gate (`file_router -> orders_raw -> elt_pipeline -> Incremental MVs -> Reports`). | `curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d "{}"` |
| `POST /ingest/upload` | Uploads a CSV file directly via browser/Swagger and executes the full hybrid pipeline immediately. | `curl -X POST http://localhost:8000/ingest/upload -F "file=@data/01_student_test_small.csv"` |
| `POST /indexes` | Creates the 3 custom indexes (including Compound Indexes) and executes before/after `explain("executionStats")` on 3 queries. | `curl -X POST http://localhost:8000/indexes` |
| `GET /queries` | Lists all 5 operational queries, their dynamic filters, and serving indexes. | `curl http://localhost:8000/queries` |
| `GET /queries/{name}` | Executes a specific operational query by name (plus 5 dedicated independent query endpoints). | `curl "http://localhost:8000/queries/city_status_recent_orders?limit=10"` |
| `GET /aggregations` | Lists all 5 analytical aggregation reports available in the system. | `curl http://localhost:8000/aggregations` |
| `GET /aggregations/{name}` | Runs any aggregation report by name (plus 5 independent routes: `/aggregations/sales_by_city`, `/top_products`, `/top_customers`, `/sales_by_period`, `/orders_by_status`). | `curl "http://localhost:8000/aggregations/sales_by_city?limit=10"` |
| `POST /refresh-mv` | Performs an Incremental Refresh of `daily_sales_summary` and `top_products_summary` using `mv_watermarks`. | `curl -X POST http://localhost:8000/refresh-mv -H "Content-Type: application/json" -d "{\"force_full\": false}"` |
| `POST /refresh-mv/daily_sales_summary` | Independently refreshes `daily_sales_summary` incrementally via watermark delta. | `curl -X POST http://localhost:8000/refresh-mv/daily_sales_summary` |
| `POST /refresh-mv/top_products_summary` | Independently refreshes `top_products_summary` incrementally via watermark delta. | `curl -X POST http://localhost:8000/refresh-mv/top_products_summary` |
| `GET /materialized-views/{name}` | Reads `daily_sales_summary` or `top_products_summary` independently. | `curl "http://localhost:8000/materialized-views/daily_sales_summary?limit=15"` |
| `GET /jobs` | Lists all scheduled background jobs, their schedules, and recent execution audit logs. | `curl http://localhost:8000/jobs` |
| `POST /jobs/{name}/run` | Immediately triggers a scheduled job manually (`refresh_materialized_views_job` or `generate_periodic_report_job`) and logs start/end timestamps & status. | `curl -X POST http://localhost:8000/jobs/refresh_materialized_views_job/run` |

---

## 🖥️ 6. Running via CLI (Individual Modules & Interactive Command Center)

Every component in Phase 1 and Phase 2 can be executed independently from the terminal:

```powershell
# 1. Render 3D Cyber Terminal Showcase
.\.venv\Scripts\python.exe -m src.showcase

# 2. Launch Interactive Rich Terminal Command Center (Menu for Phase 1 + Phase 2)
.\.venv\Scripts\python.exe -m src.cli_launcher

# 3. Run End-to-End ELT Pipeline (Default or Custom CSV Path)
.\.venv\Scripts\python.exe -m src.main
.\.venv\Scripts\python.exe -m src.main data/01_student_test_small.csv

# 4. Create 3 Indexes & Generate Before/After explain("executionStats") Report
.\.venv\Scripts\python.exe -m src.queries_and_indexes

# 5. Run All 5 Aggregation Reports (or each of the 5 reports independently)
.\.venv\Scripts\python.exe -m src.aggregations --report all --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report sales_by_city --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report top_products --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report top_customers --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report sales_by_period --limit 10
.\.venv\Scripts\python.exe -m src.aggregations --report orders_by_status --limit 10

# 6. Run Incremental Materialized Views Refresh
.\.venv\Scripts\python.exe -m src.materialized_views

# 7. Execute Scheduled Jobs Manually & Log Audit Trail
.\.venv\Scripts\python.exe -m src.scheduler
```

---

## 🔍 7. Phase 2 Deep Dive: Queries, Indexes, Explain, Aggregations, MVs & Jobs

### 7.1 The 5 Practical Operational Queries (`src/queries_and_indexes.py`)

- `city_status_recent_orders`: Retrieves the most recent orders filtered by city and status, sorted by `order_date` descending.
- `customer_order_history`: Retrieves the complete chronological order history for a specific `customer_id`.
- `high_value_orders_range`: Filters orders within a specific financial bracket (`total_amount` between `$gte` and `$lte`), sorted descending.
- `lookup_by_customer_phone`: Performs rapid order lookup by standardized Yemeni phone number (`+9677XXXXXXXX`).
- `orders_by_product_item`: Searches inside the nested `items` array (`items.item_name`) to find orders containing a specific product/SKU.

### 7.2 The 3 Indexes & `explain("executionStats")` Benchmark (Before vs. After)

| Index Name | Keys & Index Type | Target Query | Before Index (COLLSCAN) | After Index (IXSCAN) | Why Chosen & Measured Impact |
|---|---|---|---|---|---|
| `idx_city_status_order_date` | `(city: 1, status: 1, order_date: -1)`<br>**Compound Index** | `city_status_recent_orders` | Stage: `COLLSCAN`<br>Docs Examined: `95,136`<br>Time: `168 ms` | Stage: `IXSCAN`<br>Docs Examined: `25`<br>Time: `12 ms` | Follows the ESR (Equality, Sort, Range) rule. Eliminates full collection scan and in-memory sort, reducing examined docs by 99.97% and latency by 92.8%. |
| `idx_customer_id_order_date` | `(customer_id: 1, order_date: -1)`<br>**Compound Index** | `customer_order_history` | Stage: `COLLSCAN`<br>Docs Examined: `95,136`<br>Time: `145 ms` | Stage: `IXSCAN`<br>Docs Examined: `1`<br>Time: `12 ms` | Enables direct B-Tree seek for a customer's chronological orders. Reduces examined documents from 95,136 to 1 (99.99% reduction). |
| `idx_total_amount` | `(total_amount: -1)`<br>**Single Field Index** | `high_value_orders_range` | Stage: `COLLSCAN`<br>Docs Examined: `95,136`<br>Time: `196 ms` | Stage: `IXSCAN`<br>Docs Examined: `25`<br>Time: `10 ms` | Accelerates range queries (`$gte`/`$lte`) and descending sort on `total_amount`, cutting query time from 196 ms to 10 ms (94.9% faster). |

### 7.3 The 5 Independent Aggregation Reports (`src/aggregations.py`)

- `sales_by_city`: Computes `total_orders`, `total_revenue`, `avg_order_value`, and `total_delivery_cost` grouped by city.
- `top_products`: Unwinds the `$items` array (`$unwind`) to calculate `total_quantity_sold`, `total_product_revenue`, and `order_count` per product.
- `top_customers`: Ranks top customers by `total_spent`, `orders_count`, and `last_order_date`.
- `sales_by_period`: Aggregates daily order volume (`total_orders`), `daily_revenue`, and `avg_order_amount` by `period_date` (`YYYY-MM-DD`).
- `orders_by_status`: Analyzes order count distribution (`orders_count`), `status_revenue`, and `avg_amount` across order statuses.

### 7.4 Materialized Views & Incremental Refresh (`src/materialized_views.py`)

- `daily_sales_summary`: Materialized collection storing pre-aggregated daily revenue, order counts, and delivery costs keyed by `period_date`.
- `top_products_summary`: Materialized collection storing cumulative product quantities sold, `total_product_revenue`, and order frequency.

#### How Incremental Refresh Works

1. Each view tracks its latest processed `last_object_id` and `last_updated_at` watermark inside the `mv_watermarks` collection.
2. On refresh (`POST /refresh-mv`), the engine queries only newly inserted or updated documents in `orders_validated` after the watermark.
3. The delta aggregation results are merged atomically into the target view using `UpdateOne(..., {"$inc": {...}, "$set": {...}}, upsert=True)` without dropping or rebuilding historical data.
4. If no new records exist since the last watermark, it completes in `< 30 ms` with `refresh_mode: "incremental_noop"` and `delta_records_processed: 0`.

### 7.5 Scheduled Jobs & Execution Audit Logging (`src/scheduler.py`)

- `refresh_materialized_views_job` (**Scheduled every 15 minutes**): Automatically runs incremental refresh on both Materialized Views.
- `generate_periodic_report_job` (**Scheduled every 30 minutes**): Executes key analytical aggregations and exports a consolidated JSON snapshot to `reports/periodic_analytics_report.json`.
- **Audit Logging:** Every scheduled or manual run (`POST /jobs/{name}/run`) logs `job_name`, `trigger_type`, `start_time`, `end_time`, `duration_seconds`, `status` (`SUCCESS/FAILED`), and `details` to both MongoDB (`job_execution_logs`) and `reports/jobs_execution_history.json`.

---

## 🧪 8. Automated Testing Suite (PyTest - Phase 1 & Phase 2) 🩺

### Run Phase 2 API, Queries, Indexes/Explain, Aggregations, Incremental MVs & Jobs Tests

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_phase2_api.py -v -s
```

### Run Complete Test Suite (Phase 1 Cleaning/Classification/Idempotency + Phase 2 API)

```powershell
.\.venv\Scripts\python.exe -m pytest tests/ -v
```

### Expected Test Output

```text
tests/test_phase2_api.py::test_1_health_and_ingest_if_empty PASSED
tests/test_phase2_api.py::test_2_indexes_and_explain_endpoint PASSED
tests/test_phase2_api.py::test_3_queries_endpoints PASSED
tests/test_phase2_api.py::test_4_aggregations_endpoints PASSED
tests/test_phase2_api.py::test_5_materialized_views_incremental_refresh PASSED
tests/test_phase2_api.py::test_6_scheduled_jobs_endpoints PASSED
========================= 6 passed in 9.46s =========================
```

---

## 📈 9. Verified Benchmarks Across Datasets (30M Production, 100K Sample & 20K Evaluation) 📊

| Stage | Description | Metric / Attribute | 30M Production Dataset | 100K Sample Dataset | 20K Evaluation Dataset | Status |
|---|---|---|---|---|---|---|
| Stage 1 | Raw Ingestion | Engine Selected by Router | PySpark 3.5 Distributed | Python Batch Streaming | Python Batch Streaming | ✅ |
| Stage 1 | Raw Ingestion | Input File Size | 12,652.4 MB (~12.65 GB) | 41.77 MB | ~8.3 MB | ✅ |
| Stage 1 | Raw Ingestion | Total Raw Ingested (`orders_raw`) | 30,000,000 records | 100,000 records | 20,000 records | ✅ |
| Stage 2 | ELT & Upsert | Valid (Unmodified) | 25,772,434 records | 85,926 records | 12,000 records | ✅ |
| Stage 2 | ELT & Upsert | Corrected (With Audit Trail) | 2,969,128 records | 9,866 records | 5,000 records | ✅ |
| Stage 2 | ELT & Upsert | Quarantined (`orders_quarantine`) | 1,258,438 records | 4,208 records | 3,000 records | ✅ |
| Section 6.11 | Consistency Check | — | 30,000,000 == 30,000,000 | 100,000 == 100,000 | 20,000 == 20,000 | PASSED (100%) |
| Stage 2 | ELT & Upsert | Unique Validated (`orders_validated`) | 28,538,963 records | 95,136 records | 17,000 records | ✅ |
| Stage 2 | ELT & Upsert | Re-Run Duplicate Records Created | 0 Duplicates (Idempotent) | 0 Duplicates (Idempotent) | 0 Duplicates (Idempotent) | ✅ |

---

## 🛡️ 10. Sample Validated Document with Audit Trail (`orders_validated`) 📑

```json
{
  "order_id": "ORD-984210",
  "order_date": "2025-01-31T14:22:00Z",
  "status": "confirmed",
  "customer_id": "CUST-4412",
  "customer_phone": "+967771234567",
  "customer_email": "user@domain.com",
  "city": "صنعاء",
  "district": "السبعين",
  "delivery_type": "express",
  "delivery_cost": 2000.0,
  "payment_method": "cash_on_delivery",
  "payment_status": "paid",
  "payment_amount": 15000.0,
  "currency": "YER",
  "total_amount": 17000.0,
  "items": [
    {
      "item_name": "SKU-9921",
      "quantity": 1,
      "unit_price": 15000.0,
      "subtotal": 15000.0
    }
  ],
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "customer_phone",
      "original_value": "771234567",
      "corrected_value": "+967771234567",
      "rule_code": "PHONE_NORMALIZATION_967"
    }
  ],
  "last_updated_at": "2026-10-03T21:00:00Z"
}
```

---

## 👥 Author & Academic Integrity 🎓

- **Lead Data Engineer & Architect:** Eng. Nader Alshawki (@Naderalshawki)
- **Course:** Big Data Engineering — Practical (Phase 1 Core ELT + Phase 2 Final Analytics & API)
- **Academic Year:** 2026
- **Institution:** Faculty of Computer Science & Information Technology
