# 🚀 End-to-End Big Data ELT Pipeline & Advanced Analytics

**Author:** Aiham Alqasem - ايهم ال قاسم  
**Course:** Big Data (Practical) - Midterm & Final Project (Phase 1 & 2)

---

## 📖 Project Overview

This project is a highly robust, scalable, and fully modular End-to-End Big Data Pipeline. It is designed to handle massive volumes of e-commerce data through dynamic ingestion, strict validation, automated error correction, and advanced analytics.

The system leverages the power of **Apache Spark (PySpark)** for distributed data processing and **MongoDB** for flexible, document-based storage. It features an intelligent routing system, Idempotent ELT architecture, Materialized Views with incremental refreshes, and a fully interactive **FastAPI (Swagger UI)** dashboard for seamless execution.

---

## 🔁 End-to-End System Flow

The system follows the execution flow below:

1. **Data Input:** Enter or place the data as it is in the `data/` directory.
2. **File Size Inspection:** The system checks the size of the selected file.
3. **Automatic Routing:** Based on the file size, the system selects the most appropriate storage and ingestion method.
4. **Raw Storage:** All records are stored first in `orders_raw` without changing their original values, while execution metadata is added.
5. **Complete ELT Process:** The system reads the raw data, validates and transforms it, corrects recoverable issues, and quarantines unrecoverable records.
6. **Analytics and Optimization:** Validated data is used for queries, aggregations, materialized views, scheduled jobs, and API-based reporting.

---

## ✅ Full Technical Implementation Checklist

### Phase 1: Core Hybrid ELT Pipeline & Data Quality Engine

- **Small & Medium Dataset Automatic Routing:** Selects Python Batch (`load_with_python_batch`) automatically when file size `<= 200 MB`.
- **Large Dataset Automatic Routing:** Selects PySpark Distributed (`load_with_pyspark`) automatically when file size `> 200 MB`.
- **100% Raw Ingestion First:** All records land first in `orders_raw`, preserving anomalies and enriching each record with `run_id`, `source_file`, and `engine_used`.

#### Data Quality & Cleaning Rules

- **Arabic-Indic Numerals:** Converts `٥٠٠٠` → `5000` and Arabic words (`خمسة آلاف`) to digits.
- **Currency Normalization:** Standardizes `ريال يمني`, `YR`, `ريال` to `YER`.
- **Yemeni Phone E.164 Format:** Normalizes local variants to `+9677....`.
- **Email Syntax Repair:** Fixes duplicate `@` or dots (`user@@mail..com` → `user@mail.com`).
- **Date Standardization:** Normalizes mixed date formats to `YYYY-MM-DD`.
- **Items JSON Recalculation:** Parses JSON, ensures positive quantities, calculates row subtotals, and verifies the final `total_amount`.
- **Audit Trail on Corrected Records:** Every repaired field is logged inside the `corrections` array within the document.
- **Quarantine Isolation with Reasons:** Unrecoverable records are isolated in `orders_quarantine` with an array of explicit `validation_errors`.
- **Zero-Duplication Idempotent Upsert:** Enforces a Unique Index on `order_id` in `validated_orders` and writes using Upsert mechanisms to safely handle re-runs.

### Phase 2: Analytics, Incremental MVs, Scheduled Jobs & Unified API

1. **Queries, Indexes & Explain:** 5 practical queries, 3 indexes (including `idx_city_status` Compound Index). Includes a benchmarking function that drops indexes, runs `explain("executionStats")` (**BEFORE**), builds indexes, runs again (**AFTER**), and outputs the comparison.
2. **The 5 Independent Aggregation Reports (`src/analytics_reports.py`):**
   - `orders_by_status`: Analyzes order count distribution across statuses.
   - `top_cities`: Computes total revenue and orders grouped by city.
   - `top_customers`: Ranks VIP customers by total spent and order count.
   - `payment_methods`: Analyzes revenue by payment method.
   - `delivery_types`: Tracks the performance of delivery options.
3. **Materialized Views & Incremental Refresh (`src/materialized_views.py`):**
   - `mv_daily_sales_summary`: Pre-aggregated daily revenue and order counts.
   - `mv_payment_method_summary`: Pre-aggregated payment method performance.
   - **How Incremental Refresh Works:** Each view queries MongoDB for the `$max: "$processed_at"` value. On refresh, the engine filters `validated_orders` for records processed after this timestamp. It then uses the `$merge` pipeline stage with `whenMatched` to atomically `$add` new totals to existing data without rebuilding from scratch.
4. **Scheduled Jobs & Execution Audit Logging (`src/scheduler.py`):**
   - `job_refresh_daily_sales` & `job_refresh_payment_methods`: Scheduled via APScheduler.
   - **Audit Logging:** Every scheduled or manual run logs `job_name`, `start_time`, `end_time`, `duration_seconds`, and `status` (`SUCCESS/FAILED`) directly to the `job_logs` MongoDB collection.
5. **Unified FastAPI Server (`src/main.py`):** Comprehensive JSON endpoints, auto-expanded Swagger UI at `/docs`, dynamic dropdowns (Enums) for easy report selection, and an interactive Terminal CLI Launcher.

---

## 📂 Project Architecture & Directory Structure

The project is structurally divided into logical modules ensuring maintainability and scalability:

```text
MIDTERM-DATA-PIPELINE/
│
├── config/
│   └── settings.py                 # Global configuration, thresholds, and MongoDB URIs
│
├── data/                           # 📥 Drop your CSV files here dynamically!
│   ├── sample_orders.csv
│   └── orders_huge_mixed_quality.csv
│
├── reports/
│   └── results.json                # Automated master execution logs and metrics
│
└── src/
    ├── analytics_reports.py        # MongoDB Aggregation pipelines (Top Cities, Customers, etc.)
    ├── batch_loader.py             # Streaming Python Batch Loader (for files < 200MB)
    ├── create_small_sample.py      # Utility script for testing
    ├── database_queries.py         # Advanced Queries, Indexing, and ExecutionStats (Explain)
    ├── file_router.py              # Dynamic File Router (Decision Engine)
    ├── main.py                     # 🌟 Main entry point (CLI Menu & FastAPI Web Server)
    ├── materialized_views.py       # Incremental Refresh Logic via $merge
    ├── metrics.py                  # Standardized metrics logging utility
    ├── scheduler.py                # APScheduler integration for automated background jobs
    ├── spark_elt_pipeline.py       # Core Phase 2: Schema validation, Correction & Quarantining
    └── spark_loader.py             # Massive Parallel Loader using PySpark (for files > 200MB)
```

---

## ⚙️ Configuration (`config/settings.py`)

The pipeline is highly configurable. The central `settings.py` manages thresholds, paths, and database connections.

```python
import os

# Main project directory (calculated dynamically)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# -----------------------------------------
# File Router Settings
# -----------------------------------------
SMALL_FILE_THRESHOLD_MB = 200

# -----------------------------------------
# File Paths
# -----------------------------------------
HUGE_CSV_PATH = os.path.join(BASE_DIR, "data", "orders_huge_mixed_quality.csv")
SAMPLE_CSV_PATH = os.path.join(BASE_DIR, "data", "sample_orders.csv")

SAMPLE_ROWS = 100000

# -----------------------------------------
# MongoDB Database Settings
# -----------------------------------------
MONGO_URI = "mongodb://localhost:27017/" 
MONGO_DB_NAME = "for_test"
RAW_COLLECTION_NAME = "orders_raw"

# -----------------------------------------
# Python Batch Loader Engine Settings
# -----------------------------------------
BATCH_SIZE = 5000 # Records per batch chunk

# -----------------------------------------
# PySpark Engine Settings
# -----------------------------------------
SPARK_MONGO_OUTPUT_URI = f"{MONGO_URI}{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
SPARK_MONGO_PACKAGES = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
```

---

## 🔄 Phase 1: Dynamic Ingestion & File Routing

To begin the pipeline, users simply drop their dataset (`.csv`) into the `data/` folder. The system dynamically reads available files and presents them in the Interactive Interfaces.

### The `file_router.py` Decision Engine

Once a file is selected, the **File Router** analyzes the file size and routes it to the appropriate engine:

1. **Size < 200MB:** Routed to `batch_loader.py`. It uses a highly efficient Python Streaming approach (chunking) to ingest data without overloading RAM.
2. **Size > 200MB:** Routed to `spark_loader.py`. It bypasses single-thread limitations and utilizes PySpark for parallel, distributed ingestion directly into the raw collection.

### 📥 Raw Storage Structure (`orders_raw`)

Regardless of the engine used, data is stored in the raw collection strictly **"as-is"** to preserve data anomalies for later auditing, accompanied by crucial tracking metadata:

```json
{
  "_id": "ObjectId('6ac33a04a9cf1980e7a5ab05')",
  "run_id": "ed90f294-704c-4630-92b1-1093b2d8dfc7",
  "source_file": "sample_orders.csv",
  "source_row_number": 1,
  "ingested_at": "ISODate('2026-10-05T05:47:48.435Z')",
  "engine_used": "python_batch",
  "raw_record": {
    "order_id": "طلب-100000",
    "order_date": "2025-02-24T21:29:00",
    "status": "مؤكد",
    "customer_id": "عميل-0",
    "customer_name": "محمد علي",
    "customer_phone": "702390941",
    "customer_email": "user141764@example.com",
    "city": "تعز",
    "district": "شعوب",
    "delivery_type": "سريع",
    "delivery_cost": "5000.0",
    "payment_method": "محفظة إلكترونية",
    "payment_status": "تم الدفع",
    "payment_amount": "769000.0",
    "currency": "YER",
    "total_amount": "769000.0",
    "items_json": "[{\"sku\":\"SKU-1010\",\"name\":\"هاتف سامسونج A54\",\"qty\":-2,\"unit_price\":183000.0,\"total\":549000.0},{\"sku\":\"SKU-1010\",\"name\":\"هاتف سامسونج A54\",\"qty\":1,\"unit_price\":215000.0,\"total\":215000.0}]"
  }
}
```

### 📊 Ingestion Metrics Example

Upon successful ingestion, the system generates comprehensive metrics:

```json
{
  "run_id": "db1ebcf9-8a02-470e-872a-d9c6632806d5",
  "execution_date": "2026-10-05 15:13:41",
  "phase": "Ingestion_Python_Batch",
  "metrics": {
    "total_records_inserted": 50000,
    "total_time_seconds": 1.62,
    "throughput_records_per_second": 30928.48,
    "batch_size_partitions": 5000,
    "upsert_inserted_count": 50000,
    "upsert_updated_count": 0,
    "upsert_unchanged_count": 0,
    "mode": "insert_many (Streaming Mode)",
    "collection": "orders_raw"
  },
  "status": "Success"
}
```

---

## 🛠️ Phase 2: The ELT Pipeline (Extract, Load, Transform)

The ELT pipeline leverages **PySpark** to read from `orders_raw`. It applies a strict Schema and subjects every record to complex Data Quality Rules.

### Valid & Corrected Records (`orders_validated`)

If a record fails the initial validation, the system attempts to fix it using 8 complex correction rules (e.g., standardizing phone formats, fixing Arabic numeric encodings, repairing emails).

If corrected, the record moves to `validated_orders` with an exhaustive `corrections` array detailing exactly what changed (Audit Trail):

```json
{
  "_id": "طلب-100003",
  "run_id": "b0d5576b-e478-4544-a93b-a421ae734a1f",
  "source_file": "sample_orders200.csv",
  "source_row_number": 4,
  "ingested_at": "ISODate('2026-10-05T13:15:36.534Z')",
  "engine_used": "python_batch",
  "raw_record": {
    "order_id": "طلب-100003",
    "total_amount": "٧٠٦٠٠٠٫٠"
  },
  "processed_at": "ISODate('2026-10-05T13:16:15.424Z')",
  "order_id": "طلب-100003",
  "total_amount": 706000.0,
  "items": [
    {
      "sku": "SKU-1010",
      "quantity": 2,
      "unit_price": 257000.0
    }
  ],
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "total_amount",
      "original_value": "٧٠٦٠٠٠٫٠",
      "corrected_value": "706000.0",
      "rule_code": "NUMERIC_FORMAT_NORMALIZED"
    }
  ]
}
```

### Quarantined Records (`orders_quarantine`)

If a record is fundamentally corrupted (e.g., missing essential IDs, impossible dates, corrupt JSON), it is permanently isolated in the quarantine collection. It logs the exact `validation_errors`:

```json
{
  "_id": "sample_orders.csv_1_طلب-100000",
  "run_id": "db1ebcf9-8a02-470e-872a-d9c6632806d5",
  "validation_errors": [
    "Quarantined: INVALID_ITEM_QUANTITY_OR_PRICE"
  ],
  "raw_record": {
    "order_id": "طلب-100000",
    "items_json": "[{\"sku\":\"SKU-1010\",\"qty\":-2}]"
  },
  "quality_status": "quarantined"
}
```

### 🛡️ Idempotency & Upsert Logic

The pipeline is strictly **Idempotent**. It builds a `Unique Index` on the `order_id` field. During the writing phase back to MongoDB, it utilizes the `Upsert` mechanism (via Spark Connector). If a pipeline is rerun, it does not duplicate records; it gracefully updates existing ones, preventing data swamp scenarios.

### 📈 ELT Master Summary Metrics

```json
{
    "run_id": "db1ebcf9-8a02-470e-872a-d9c6632806d5",
    "file_name": "sample_orders.csv",
    "file_size_mb": 88.0,
    "engine_used": "pyspark_elt",
    "rows_read": 50000,
    "raw_loaded": 50000,
    "valid_count": 33899,
    "corrected_count": 11205,
    "quarantine_count": 4896,
    "elapsed_seconds": 272.52,
    "throughput": 183.47,
    "partitions": "10",
    "error_case_counts": {
        "Quarantined: INVALID_ITEM_QUANTITY_OR_PRICE": 715,
        "Quarantined: MISSING_CUSTOMER_ID": 402,
        "Quarantined: INVALID_IMPOSSIBLE_DATE": 332
    }
}
```

---

## 🧠 Phase 3: Advanced Analytics & Optimization

1. **Queries & Indexes:**  
   The project implements 5 core business queries. It creates 3 indexes including a **Compound Index** (`idx_city_status`). It executes `explain("executionStats")` before and after indexing to scientifically prove performance gains (reduced execution time and docs examined).
2. **Aggregations:**  
   5 advanced aggregation pipelines provide insights such as *Top Cities by Sales*, *VIP Customers*, and *Delivery Type Performance*.
3. **Materialized Views:**  
   We implemented two Materialized Views (`mv_daily_sales_summary`, `mv_payment_method_summary`). They feature an **Incremental Refresh Strategy** utilizing MongoDB's `$merge` operator. It only processes newly added records (delta processing) based on `processed_at`, making it highly efficient.
4. **Scheduled Jobs:**  
   `APScheduler` handles background tasks to auto-refresh the views every few hours. Execution logs (success/fail, duration) are saved in the `job_logs` collection.

---

## ▶️ Running the Unified FastAPI Server & Web Dashboard

### Launching the System

Open your terminal and run the main controller script. It provides an Interactive CLI to run files manually, or launch the API Server:

```bash
python src/main.py
```

### Accessing the Web Dashboard

Once the server is running, open your browser at:

- **Swagger UI (Official Automated Evaluation Interface):** `http://127.0.0.1:8000/docs`
- **Auto-Redirect Root:** `http://127.0.0.1:8000/`

### Unified API Endpoints Reference

| Method & Endpoint | Description & Functionality |
|---|---|
| `GET /health` | Verifies system health and returns API status. |
| `POST /ingest` | Triggers the Hybrid Ingestion & ELT pipeline. Features a dynamic Enum Dropdown mapping to files in the `/data` directory. |
| `POST /indexes` | Creates the 3 custom MongoDB indexes (including Compound Indexes). |
| `GET /queries` | Lists all 5 operational queries available for Explain analysis. |
| `GET /queries/{name}` | Executes `explain("executionStats")` before/after indexing via an interactive Dropdown selection. |
| `GET /aggregations` | Lists all 5 analytical aggregation reports. |
| `GET /aggregations/{name}` | Runs any specific aggregation report dynamically via an Enum Dropdown. |
| `POST /refresh-mv` | Performs a `$merge` Incremental Refresh of Materialized Views using watermark deltas. |
| `GET /jobs` | Lists the most recent APScheduler execution audit logs from `job_logs`. |
| `POST /jobs/{name}/run` | Immediately triggers a scheduled job manually (e.g., `refresh_daily_sales`) via Dropdown selection. |

---

## 🧰 How to Run the Project

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Launch the Master Interface

Run the following command in your terminal:

```bash
python src/main.py
```

### 3. Select Execution Mode

The CLI will greet you with a dynamic menu. You can either select a specific dataset to process locally via the terminal, OR select `[1]` to launch the Web Interface.

### 4. Access the Swagger UI

Open your browser and navigate to:

👉 **`http://127.0.0.1:8000/docs`**

*Enjoy exploring the pipeline!*
