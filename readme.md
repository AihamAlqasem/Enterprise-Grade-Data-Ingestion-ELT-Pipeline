# 🚀 Enterprise-Grade Data Ingestion & ELT Pipeline

## 📌 Project Overview

This project is a highly scalable, robust **Data Ingestion and ELT (Extract, Load, Transform)** pipeline designed to process massive, messy datasets. Built with **PySpark** and **MongoDB**, it handles data extraction, advanced regex-based cleansing, strict schema validation, and intelligent data routing.

**🔥 Proven Scale:** This architecture has been stress-tested and successfully executed against massive datasets, including a **12 GB CSV file containing over 30 Million records**, processing them with extreme speed and zero memory crashes using distributed computing and optimized Spark memory configurations (12GB driver/executor allocation).

---

## 🏗️ Architecture & Data Flow

The pipeline is divided into two distinct, highly decoupled phases:

### Phase 1: High-Speed Raw Ingestion

Data is ingested from large CSV files directly into the MongoDB `order_raw` collection. This layer acts as an immutable historical log.

* **Python Streaming Batch:** Reads data row-by-row with controlled chunking (`insert_many`), stripping hidden Unicode spaces from headers and keys to prevent "Mojibake".
* **PySpark Fast Loader:** Utilizes parallel writing across partitions. Bypasses `inferSchema` and `repartitioning` to avoid costly shuffles, reading all fields as `String` to preserve raw data fidelity.
* **Key Concept:** MongoDB assigns a random `ObjectId` here, ensuring every execution (`id_run`) is fully recorded historically without data loss.

### Phase 2: Single-Pass ELT & Separation (`spark_elt_pipeline.py`)

This is the core engineering marvel of the pipeline. It reads from the raw layer and performs a single-pass transformation:

1. **Deduplication & Key Extraction:** Extracts pure numeric values from dirty `order_id` strings (removing Arabic text/spaces) to create a deterministic primary   key.
2. **Cleansing & Audit Trail:** Applies business rules (regex formatting for phones, trimming spaces from statuses, fixing negative payments). Every change is documented in a precise `corrections` JSON array.
3. **Strict Validation:** Evaluates 9 mandatory business rules.
4. **Tri-Routing (Splitting):**
* **Valid/Corrected Records ➡️ `validated_orders`:** Clean data ready for analytics.
* **Invalid Records ➡️ `quarantine_orders`:** Bad data isolated for manual review.


5. **Native Upsert & Idempotency:** Maps the cleaned `order_id` directly to MongoDB's `_id`. This forces a **Native Upsert**, ensuring absolute Idempotency. Rerunning the pipeline 100 times will never duplicate valid business records (solving the dreaded `E11000` Duplicate Key Error).

---

## 🌟 Key Features & Technical Highlights

| Feature | Description |
| --- | --- |
| **🛡️ Absolute Idempotency** | Prevents duplicate data insertion on re-runs. Uses deterministic `_id` extraction for `validated_orders` and source-based composite IDs (`file_source` + `row_number`) for `quarantine_orders`. |
| **🧹 Smart Data Cleansing** | Automatically trims hidden spaces, fixes malformed emails (`@@`), extracts raw integers from mixed text, and converts negative payments to absolute values. |
| **📝 Detailed Audit Trails** | Generates a granular `corrections` array for every modified record (tracking `field`, `original_value`, `corrected_value`, and `rule_code`). |
| **⚡ Zero-Shuffle Processing** | Optimized PySpark dataframes that avoid `repartition()` and `inferSchema()`, preventing massive network overhead and memory exhaustion on 30M+ rows. |
| **📊 Unified Cumulative Reporting** | Automatically generates performance metrics (throughput, processing time, error counts) and safely *appends* them to a shared `results.json` without overwriting historical runs. |

---

## 📂 Directory Structure

```text
## 📂 Directory Structure

```text
MIDTERM-DATA-PIPELINE/
├── config/
│   └── settings.py
├── data/
│   ├── orders_huge_mixed_quality.csv    # Massive 12GB dataset
│   └── sample_orders.csv
├── reports/
│   └── results.json                     # Cumulative ELT & Ingestion metrics
├── Screenshot/
│   └── ...                              # All project screenshots and visuals
├── src/
│   ├── batch_loader.py
│   ├── create_small_sample.py
│   ├── file_router.py
│   ├── main.py
│   ├── metrics.py
│   ├── spark_elt_pipeline.py            # Core logic for processing and Tri-Routing
│   └── spark_loader.py
├── tests/
│   └── test_elt_from_file.py            # Isolated logic testing environment
├── readme.md
└── requirements.txt
```

---

## 🚀 How to Run

### 1. Configuration

Ensure your MongoDB instance is running. Update the `config/settings.py` with your specific database URI and file paths.

### 2. Execution

You can run the entire pipeline orchestrator, or run specific components individually:

**Run the Full Pipeline (Orchestrator):**

```bash
python src/main.py

```

**Run Only the ELT Phase (Standalone):**

```bash
python src/spark_elt_pipeline.py

```

## 📸 Screenshots & Visuals
This project includes a dedicated `Screenshot` folder located in the root directory. It contains all visual documentation, including execution snapshots, terminal outputs of the ELT separation tests, and MongoDB database states, providing a clear visual proof of the pipeline's performance and data integrity rules.

*Note: Ensure you have `HADOOP_HOME` properly configured if running on Windows.*

### 3. Review Reports

After execution, check the `reports/results.json` file. You will find a continuously appended array of execution metrics, showing exact throughput (`records/second`), elapsed time, and error distribution.

---

> *"Data Engineering is not just about moving data; it's about guaranteeing its integrity at any scale."*

👨‍💻 **Author & Architect:** Aiham Sadeq Alqasem