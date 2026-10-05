🚀 Big Data Pipeline and Advanced Analytics Project
Author: Aiham Alqasem — أيهم ال قاسم
Course: Big Data (Practical) — Midterm Project and Final Phase (Phase I & II)


Table of Contents
1	Overview and High-level Architecture
2	Project Objectives
3	System Architecture and Data Layers
4	Repository Structure
5	Project Files and Their Roles
6	Environment Setup
7	Running the Midterm Phase
8	Data Processing and Quality Rules
9	Experimental Execution Results
10	Final Phase Requirements
11	Queries and Indexes
12	Aggregation Reports
13	Materialized Views
14	Scheduled Jobs
15	FastAPI Interface
16	Suggested Test Sequence
17	Reproducibility and Dynamic Input
18	Reports and Outputs
19	Coverage of Evaluation Requirements
20	Discussion Questions and Answers
21	Conclusion
22	Quick Run and Tests



1. Overview and High-level Architecture
This project is a comprehensive, reusable data pipeline for big data processing and analytics. It is designed to handle large volumes of e-commerce data via dynamic input, strict validation, safe error correction, organized quarantine of invalid records, and producing analytics and reports consumable via an API.

The system relies on:

•	Apache Spark / PySpark for distributed processing and large file handling.
•	Python Batch for efficient small-file processing with low latency.
•	MongoDB for flexible document storage, querying, indexing, and aggregation.
•	FastAPI to provide a unified run-and-test interface via Swagger UI and JSON.

Project features also include:

•	Intelligent routing of files to the most appropriate processing engine.
•	Clear ELT separation between ingestion and transformation.
•	Safe rerun capability using idempotency and upsert operations.
•	Materialized views with incremental updates.
•	Execution metrics logging and run reports.
•	Separation of raw records from validated and quarantined records.

The API is not a separate backend that reimplements the project; it is a thin execution layer that invokes existing module functions, satisfying the project requirement while preserving modular design.

Midterm Phase — Data Engineering and ELT
The first phase covers:

23	Reading CSV files.
24	Auto-selecting the appropriate processing engine.
25	Using Python Batch for small files.
26	Using PySpark for large files.
27	Loading raw data into MongoDB.
28	Cleaning, validating, and normalizing records.
29	Safely correcting fixable errors.
30	Quarantining records with critical errors.
31	Storing execution metrics and processing reports.

Final Phase — Analytics and API
The second phase adds:

32	Creating operational indexes and queries.
33	Measuring the effect of indexes using explain("executionStats").
34	Building aggregation reports based on actual DB data.
35	Creating materialized views with incremental update.
36	Creating scheduled jobs with full execution logging.
37	Providing a unified FastAPI interface for running and testing.
38	Documenting installation, setup, run, and test procedures.



2. Project Objectives
The project aims to build a practical, scalable data processing system achieving the following objectives:

•	Create a reusable data processing pipeline.
•	Accept different input files without relying on a fixed filename or fixed record count.
•	Separate data into layers: raw, validated, and quarantined.
•	Apply clear, traceable data quality rules.
•	Use MongoDB for storage, querying, indexing, and aggregation.
•	Demonstrate the practical effect of indexes rather than just creating them.
•	Produce meaningful analytical reports.
•	Persist analytical results in reusable materialized views.
•	Support manual and scheduled execution.
•	Facilitate evaluation via Swagger UI and JSON responses.



3. System Architecture and Data Layers
Data flow passes through the following stages:

[CSV File]
     │
     ▼
[File Router]
     │
     ├── Small file ──> [Python Batch]
     │
     └── Large file ──> [PySpark ELT]
                           │
                           ▼
                  [orders_raw — raw data]
                           │
                           ▼
                    [data quality rules]
                      /        │        \
                     /         │         \
                    ▼          ▼          ▼
       [orders_validated] [corrections] [orders_quarantine]
                    │
                    ▼
       [queries + aggregations + materialized views]
                    │
                    ▼
                [FastAPI]
                    │
                    ▼
             [Swagger / JSON]

3.1 MongoDB Collections
Collection	Purpose
orders_raw	Store records immediately after ingestion without modification, with metadata for traceability.
orders_validated	Store clean records and records that were successfully corrected.
orders_quarantine	Store records that contain critical or non-safely-fixable errors.
jobs_log	Store history of scheduled job runs, statuses, and errors.
daily_sales_mv	Materialized view summarizing orders and sales by day.
city_sales_mv	Materialized view summarizing orders and sales by city.


4. Repository Structure
midterm-data-pipeline/
│
├── config/
│   └── settings.py                 # Application and database settings
│
├── data/                           # Input CSV files
├── docs/                           # Documentation, supporting files, and screenshots
├── reports/                        # JSON reports and execution outputs
│
├── src/
│   ├── api.py                      # FastAPI app and endpoints
│   ├── main.py                     # Main pipeline orchestrator
│   ├── load_batch.py               # Python Batch engine for small files
│   ├── spark_load.py               # File loader using PySpark
│   ├── spark_etl_pipeline.py       # ELT stages, cleaning, and validation
│   ├── quality_rules.py            # Data quality and correction rules
│   ├── db_loader.py                # Functions to load data into MongoDB
│   ├── indexes.py                  # Create required indexes
│   ├── queries.py                  # Queries and Explain statistics
│   ├── aggregations_final.py       # Aggregation reports and analytics
│   ├── materialized_views.py       # Create and refresh materialized views
│   ├── scheduled_jobs.py           # Scheduled jobs and logging
│   ├── create_small_sample.py      # Small test data generator
│   └── reset_db.py                 # Optional DB reset utility
│
├── .env.example                    # Environment variables template
├── requirements.txt                # Required dependencies
└── README.md                       # Project documentation



5. Project Files and Their Roles
This section explains the role of each file in the execution path so the data flow can be traced from ingestion to API results.

5.1 config/settings.py — Central Settings
This file contains application and database settings, such as the MongoDB URI, database name, and small-file size thresholds. Collecting settings in one file makes environment changes easy and avoids duplicating values across files. It also uses environment variables where appropriate instead of hardcoding values.

5.2 src/main.py — Pipeline Entry Point and Orchestrator
This file is the primary entry point for the project. It accepts the data file path or reads it from the PIPELINE_INPUT_FILE variable, then passes it to the file router. It then monitors the execution result and compiles execution metrics and the final report.

In practice: main.py receives the file and routes it to the File Router, and does not implement all processing details itself. This separation keeps the orchestrator simple and allows changing the processing engine without altering how the project is executed.

Run command:

python src/main.py

5.3 File Router
The router picks the processing engine based on the file size and the SMALL_FILE_THRESHOLD_MB value:

•	If the file size is smaller than the threshold, route it to load_batch.py.
•	If the file size exceeds the threshold, route it to spark_load.py and then to the PySpark ELT stages.

Thus the system does not rely on a fixed filename or a fixed record count; it selects the engine based on the actual input.

5.4 src/load_batch.py — Python Batch Engine
This file is dedicated to small files. It reads data in batches and stores the record as-is (without cleaning or altering its original content), then enriches it with important metadata such as:

•	run id run_id.
•	source filename.
•	source row number.
•	ingestion timestamp.
•	engine used.

Loading uses batch insert (storing a group of rows in one operation instead of inserting each row individually). This improves performance and reduces the number of DB connections.

Example record stored in orders_raw:

{
  "_id": "ObjectId('6ac3e55655bdc0f333119643')",
  "run_id": "run_20261005_205846",
  "source_file": "01_student_test_small.csv",
  "source_row_number": 1,
  "ingested_at": "2026-10-05T20:58:46.735122",
  "engine_used": "python_batch",
  "raw_record": {
    "\\ufefforder_id": "طلب-8000001",
    "order_date": "2026-05-21T16:52:00",
    "status": "ملغي",
    "customer_id": "عميل-8000001",
    "customer_name": "خالد سالم",
    "customer_phone": "717736067",
    "customer_email": "customer8000001_221560@example.com",
    "city": "تعز",
    "district": "التحرير",
    "delivery_type": "عادي",
    "delivery_cost": "2000.0",
    "payment_method": "نقدًا عند التسليم",
    "payment_status": "بانتظار الدفع",
    "payment_amount": "55000.0",
    "currency": "YER",
    "total_amount": "55000.0",
    "items_json": "[{\"sku\":\"SKU-1008\",\"name\":\"محول HDMI\",\"qty\":1,\"unit_price\":14000.0,\"total\":14000.0},{\"sku\":\"SKU-1008\",\"name\":\"محول HDMI\",\"qty\":3,\"unit_price\":13000.0,\"total\":39000.0}]"
  }
}

(Note: sample dataset values remain as provided.)

5.5 src/spark_load.py — PySpark Engine for Large Files
This file performs the same raw ingestion function but uses Spark to leverage distributed processing and partitioning. It saves the data as-is, adds the same metadata, then writes to the orders_raw collection before starting cleaning and validation.

The Spark-generated record has a very similar structure and records the engine used:

{
  "_id": "ObjectId('6ac3e55655bdc0f333119643')",
  "run_id": "run_20261005_205846",
  "source_file": "01_student_test_small.csv",
  "source_row_number": 1,
  "ingested_at": "2026-10-05T20:58:46.735122",
  "engine_used": "pyspark_elt",
  "raw_record": {
    "ufefforder_id": "طلب-8000001",
    "order_date": "2026-05-21T16:52:00",
    "status": "ملغي",
    "customer_id": "عميل-8000001",
    "customer_name": "خالد سالم",
    "customer_phone": "717736067",
    "customer_email": "customer8000001_221560@example.com",
    "city": "تعز",
    "district": "التحرير",
    "delivery_type": "عادي",
    "delivery_cost": "2000.0",
    "payment_method": "نقدًا عند التسليم",
    "payment_status": "بانتظار الدفع",
    "payment_amount": "55000.0",
    "currency": "YER",
    "total_amount": "55000.0",
    "items_json": "[{\"sku\":\"SKU-1008\",\"name\":\"محول HDMI\",\"qty\":1,\"unit_price\":14000.0,\"total\":14000.0},{\"sku\":\"SKU-1008\",\"name\":\"محول HDMI\",\"qty\":3,\"unit_price\":13000.0,\"total\":39000.0}]"
  }
}

5.6 src/spark_etl_pipeline.py — ELT Transformations and Validation
This file reads records from orders_raw, applies quality rules and classification. If a record is valid or can be safely corrected, it is sent to orders_validated. If it contains a critical error or cannot be safely fixed without changing the meaning, it is moved to orders_quarantine with added error details.

The file also records correction metadata when processing a record and relies on a unique index to prevent duplicate orders. It uses upsert and idempotency — re-running the process does not create duplicate copies of the same record; it inserts when needed or updates when present.

At the end of execution, it produces a report showing counts of records in each category and the number of inserted, updated, or unchanged operations.

5.7 src/quality_rules.py — Quality Rules and Corrections
Contains field validation rules and classification of errors into fixable and critical errors. Examples include validation for order and customer IDs, email and phone format, currency, status, date, product data, quantity, price, and items list.

Separating these rules makes them testable and modifiable without touching the ingestion engine or API.

5.8 src/db_loader.py — MongoDB Loading Layer
Provides functions for DB connection and executing batch inserts and upserts while preserving run metadata. This layer prevents duplication of MongoDB logic across processing modules.

5.9 Metrics Module metrics
The metrics module logs ingestion, storage, and performance metrics such as rows read, raw rows, elapsed time, throughput, batch size, and engine used. These results can be saved to reports/results.json.

Example metrics report:

{
  "run_id": "run_20261005_205846",
  "file_name": "01_student_test_small.csv",
  "engine_used": "python_batch",
  "rows_read": 20000,
  "raw_loaded": 20000,
  "elapsed_seconds": 0.82,
  "throughput": 24395.39,
  "batch_size": 5000
}

5.10 src/indexes.py — Create Indexes
Creates required indexes on appropriate collections and ensures safe repeated execution. Includes a unique index on order_id, an index on record_status in quarantine records, and a compound index on date and status.

5.11 src/queries.py — Queries and Performance Measurement
Contains five practical queries and collects explain("executionStats") statistics for the first three queries to compare the number of documents and index keys examined and execution time before and after index creation.

5.12 src/aggregations_final.py — Analytical Reports
Executes independent aggregation pipelines based on current MongoDB data, not on precomputed results. Provides city reports, status distributions, payment methods, busy days, and top customers.

5.13 src/materialized_views.py — Materialized Views
Creates and refreshes daily_sales_mv and city_sales_mv. Uses $merge to merge new or modified results into the materialized collection instead of deleting and rebuilding it entirely.

5.14 src/scheduled_jobs.py — Scheduled Jobs
Defines real jobs that can run on a schedule and logs start/end times, status, and error messages to jobs_log.

5.15 src/api.py — FastAPI Interface
Provides a unified run-and-test layer. It does not reimplement ingestion, aggregation, or indexing logic; it calls the previous modules and returns JSON results. Therefore it remains concise, clear, and compatible with modular design.

5.16 src/create_small_sample.py — Create Test Sample
Generates a small data file to quickly exercise the project and validate the main path without needing a very large file.

5.17 src/reset_db.py — Optional Reset
Offers an optional way to clear project data and reset the database to a clean state during testing or reruns. Use with caution as it may delete existing run data.

5.18 .env.example, requirements.txt, and README.md
•	.env.example: shows required environment variables without exposing local values.
•	requirements.txt: lists dependencies for installation.
•	README.md: documents setup, run, test, and reproducibility instructions.



6. Environment Setup
6.1 Prerequisites
•	Python 3.8 or newer.
•	MongoDB running on port 27017.
•	Apache Spark and PySpark for large-file processing.
•	Java version compatible with Spark.
•	Local network connectivity to MongoDB.

6.2 Install Dependencies
From the project root folder:

pip install -r requirements.txt

Core packages include:

fastapi
uvicorn
pymongo
pyspark
schedule
python-dotenv
pytest
httpx

6.3 Configure Environment Variables
Copy the template to a local env file:

cp .env.example .env

Then edit values according to your MongoDB setup, input file path, and small file size threshold as needed.



7. Running the Midterm Phase
7.1 Create a Small Sample
python src/create_small_sample.py

7.2 Run the Pipeline
python src/main.py

The program accepts a file path, for example:

D:\midterm-data-pipeline\data\01_student_test_small.csv
/home/user/midterm-data-pipeline/data/01_student_test_small.csv

7.3 Run Using an Environment Variable
On Linux/macOS:

export PIPELINE_INPUT_FILE="data/01_student_test_small.csv"
python src/main.py

On Windows PowerShell:

$env:PIPELINE_INPUT_FILE="data\01_student_test_small.csv"
python src/main.py

7.4 Engine Selection Mechanism
The router selects the engine based on file size:

•	Files smaller than SMALL_FILE_THRESHOLD_MB use Python Batch.
•	Larger files use PySpark.

The system does not rely on a fixed filename or predetermined record count.



8. Data Processing and Quality Rules
8.1 Raw Ingestion
Records are written first to orders_raw before cleaning. This ensures retention of original data, traceability, auditability, and the ability to reprocess later.

8.2 Quality Checks
The quality layer can validate rules such as:

•	Missing order_id.
•	Missing customer_id.
•	Invalid email address.
•	Invalid phone number.
•	Unsupported currency.
•	Unknown order status.
•	Impossible or invalid order date.
•	Missing product SKU or name.
•	Invalid quantity or price.
•	Corrupted items JSON.
•	Empty items list.

8.3 Correction vs. Quarantine
•	Corrected record: a record containing an error that can be safely fixed without changing the meaning of the data.
•	Quarantined record: a record containing a critical or conflicting error that cannot be safely corrected without risking data integrity.

8.4 Conflicting Errors
If a record contains more than one critical error, it is classified under:

MULTIPLE_CONFLICTING_ERRORS

This prevents counting the same record in multiple error categories and keeps quarantine statistics consistent and non-overlapping.



9. Experimental Execution Results
The project was tested with the file:

01_student_test_small.csv

Recorded results were as follows:

{
  "run_id": "run_20261005_205846",
  "file_name": "01_student_test_small.csv",
  "file_size_mb": 0.0,
  "engine_used": "pyspark_elt",
  "rows_read": 20000,
  "raw_loaded": 20000,
  "valid_count": 12000,
  "corrected_count": 5000,
  "quarantine_count": 3000,
  "elapsed_seconds": 37.5,
  "throughput": 533.4,
  "partitions": "10",
  "error_case_counts": {
    "Quarantined: EMPTY_ITEMS": 250,
    "Quarantined: INVALID_ITEM_QUANTITY_OR_PRICE": 250,
    "Quarantined: INVALID_CURRENCY": 250,
    "Quarantined: MISSING_CUSTOMER_ID": 250,
    "Quarantined: INVALID_IMPOSSIBLE_DATE": 250,
    "Quarantined: MISSING_ORDER_ID": 250,
    "Quarantined: INVALID_UNKNOWN_STATUS": 250,
    "Quarantined: MISSING_ITEM_SKU_OR_NAME": 250,
    "Quarantined: INVALID_EMAIL_ADDRESS": 250,
    "Quarantined: MULTIPLE_CONFLICTING_ERRORS": 250,
    "Quarantined: CORRUPTED_ITEMS_JSON": 250,
    "Quarantined: INVALID_PHONE_NUMBER": 250
  },
  "inserted_count": 17000,
  "updated_count": 0,
  "unchanged_count": 0
}

Execution time and analytical results may vary when using a different file, as the system computes results from the current database content on each run.



10. Final Phase Requirements
The final phase adds seven points to the midterm project and includes:

39	Queries, indexes, and Explain statistics.
40	Aggregation reports.
41	Materialized views.
42	Scheduled jobs.
43	FastAPI interface.
44	GitHub documentation, README, env template, and reproducibility.

Midterm functionality remains inside the same repository; the final phase expands rather than replaces it.



11. Queries and Indexes
11.1 Required Indexes
The file src/indexes.py creates at least three primary indexes:

45	Unique index on orders_validated:

("order_id", ASCENDING)

46	Prevents duplicate order IDs and supports direct lookup.
47	Single-field index on orders_quarantine:

("record_status", ASCENDING)

48	Speeds filtering of quarantined records by status.
49	Compound index on orders_validated:

[("order_date", DESCENDING), ("status", ASCENDING)]

50	Supports queries that combine date and status.

record_status is used because it matches the actual field in the quarantine record structure. Index names must match the stored MongoDB field names exactly.

11.2 Index Rationale
•	order_id: fast direct lookup and duplicate prevention.
•	record_status: fast filtering for quarantined records.
•	order_date + status: support combined date/status lookups.

11.3 Creating Indexes
Via API:

POST /indexes

Or directly:

python src/indexes.py

11.4 Five Practical Queries
Query name	Purpose
q1_search_by_id	Search by order_id.
q2_date_and_status	Search by date and status using the compound index.
q3_quarantine_errors	Search in quarantine records by record_status.
q4_wallet_payments	Return a sample of orders paid via wallet.
q5_city_search	Search orders by city.
11.5 Explain Statistics
Collect performance info for the first three queries using:

explain("executionStats")

The response may include:

•	executionTimeMillis — execution time.
•	totalDocsExamined — number of documents examined.
•	totalKeysExamined — number of index keys examined.
•	nReturned — number of returned results.
•	the chosen execution plan.

To show the index effect, run the queries before and after index creation, then compare execution time and the number of documents and index keys examined.

Example requests:

GET /queries/q1_search_by_id
GET /queries/q2_date_and_status
GET /queries/q3_quarantine_errors
GET /queries/q4_wallet_payments
GET /queries/q5_city_search



12. Aggregation Reports
src/aggregations_final.py provides at least five reports based on live MongoDB data:

Report	Description
top_cities	Count of orders aggregated by city.
order_status	Distribution of orders by status.
payment_methods	Usage of payment methods.
busy_days	Count of orders aggregated by day.
top_customers	Ranking of customers by order count.
Running Reports
List available reports:

GET /aggregations

Run a specific report:

GET /aggregations/top_cities
GET /aggregations/order_status
GET /aggregations/payment_methods
GET /aggregations/busy_days
GET /aggregations/top_customers

Each report has a clear name and an independent aggregation pipeline, and returns a JSON response built from current DB data, not from precomputed static results.


13. Materialized Views
The project includes two primary materialized views:

51	daily_sales_mv
◦	Summarizes orders and sales by day.
52	city_sales_mv
◦	Summarizes orders and sales by city.

13.1 Incremental Update
Views are refreshed using aggregation pipelines that use $merge. This merges new or modified results into the materialized collection instead of deleting and rebuilding the entire view each time.

This approach allows updating results as new data arrives while keeping analytical results available for fast queries.

13.2 Refreshing Views
POST /refresh-mv

The endpoint returns the operation status and samples from:

•	daily_sales_mv.
•	city_sales_mv.

14. Scheduled Jobs
The project provides two real scheduled jobs:

14.1 update_views
Refreshes the materialized views, including:

•	daily_sales_mv.
•	city_sales_mv.

14.2 generate_report
Generates periodic JSON reports and stores them in the reports folder.

14.3 Job Logging
Each job is logged into the jobs_log collection with:

•	job name.
•	start time.
•	end time.
•	status: success or failed.
•	error message if any.
•	result details on success.

14.4 Manual Execution
List recent job logs:

GET /jobs

Run the views update job manually:

POST /jobs/update_views/run

Run the report generation job manually:

POST /jobs/generate_report/run

The db_scheduled_jobs.py file contains scheduling logic and can run jobs according to schedules specified in settings.



15. FastAPI Interface
15.1 Running the API
You can run the app with Uvicorn:

python -m uvicorn src.api:app --reload

You can also run the file directly if supported by the application:

python src/api.py

15.2 Swagger UI
After starting the server open:

http://127.0.0.1:8000/docs

Swagger includes the following sections:

•	Core operations.
•	Queries.
•	Aggregations.
•	Materialized views.
•	Scheduled jobs.

15.3 API Endpoints
Method	Endpoint	Purpose
GET	/health	Check service and DB health.
POST	/ingest	Trigger the existing ingestion pipeline.
POST	/indexes	Create the required indexes.
GET	/queries	List available queries.
GET	/queries/{name}	Run a specific query.
GET	/aggregations	List available reports.
GET	/aggregations/{name}	Run a specific aggregation report.
POST	/refresh-mv	Refresh materialized views.
GET	/jobs	List job execution logs.
POST	/jobs/{name}/run	Run a specific job manually.
All responses are JSON. The API delegates real work to the project modules rather than duplicating execution inside api.py.

16. Suggested Test Sequence
Step 1 — Start MongoDB
Ensure MongoDB is available at:

mongodb://localhost:27017/

Step 2 — Install Dependencies and Setup
pip install -r requirements.txt
cp .env.example .env

Step 3 — Load Data
python src/main.py

Step 4 — Run the API
python src/api.py

Step 5 — Health Check
GET /health

Step 6 — Create Indexes
POST /indexes

Step 7 — Test Queries
GET /queries
GET /queries/q1_search_by_id
GET /queries/q2_date_and_status
GET /queries/q3_quarantine_errors
GET /queries/q4_wallet_payments
GET /queries/q5_city_search

Step 8 — Test Aggregations
GET /aggregations
GET /aggregations/top_cities
GET /aggregations/order_status
GET /aggregations/payment_methods
GET /aggregations/busy_days
GET /aggregations/top_customers

Step 9 — Refresh Materialized Views
POST /refresh-mv

Step 10 — Test Scheduled Jobs
POST /jobs/update_views/run
POST /jobs/generate_report/run
GET /jobs

Step 11 — Run Automated Tests
pytest -q



17. Reproducibility and Dynamic Input
The project does not rely on:

•	A single fixed filename.
•	A fixed row count.
•	Prewritten analytical results.
•	A specific city or customer.
•	Training dataset only.
•	Developer-specific absolute paths.

When a different valid input file is provided, the system recomputes the entire pipeline, reports, indexes, materialized views, and execution metrics based on the new data.



18. Reports and Outputs
Execution outputs are stored in the reports folder. Examples include:

•	results.json: processing metrics and run log.
•	payment and analytics reports.
•	PySpark processing results.
•	scheduled job logs.

It is recommended to use a single results.json file containing a list of run records rather than creating a separate report file for each run.

A run record may include:

•	run_id.
•	input filename.
•	selected engine.
•	rows read.
•	raw rows loaded.
•	counts of validated, corrected, and quarantined records.
•	elapsed time.
•	throughput.
•	counts of inserted and updated records.
•	error distribution.


19. Coverage of Evaluation Requirements
Requirement	Points / Weight
Midterm project baseline	18
Queries, indexes, and Explain	1.5
Aggregation reports	1.5
Materialized views	1.0
Scheduled jobs	1.0
FastAPI API	0.75
GitHub, README, reproducibility	0.50
Discussion & understanding	0.25
Final phase additions	7
Total	25
A dashboard is optional and not required by the final phase criteria listed.


20. Discussion Questions and Answers
Why does the project use two processing engines?
Python Batch is suitable for small files because it is lightweight and fast in that range. PySpark is used for large files because it supports parallel processing and partition-based execution.

Why store raw data separately?
Keeping the original data in orders_raw preserves traceability and allows review and reprocessing later without losing the original source.

What is the difference between a corrected record and a quarantined record?
A corrected record contains an error that can be safely fixed; a quarantined record contains a critical or conflicting error that cannot be fixed without risking data integrity.

Why create a compound index?
Some queries filter by more than one field, such as order_date and status. The compound index supports these queries and reduces the number of unnecessary documents MongoDB must scan.

How is the index effect demonstrated?
Run the query with explain("executionStats") before and after creating the index, then compare execution time and the number of documents and index keys examined.

Why use materialized views?
Recomputing reports like daily sales or city sales on every request can be expensive. Materialized views store reusable analytical results, making repeated reads faster.

What does incremental update mean?
It means merging new or modified aggregation results into the materialized collection using $merge instead of rebuilding the whole view manually.

How do you verify a job succeeded?
Each job writes start time, end time, status, and optional error details to jobs_log. The log can be viewed via GET /jobs.

Why is api.py relatively short?
The API is intentionally a thin execution layer. Indexing, querying, aggregations, materialized views, and scheduled jobs reside in separate modules, enabling modular design and avoiding code duplication.

Is this API a separate backend?
No. It is a unified interface to run and test project functions via JSON and Swagger, in accordance with the project specifications.


21. Conclusion
The project provides a complete pipeline starting from file ingestion, engine selection, raw data storage, quality validation, correction, quarantine, organized storage, then querying and analysis, and finally operation through an API.

The midterm phase demonstrates data engineering and ELT using Python Batch and PySpark. The final phase extends the repository by adding indexes, explainable queries, aggregation reports, incrementally updated materialized views, scheduled jobs, and a documented FastAPI interface.

The design is modular, reproducible, and suitable for evaluation via Swagger UI. It avoids static results and supports different datasets, making it more reliable and scalable.



22. Quick Run and Tests
Run API tests only
python -m pytest -q api.py

Show test results with details
python -m pytest -v src/api.py

What do these tests verify?
The tests ensure that:

•	The FastAPI app can be imported successfully.
•	GET /health works correctly.
•	GET /queries works correctly.
•	GET /jobs works correctly.
•	Responses are returned as JSON.
•	Endpoints return expected HTTP codes.

If you see:

3 passed

it indicates the basic API tests passed successfully.

Difference between main run commands
Command	Purpose
python src/main.py	Run the pipeline and process the input file.
python src/api.py	Run FastAPI and Swagger UI if direct run is supported.
Full execution order
1. Start MongoDB
Ensure MongoDB is running at:

mongodb://localhost:27017/

2. Run the data pipeline
python src/main.py

This command reads the input file, selects the appropriate engine, processes the data, and stores results in MongoDB.

3. Run the FastAPI app
Open a second PowerShell window and run:

python src/api.py --reload

4. Open Swagger UI
Open in your browser:

http://127.0.0.1:8000/docs

Use Swagger to interactively test API endpoints.

5. Run API tests
Open a third PowerShell window and run:

python src/api.py

Do not run python tests/test_api.py directly to execute tests; running the file directly may not automatically run functions named with the test_ prefix. Use pytest instead.

Shortcut commands
# Install dependencies
pip install -r requirements.txt
 
# Start MongoDB then run the data pipeline
python src/main.py
 
# Run API
python src/api.py

# Open Swagger UI
# http://127.0.0.1:8000/docs



Screenshots and Documentation
Project Structure
[Image failed to load: Project Structure]

Pipeline Execution
[Image failed to load: Pipeline Execution]

Pipeline Results
[Image failed to load: Pipeline Results]

FastAPI & Swagger Documentation
[Image failed to load: Swagger API]

Indexes
[Image failed to load: Indexes]

Aggregation Reports
[Image failed to load: Aggregation Result]

Materialized Views
[Image failed to load: Materialized Views]

Scheduled Jobs
[Image failed to load: Scheduled Jobs]


Note: All requirements, information, examples, commands, endpoints, and results from the original document have been preserved. The content was translated into English and organized to provide a functional description of each major project file.
