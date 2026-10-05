import os
import sys
import time
from datetime import datetime
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from pyspark.sql.functions import col, lit, coalesce, trim

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import HUGE_CSV_PATH, SAMPLE_CSV_PATH, MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME

# استدعاء الأدوات والمراحل (القسم السابق ELT)
from src.file_router import route_file
from src.batch_loader import load_with_python_batch
from src.spark_loader import load_with_pyspark
from src.metrics import save_run_metrics
from src.spark_elt_pipeline import validate_and_transform_data
from pyspark.sql import SparkSession

# استدعاء وحدات المشروع النهائي (التحليل والتقارير)
from src.database_queries import DatabaseQueriesAnalyzer
from src.analytics_reports import AggregationReports
from src.materialized_views import MaterializedViewManager
from src.scheduler import JobSchedulerManager

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from fastapi.responses import RedirectResponse

app = FastAPI(
    title="Data Pipeline & Analytics API",
    description="واجهة تفاعلية بمسارات مستقلة (أزرار جاهزة) لتسهيل العرض والمناقشة.",
    version="1.0.0"
)

@app.get("/", include_in_schema=False)
def root():
    """تحويل تلقائي إلى واجهة التوثيق"""
    return RedirectResponse(url="/docs")

# =====================================================================
# 1. مسار الصحة والنظام
# =====================================================================
@app.get("/health", tags=["1. System & Health"])
def health_check():
    return {"status": "ok", "message": "API is running securely"}

# =====================================================================
# 2. مسارات الإدخال (Ingestion)
# =====================================================================
@app.post("/ingest/run-sample", tags=["2. ELT Pipeline"])
def trigger_ingest_sample(background_tasks: BackgroundTasks):
    """تشغيل Pipeline الإدخال والتنظيف على العينة التجريبية"""
    background_tasks.add_task(run_elt_pipeline, SAMPLE_CSV_PATH)
    return {"message": "ELT Pipeline started in background for SAMPLE Data."}

@app.post("/ingest/run-huge", tags=["2. ELT Pipeline"])
def trigger_ingest_huge(background_tasks: BackgroundTasks):
    """تشغيل Pipeline الإدخال والتنظيف على الملف الضخم"""
    background_tasks.add_task(run_elt_pipeline, HUGE_CSV_PATH)
    return {"message": "ELT Pipeline started in background for HUGE Data."}

# =====================================================================
# 3. مسارات الفهارس والاستعلامات (Explain)
# =====================================================================
@app.post("/indexes/build-all", tags=["3. Database Indexes & Queries"])
def create_indexes():
    """إنشاء جميع الفهارس (المفردة والمركبة)"""
    DatabaseQueriesAnalyzer().create_project_indexes()
    return {"message": "Indexes created successfully."}

@app.get("/queries/explain-customer-search", tags=["3. Database Indexes & Queries"])
def explain_customer_query():
    """تحليل أداء استعلام البحث عن عميل (قبل وبعد الفهرسة)"""
    return DatabaseQueriesAnalyzer().analyze_performance().get("Q1_Customer")

@app.get("/queries/explain-city-status", tags=["3. Database Indexes & Queries"])
def explain_city_status_query():
    """تحليل أداء الاستعلام المركب (المدينة + الحالة)"""
    return DatabaseQueriesAnalyzer().analyze_performance().get("Q2_Compound")

@app.get("/queries/explain-recent-orders", tags=["3. Database Indexes & Queries"])
def explain_recent_orders_query():
    """تحليل أداء الاستعلام الزمني (الطلبات الحديثة)"""
    return DatabaseQueriesAnalyzer().analyze_performance().get("Q3_DateRange")

# =====================================================================
# 4. مسارات التقارير التجميعية (Aggregations)
# =====================================================================
@app.get("/aggregations/orders-by-status", tags=["4. Analytics Reports"])
def report_orders_by_status():
    """تقرير: توزيع الطلبات حسب الحالة"""
    return {"report": "Orders by Status", "data": AggregationReports().report_orders_by_status()}

@app.get("/aggregations/top-cities", tags=["4. Analytics Reports"])
def report_top_cities():
    """تقرير: أفضل المدن من حيث المبيعات"""
    return {"report": "Top Cities", "data": AggregationReports().report_sales_by_city()}

@app.get("/aggregations/top-customers", tags=["4. Analytics Reports"])
def report_top_customers():
    """تقرير: أفضل 5 عملاء (VIP)"""
    return {"report": "Top Customers", "data": AggregationReports().report_top_customers()}

@app.get("/aggregations/payment-methods", tags=["4. Analytics Reports"])
def report_payment_methods():
    """تقرير: المبيعات حسب طريقة الدفع"""
    return {"report": "Payment Methods", "data": AggregationReports().report_sales_by_payment_method()}

@app.get("/aggregations/delivery-types", tags=["4. Analytics Reports"])
def report_delivery_types():
    """تقرير: أداء أنواع التوصيل"""
    return {"report": "Delivery Types", "data": AggregationReports().report_sales_by_delivery_type()}

# =====================================================================
# 5. مسارات العروض المادية (Materialized Views)
# =====================================================================
@app.post("/materialized-views/refresh-daily-sales", tags=["5. Materialized Views"])
def refresh_daily_sales():
    """التحديث التزايدي لملخص المبيعات اليومية"""
    return MaterializedViewManager().refresh_daily_sales_summary()

@app.post("/materialized-views/refresh-payment-methods", tags=["5. Materialized Views"])
def refresh_payment_methods():
    """التحديث التزايدي لملخص طرق الدفع"""
    return MaterializedViewManager().refresh_payment_method_summary()

# =====================================================================
# 6. مسارات المهام المجدولة (Scheduled Jobs)
# =====================================================================
@app.get("/jobs/logs", tags=["6. Scheduled Jobs"])
def get_job_logs():
    """عرض سجلات التنفيذ للمهام السابقة"""
    manager = JobSchedulerManager()
    logs = list(manager.logs_col.find({}, {"_id": 0}).sort("start_time", -1).limit(10))
    return {"recent_logs": logs}

@app.post("/jobs/trigger-daily-sales", tags=["6. Scheduled Jobs"])
def trigger_job_daily_sales(background_tasks: BackgroundTasks):
    """تشغيل مهمة تحديث المبيعات اليومية الآن"""
    manager = JobSchedulerManager()
    background_tasks.add_task(manager.job_refresh_daily_sales)
    return {"message": "Job 'refresh_daily_sales' triggered in background."}

@app.post("/jobs/trigger-payment-methods", tags=["6. Scheduled Jobs"])
def trigger_job_payment_methods(background_tasks: BackgroundTasks):
    """تشغيل مهمة تحديث طرق الدفع الآن"""
    manager = JobSchedulerManager()
    background_tasks.add_task(manager.job_refresh_payment_methods)
    return {"message": "Job 'refresh_payment_methods' triggered in background."}



# =====================================================================
# [Phase 1 & 2] دوال الـ ELT الأصلية (كما هي بدون تغيير)
# =====================================================================
def classify_and_ingest(file_path):
    print("\n" + "="*50)
    print(" 🛠️ [Phase 1] Classification & Raw Ingestion 🛠️ ")
    print("="*50)
    router_result = route_file(file_path)
    if not router_result:
        raise FileNotFoundError(f"Cannot proceed. File not found: {file_path}")
        
    # 👇 التعديل هنا: استخراج run_id بالاسم الصحيح الذي يرجعه الراوتر
    run_id = router_result["run_id"] 
    engine = router_result["engine"]
    file_size_mb = router_result["file_size_mb"]
    file_name = os.path.basename(file_path)

    print(f"File Size: {file_size_mb:.2f} MB | Selected Engine: {engine} | Run ID: {run_id}")

    ingest_metrics = {}
    if engine == "python_batch":
        # 👇 تمرير المتغير المحدث run_id
        ingest_metrics = load_with_python_batch(file_path, run_id)
    elif engine == "pyspark":
        # 👇 تمرير المتغير المحدث run_id
        ingest_metrics = load_with_pyspark(file_path, run_id)
    else:
        raise ValueError(f"Unknown engine returned: {engine}")

    return run_id, engine, file_size_mb, file_name, ingest_metrics

def run_single_pass_elt(run_id):
    print("\n" + "="*50)
    print(f" 🧠 [Phase 2] Single-Pass ELT for Run ID: {run_id} 🧠 ")
    print("="*50)
    
    spark_elt = SparkSession.builder \
        .appName(f"Midterm_ELT_Processor_{run_id}") \
        .config("spark.mongodb.input.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
        .config("spark.mongodb.output.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.validated_orders") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .getOrCreate()
    
    spark_elt.sparkContext.setLogLevel("ERROR")
    input_uri = f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
    output_uri = input_uri

    elt_metrics = None
    try:
        elt_metrics = validate_and_transform_data(spark_elt, input_uri, output_uri, run_id)
    except Exception as e:
        print(f"❌ Error during ELT execution: {e}")
    finally:
        print("🔌 Stopping Spark Session securely...")
        spark_elt.stop()
    
    return elt_metrics

def run_elt_pipeline(file_path):
    """تم تغيير الاسم من main إلى run_elt_pipeline لفصلها عن الـ API"""
    print("\n" + "#"*60)
    print("🚀 STARTING END-TO-END MODULAR DATA PIPELINE 🚀")
    print("#"*60)

    start_total_time = time.time()
    try:
        run_id, engine, file_size_mb, file_name, ingest_metrics = classify_and_ingest(file_path)
        elt_metrics = run_single_pass_elt(run_id)
        if elt_metrics is None: elt_metrics = {}

        total_elapsed = round(time.time() - start_total_time, 2)
        total_rows = ingest_metrics.get("total_records_inserted", elt_metrics.get("rows_read", 0))
        total_throughput = total_rows / total_elapsed if total_elapsed > 0 else 0

        final_report = {
            "_id": run_id,
            "file_name": file_name,
            "file_size_mb": round(file_size_mb, 2),
            "engine_used": f"{engine} + PySpark_SinglePass_ELT",
            "rows_read": total_rows,
            "raw_loaded": total_rows,
            "valid_count": elt_metrics.get("valid_count", 0),
            "corrected_count": elt_metrics.get("corrected_count", 0),
            "quarantine_count": elt_metrics.get("quarantine_count", 0),
            "elapsed_seconds": total_elapsed,
            "throughput": round(total_throughput, 2),
            "batch_size_partitions": elt_metrics.get("batch_size_partitions", ingest_metrics.get("batch_size_partitions", 0)),
            "error_case_counts": elt_metrics.get("error_case_counts", {}),
            "upsert_inserted_count": elt_metrics.get("upsert_inserted_count", 0),
            "upsert_updated_count": elt_metrics.get("upsert_updated_count", 0),
            "upsert_unchanged_count": elt_metrics.get("upsert_unchanged_count", 0)
        }
        
        save_run_metrics(final_report)
        print("\n" + "="*60)
        print(" 📊 MASTER REPORT SUMMARY 📊")
        print("="*60)
        print(f"Run ID        : {final_report['_id']}")
        print(f"Total Rows    : {final_report['rows_read']}")
        print(f"Valid         : {final_report['valid_count']}")
        print(f"Total Time    : {final_report['elapsed_seconds']} sec")
        print("="*60 + "\n")
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR during pipeline execution: {e}")

# =====================================================================
# القائمة التفاعلية في اللوحة الطرفية المحدثة
# =====================================================================
if __name__ == "__main__":
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    
    while True:
        print("\n" + "*"*60)
        print(" 🚀 نظام إدارة خطوط البيانات والتحليلات (Data & Analytics) 🚀 ")
        print("*"*60)
        print("\nاختر وضع التشغيل الذي تريده:")
        print("  [1] تشغيل واجهة الويب التفاعلية (FastAPI Server / Swagger UI)")
        
        csv_files = []
        if os.path.exists(DATA_DIR):
            csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
        
        # عرض الملفات كخيارات ديناميكية (يبدأ الترقيم من 2)
        for idx, file_name in enumerate(csv_files, start=2):
            print(f"  [{idx}] {file_name}")
            
        print("  [0] خروج من النظام")
        
        choice = input("\n👉 أدخل رقم الخيار: ").strip()
        
        if choice == '1':
            print(f"\n🌐 جاري تشغيل سيرفر الويب... الرجاء زيارة: http://127.0.0.1:8000/docs")
            uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
            break
        elif choice == '0':
            print("\n👋 تم إغلاق النظام. إلى اللقاء!")
            sys.exit(0)
        else:
            try:
                choice_idx = int(choice)
                if 2 <= choice_idx < 2 + len(csv_files):
                    selected_file = csv_files[choice_idx - 2]
                    file_path = os.path.join(DATA_DIR, selected_file)
                    print(f"\n🔄 جاري تجهيز النظام لمعالجة الملف: {selected_file}...")
                    run_elt_pipeline(file_path)
                    break
                else:
                    print("⚠️ خيار غير صالح، الرجاء المحاولة مرة أخرى.")
            except ValueError:
                print("⚠️ يرجى إدخال رقم صحيح.")