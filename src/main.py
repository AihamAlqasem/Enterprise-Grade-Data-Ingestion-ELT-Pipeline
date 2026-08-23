import os
import sys
import time
from datetime import datetime

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import HUGE_CSV_PATH, SAMPLE_CSV_PATH, MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME

# استدعاء الأدوات والمراحل
from src.file_router import route_file
from src.batch_loader import load_with_python_batch
from src.spark_loader import load_with_pyspark
from src.metrics import save_run_metrics
from src.spark_elt_pipeline import validate_and_transform_data
from pyspark.sql import SparkSession

# =====================================================================
# [Phase 1] دالة التقسيم والتوجيه والتحميل الخام
# =====================================================================
def classify_and_ingest(file_path):
    print("\n" + "="*50)
    print(" 🛠️ [Phase 1] Classification & Raw Ingestion 🛠️ ")
    print("="*50)
    
    router_result = route_file(file_path)
    if not router_result:
        raise FileNotFoundError(f"Cannot proceed. File not found: {file_path}")
        
    id_run = router_result["id_run"]
    engine = router_result["engine"]
    file_size_mb = router_result["file_size_mb"]
    file_name = os.path.basename(file_path)

    print(f"File Size: {file_size_mb:.2f} MB | Selected Engine: {engine} | Run ID: {id_run}")

    ingest_metrics = {}
    if engine == "python_batch":
        ingest_metrics = load_with_python_batch(file_path, id_run)
    elif engine == "pyspark":
        ingest_metrics = load_with_pyspark(file_path, id_run)
    else:
        raise ValueError(f"Unknown engine returned: {engine}")

    return id_run, engine, file_size_mb, file_name, ingest_metrics

# =====================================================================
# [Phase 2] دالة التنظيف، التحقق، والعزل (Single-Pass ELT)
# =====================================================================
def run_single_pass_elt(id_run):
    print("\n" + "="*50)
    print(f" 🧠 [Phase 2] Single-Pass ELT for Run ID: {id_run} 🧠 ")
    print("="*50)
    
    # تهيئة Spark
    spark_elt = SparkSession.builder \
        .appName(f"Midterm_ELT_Processor_{id_run}") \
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
        # استدعاء دالة الـ ELT الشاملة
        elt_metrics = validate_and_transform_data(spark_elt, input_uri, output_uri, id_run)
    except Exception as e:
        print(f"❌ Error during ELT execution: {e}")
    finally:
        # إغلاق آمن لـ Spark كما طلبت
        print("🔌 Stopping Spark Session securely...")
        spark_elt.stop()
    
    return elt_metrics

# =====================================================================
# الدالة الرئيسية المنظمة (Main Orchestrator)
# =====================================================================
def main(file_path):
    print("\n" + "#"*60)
    print("🚀 STARTING END-TO-END MODULAR DATA PIPELINE 🚀")
    print("#"*60)

    start_total_time = time.time()
    
    try:
        # 1. التقسيم والتحميل الخام
        id_run, engine, file_size_mb, file_name, ingest_metrics = classify_and_ingest(file_path)

        # 2. التنظيف والفحص والتصحيح والعزل (كلها في تمريرة واحدة)
        elt_metrics = run_single_pass_elt(id_run)
        if elt_metrics is None: elt_metrics = {}

        # 3. تجميع المقاييس في الشكل النهائي المطلوب (Master Report)
        total_elapsed = round(time.time() - start_total_time, 2)
        total_rows = ingest_metrics.get("total_records_inserted", elt_metrics.get("rows_read", 0))
        total_throughput = total_rows / total_elapsed if total_elapsed > 0 else 0

        final_report = {
            "_id": id_run,
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
        
        # الحفظ النهائي للتقرير الشامل 
        save_run_metrics(final_report)
        
        # الطباعة في الـ Terminal كما طلبت
        print("\n" + "="*60)
        print(" 📊 MASTER REPORT SUMMARY 📊")
        print("="*60)
        print(f"Run ID        : {final_report['_id']}")
        print(f"File          : {final_report['file_name']} ({final_report['file_size_mb']} MB)")
        print(f"Engine        : {final_report['engine_used']}")
        print(f"Total Rows    : {final_report['rows_read']}")
        print(f"Valid         : {final_report['valid_count']}")
        print(f"Corrected     : {final_report['corrected_count']}")
        print(f"Quarantined   : {final_report['quarantine_count']}")
        print(f"Total Time    : {final_report['elapsed_seconds']} sec")
        print(f"Throughput    : {final_report['throughput']} rec/sec")
        print("="*60)
        print("✅ FULL PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        print("="*60 + "\n")
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR during pipeline execution: {e}")

# =====================================================================
# القائمة التفاعلية في اللوحة الطرفية (Interactive Terminal Menu)
# =====================================================================
if __name__ == "__main__":
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    
    print("\n" + "*"*50)
    print(" 🛠️  نظام معالجة وتدقيق البيانات (ELT Pipeline) 🛠️ ")
    print("*"*50)
    print("\nاختر مسار البيانات الذي تريد البدء بتحميله ومعالجته:")
    print("  [1] معالجة البيانات الضخمة (HUGE_CSV_PATH)")
    print("  [2] معالجة عينة تجريبية (SAMPLE_CSV_PATH)")
    print("  [0] خروج من النظام")
    
    while True:
        choice = input("\n👉 أدخل رقم الخيار (0, 1, 2): ").strip()
        
        if choice == '1':
            print(f"\n🔄 جاري تجهيز النظام لمعالجة البيانات الضخمة...")
            main(HUGE_CSV_PATH)
            break
        elif choice == '2':
            print(f"\n🔄 جاري تجهيز النظام لمعالجة العينة التجريبية...")
            main(SAMPLE_CSV_PATH)
            break
        elif choice == '0':
            print("\n👋 تم إغلاق النظام. إلى اللقاء!")
            sys.exit(0)
        else:
            print("⚠️ خيار غير صالح، الرجاء إدخال 1 أو 2 أو 0.")