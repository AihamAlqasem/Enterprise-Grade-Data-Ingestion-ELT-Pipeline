import time
import os
import sys
from datetime import datetime
from pymongo import MongoClient

os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SPARK_MONGO_OUTPUT_URI, SPARK_MONGO_PACKAGES, MONGO_URI, MONGO_DB_NAME
from src.metrics import save_run_metrics

TARGET_COLLECTION = "order_raw"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, current_timestamp, struct, monotonically_increasing_id

def load_with_pyspark(file_path, id_run):
    """
    يقرأ ملف CSV الضخم باستخدام PySpark ويكتبه بسرعة فائقة في MongoDB بشكل متوازٍ.
    تم إلغاء الـ Upsert والـ Idempotency لضمان السرعة القصوى للإدراج المباشر.
    """
    print(f"\n--- 🚀 Starting PySpark Fast Ingestion for Run ID: {id_run} ---")
    start_time = time.time()
    file_name = os.path.basename(file_path)

    # 1. تهيئة جلسة Spark مع ذاكرة 12 جيجا لضمان معالجة البيانات الضخمة بدون انهيار
    print("⏳ Initializing Spark Session (12GB Memory allocated)...")
    spark = SparkSession.builder \
        .appName("Midterm_PySpark_Fast_Loader") \
        .config("spark.mongodb.output.uri", SPARK_MONGO_OUTPUT_URI) \
        .config("spark.jars.packages", SPARK_MONGO_PACKAGES) \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 2. بناء Schema ثابتة (كلها String للحفاظ على الأخطاء كما هي في الطبقة الخام)
    # ⚠️ شرط أساسي: عدم استخدام inferSchema في الملفات الضخمة لتجنب البطء والأخطاء
    csv_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True)
    ])

    # 3. قراءة الملف الضخم بشكل متوازٍ
    print(f"📖 Reading CSV file: {file_name}...")
    df_raw = spark.read.csv(
        file_path,
        header=True,
        schema=csv_schema,
        enforceSchema=False
    )

    num_partitions = df_raw.rdd.getNumPartitions()
    print(f"🔗 File naturally split into {num_partitions} input partitions (No manual repartitioning applied to avoid shuffle).")

    # 4. تغليف البيانات لتطابق معمارية الـ Raw Layer (إضافة الميتاداتا)
    print("🛠️ Structuring data with metadata (id_run, file_source, etc.)...")
    columns_to_struct = [df_raw[col] for col in df_raw.columns]
    
    df_elt = df_raw.select(
        lit(id_run).alias("id_run"),
        lit(file_name).alias("file_source"),
        monotonically_increasing_id().alias("number_row_source"), 
        current_timestamp().alias("at_ingested"),
        lit("pyspark_fast").alias("engine_used"),
        struct(*columns_to_struct).alias("record_raw")
    )

    # 5. الكتابة المتوازية السريعة المباشرة إلى MongoDB (Append Mode) عبر Spark Connector
    print(f"📦 Writing massive data in parallel directly to MongoDB collection '{TARGET_COLLECTION}'...")
    df_elt.write \
        .format("mongo") \
        .mode("append") \
        .save()

    # 6. حساب المقاييس بسرعة باستخدام MongoDB لتجنب بطء أمر count() الخاص بـ Spark
    print("📊 Calculating final metrics...")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB_NAME]
    total_inserted = db[TARGET_COLLECTION].count_documents({"id_run": id_run})
    mongo_client.close()

    total_time = time.time() - start_time
    throughput = total_inserted / total_time if total_time > 0 else 0

    print("\n--- PySpark Fast Loading Summary ---")
    print(f"Target Collection: {TARGET_COLLECTION}")
    print(f"Total Records Inserted: {total_inserted}")
    print(f"Input Partitions: {num_partitions}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} records/second")
    print("---------------------------------------\n")

   # 7. حفظ التقرير في ملف results.json وفقاً للشروط
    metrics = {
        "total_records_inserted": total_inserted,
        "total_time_seconds": round(total_time, 2),
        "throughput_records_per_second": round(throughput, 2),
        "batch_size_partitions": num_partitions,
        "upsert_inserted_count": total_inserted, 
        "upsert_updated_count": 0,
        "upsert_unchanged_count": 0,
        "mode": "parallel_insert (no upsert, no shuffle)",
        "collection": TARGET_COLLECTION
    }

    spark_report = {
        "id_run": id_run,
        "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "phase": "Ingestion_Spark_Batch",
        "metrics": metrics,
        "status": "Success" if total_inserted > 0 else "Failed"
    }
    
    # حفظ التقرير باستخدام الدالة الأصلية الخاصة بك
    save_run_metrics(spark_report)
    print("✅ Spark Ingestion metrics successfully saved to reports/results.json")

    spark.stop()
    return metrics

# وضع الاختبار المستقل (ملاحظة: القيمة الثابتة هنا للاختبار، وستتغير عند التشغيل من main.py)
if __name__ == "__main__":
    from config.settings import HUGE_CSV_PATH
    test_id_run = "spark_test_run_001"
    load_with_pyspark(HUGE_CSV_PATH, test_id_run)
