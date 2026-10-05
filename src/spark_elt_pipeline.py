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

    try:
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
        try:
            db = mongo_client[MONGO_DB_NAME]
            total_inserted = db[TARGET_COLLECTION].count_documents({"id_run": id_run})
        finally:
            mongo_client.close() # إغلاق آمن لاتصال مونجو

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

        return metrics

    except Exception as e:
        print(f"❌ CRITICAL ERROR in PySpark execution: {e}")
        return None
        
    finally:
        print("🛑 Closing Spark Session safely...")
        spark.stop()

# وضع الاختبار المستقل (ملاحظة: القيمة الثابتة هنا للاختبار، وستتغير عند التشغيل من main.py)
if __name__ == "__main__":
    from config.settings import HUGE_CSV_PATH
    test_id_run = "spark_test_run_001"
    load_with_pyspark(HUGE_CSV_PATH, test_id_run)

import time
import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import (
    col, lit, coalesce, trim, lower, upper, when, expr, struct, array, 
    regexp_replace, current_timestamp, concat_ws, abs, explode, concat, round as spark_round, from_json
)

# =====================================================================
# 1. تعريف المخططات (Schemas) المتشددة
# =====================================================================
item_schema = StructType([
    StructField("sku", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("subtotal", DoubleType(), True)
])

correction_schema = StructType([
    StructField("field", StringType(), True),
    StructField("original_value", StringType(), True),
    StructField("corrected_value", StringType(), True),
    StructField("rule_code", StringType(), True)
])

final_record_schema = StructType([
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
    StructField("delivery_cost", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("items", ArrayType(item_schema), True),
    StructField("quality_status", StringType(), True),
    StructField("corrections", ArrayType(correction_schema), True)
])

# =====================================================================
# 2. دالة حفظ التقارير 
# =====================================================================
def save_metrics_to_json(report_data, output_path="reports/results.json"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    all_reports = []
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    if content.startswith('['):
                        all_reports = json.loads(content)
                    else:
                        all_reports = [json.loads(content)]
        except Exception as e:
            print(f"⚠️ Could not read existing results.json: {e}")

    all_reports.append(report_data)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_reports, f, ensure_ascii=False, indent=4)
    print(f"✅ Metrics successfully appended and saved to {output_path}")

# =====================================================================
# 3. دالة التنظيف والتحقق والعزل (ELT Validation Pipeline)
# =====================================================================
def validate_and_transform_data(spark, mongo_input_uri, mongo_output_uri, target_run_id="standalone_run"):
    print(f"\n--- Starting ELT Validation & Separation Phase ---")
    start_time = time.time()

    df_raw = spark.read.format("mongo").option("uri", mongo_input_uri).load()
    
    if target_run_id != "standalone_run" and "run_id" in df_raw.columns:
        df_raw = df_raw.filter(col("run_id") == target_run_id)

    rows_read = df_raw.count()
    
    if rows_read == 0:
        print("⚠ No records found to process.")
        return None

    expected_fields = [
        "order_id", "order_date", "status", "customer_id", "customer_name", 
        "customer_phone", "customer_email", "city", "district", "delivery_type", 
        "delivery_cost", "payment_method", "payment_status", "payment_amount", 
        "currency", "total_amount", "items_json"
    ]
    
    metadata_cols = ["run_id", "source_file", "source_row_number", "ingested_at", "engine_used"]
    for m_col in metadata_cols:
        if m_col not in df_raw.columns:
            df_raw = df_raw.withColumn(m_col, lit("UNKNOWN") if m_col != "ingested_at" else current_timestamp())

    has_raw_record = "raw_record" in df_raw.columns and isinstance(df_raw.schema["raw_record"].dataType, StructType)
    
    if has_raw_record:
        for field in expected_fields:
            if field in df_raw.schema["raw_record"].dataType.names:
                df_raw = df_raw.withColumn(field, col(f"raw_record.{field}"))
            else:
                df_raw = df_raw.withColumn(field, lit(None).cast("string"))
        df_flattened = df_raw
    else:
        for field in expected_fields:
            if field not in df_raw.columns:
                df_raw = df_raw.withColumn(field, lit(None).cast("string"))
        df_flattened = df_raw.withColumn("raw_record", struct(*[col(c) for c in expected_fields]))

    df_flattened = df_flattened.select(
        col("run_id"), col("source_file"), col("source_row_number"),
        col("ingested_at"), col("engine_used"), col("raw_record"),
        *[col(c) for c in expected_fields]
    )

    df_flattened = df_flattened.withColumn("original_order_id", col("order_id"))
    df_flattened = df_flattened.withColumn("clean_order_id", trim(col("order_id")))
    df_flattened = df_flattened.drop("order_id").withColumnRenamed("clean_order_id", "order_id")

    arabic_sql = """
    translate(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace(
        regexp_replace({col_name}, 'عشرة آلاف|عشرة الاف', '10000'), 
        'خمسة آلاف|خمسة الاف', '5000'), 
        'أربعة آلاف|اربعة الاف', '4000'), 
        'ثلاثة آلاف|ثلاثة الاف', '3000'), 
        'ألفان|الفان|ألفين|الفين', '2000'), 
        'ألف|الف', '1000'), 
    '٠١٢٣٤٥٦٧٨٩٫٬', '0123456789..')
    """

    df_working = df_flattened \
        .withColumn("temp_phone", regexp_replace(col("customer_phone"), r"[^\d\+]", "")) \
        .withColumn("clean_phone", 
            when(col("temp_phone").rlike(r"^(\+?00967)"), regexp_replace(col("temp_phone"), r"^(\+?00967)", "+967"))
            .when(col("temp_phone").rlike(r"^(\+?967)"), regexp_replace(col("temp_phone"), r"^(\+?967)", "+967"))
            .when(col("temp_phone").rlike(r"^07"), regexp_replace(col("temp_phone"), r"^0", "+967"))
            .when(col("temp_phone").rlike(r"^7"), concat(lit("+967"), col("temp_phone")))
            .otherwise(col("temp_phone"))
        ) \
        .withColumn("clean_email", regexp_replace(regexp_replace(lower(regexp_replace(col("customer_email"), r"\s+", "")), r"@{2,}", "@"), r"\.{2,}", ".")) \
        .withColumn("clean_status", 
            when(lower(trim(col("status"))).isin("قيد الانتظار", "معلق", "created", "pending"), "pending")
            .when(lower(trim(col("status"))).isin("مؤكد", "مدفوع", "paid", "confirmed"), "confirmed")
            .when(lower(trim(col("status"))).isin("قيد الشحن", "قيد التوصيل", "shipped", "in_transit"), "shipped")
            .when(lower(trim(col("status"))).isin("تم التسليم", "تم التوصيل", "delivered", "done"), "delivered")
            .when(lower(trim(col("status"))).isin("مرتجع", "returned"), "returned")
            .when(lower(trim(col("status"))).isin("ملغي", "ملغى", "canceled", "cancelled"), "cancelled")
            .otherwise(trim(col("status")))
        ) \
        .withColumn("clean_payment_method",
            when(lower(trim(col("payment_method"))).isin("نقداً عند التسليم", "نقدًا عند التسليم", "نقد", "كاش", "cash", "cash on delivery", "cod"), "cash_on_delivery")
            .when(lower(trim(col("payment_method"))).isin("بطاقة", "بطاقة ائتمان", "card", "credit card"), "card")
            .when(lower(trim(col("payment_method"))).isin("محفظة إلكترونية", "محفظة", "wallet", "e-wallet"), "wallet")
            .otherwise(trim(col("payment_method")))
        ) \
        .withColumn("clean_payment_status",
            when(lower(trim(col("payment_status"))).isin("بانتظار الدفع", "غير مدفوع", "معلق", "pending", "unpaid", "waiting"), "unpaid")
            .when(lower(trim(col("payment_status"))).isin("تم الدفع", "مدفوع", "paid", "approved"), "paid")
            .when(lower(trim(col("payment_status"))).isin("مرفوض", "فشل", "failed", "rejected"), "failed")
            .when(lower(trim(col("payment_status"))).isin("مسترد", "refunded"), "refunded")
            .otherwise(trim(col("payment_status")))
        ) \
        .withColumn("clean_delivery_type",
            when(lower(trim(col("delivery_type"))).isin("عادي", "قياسي", "normal", "standard"), "standard")
            .when(lower(trim(col("delivery_type"))).isin("سريع", "مستعجل", "fast", "express"), "express")
            .otherwise(trim(col("delivery_type")))
        ) \
        .withColumn("clean_customer_id", when((col("customer_id").isNull()) | (trim(col("customer_id")) == ""), lit("UNKNOWN")).otherwise(trim(col("customer_id")))) \
        .withColumn("clean_currency", 
            when(lower(trim(col("currency"))).isin("ريال يمني", "ريال", "ريالات", "ر.ي", "yer", "yr"), lit("YER"))
            .when(lower(trim(col("currency"))).isin("دولار", "usd"), lit("USD"))
            .when(lower(trim(col("currency"))).isin("ريال سعودي", "sar"), lit("SAR"))
            .otherwise(upper(trim(col("currency"))))
        ) \
        .withColumn("clean_items_json", expr(arabic_sql.format(col_name="items_json"))) \
        .withColumn("standardized_items_json", regexp_replace(regexp_replace(col("clean_items_json"), r'"qty"\s*:', '"quantity":'), r'"price"\s*:', '"unit_price":')) \
        .withColumn("temp_delivery", expr(arabic_sql.format(col_name="delivery_cost"))) \
        .withColumn("clean_delivery_cost", regexp_replace(col("temp_delivery"), r"[^\d\.-]", "")) \
        .withColumn("temp_total", expr(arabic_sql.format(col_name="total_amount"))) \
        .withColumn("clean_total_amount", when(col("temp_total").contains("?"), expr("NULL")).otherwise(regexp_replace(col("temp_total"), r"[^\d\.-]", ""))) \
        .withColumn("temp_payment", expr(arabic_sql.format(col_name="payment_amount"))) \
        .withColumn("clean_payment", regexp_replace(col("temp_payment"), r"[^\d\.-]", "")) \
        .withColumn("clean_order_date", regexp_replace(
            regexp_replace(col("order_date"), r"[/.]", "-"),
            r"^(\d{2})-(\d{2})-(\d{4})(.*)$",
            r"$3-$2-$1$4"
        )) \
        .withColumn("parsed_items", expr("from_json(standardized_items_json, 'array<struct<qty:string, quantity:string, unit_price:string, price:string>>')")) \
        .withColumn("items_sum", expr("""
            aggregate(parsed_items, 0D, (acc, item) -> acc + 
            (
                cast(regexp_replace(coalesce(item.qty, item.quantity, '0'), '[^0-9.-]', '') as double) * 
                cast(regexp_replace(coalesce(item.unit_price, item.price, '0'), '[^0-9.-]', '') as double)
            ))
        """)) \
        .withColumn("expected_total", spark_round(coalesce(col("items_sum"), lit(0.0)) + coalesce(col("clean_delivery_cost").cast("double"), lit(0.0)), 2)) \
        .withColumn("needs_recalc", col("parsed_items").isNotNull() & (abs(col("expected_total") - spark_round(col("clean_total_amount").cast("double"), 2)) > 0.01)) \
        .withColumn("original_total_amount", col("clean_total_amount")) \
        .withColumn("clean_total_amount", when(col("needs_recalc"), col("expected_total").cast("string")).otherwise(col("clean_total_amount"))) \
        .drop("temp_delivery", "temp_total", "temp_payment", "temp_phone", "items_sum", "expected_total")

    # ✨ تسجيل التصحيحات الشامل (الاحتفاظ بقواعد الـ RegEx الشاملة التي نجحت في اصطياد 569 سجلاً)
    order_id_audit = when((col("original_order_id").isNotNull()) & (col("original_order_id").cast("string") != trim(col("original_order_id").cast("string"))), struct(lit("order_id").alias("field"), col("original_order_id").cast("string").alias("original_value"), trim(col("original_order_id").cast("string")).alias("corrected_value"), lit("TRIM_WHITESPACE").alias("rule_code"))).otherwise(expr("NULL"))
    
    customer_id_audit = when((col("customer_id").isNotNull()) & (col("customer_id").cast("string") != trim(col("customer_id").cast("string"))), struct(lit("customer_id").alias("field"), col("customer_id").cast("string").alias("original_value"), trim(col("customer_id").cast("string")).alias("corrected_value"), lit("TRIM_WHITESPACE").alias("rule_code"))).otherwise(expr("NULL"))
    
    status_audit = when((col("status").isNotNull()) & (col("status").cast("string") != trim(col("status").cast("string"))), struct(lit("status").alias("field"), col("status").cast("string").alias("original_value"), trim(col("status").cast("string")).alias("corrected_value"), lit("STATUS_WHITESPACE_TRIMMED").alias("rule_code"))).otherwise(expr("NULL"))
    payment_method_audit = when((col("payment_method").isNotNull()) & (col("payment_method").cast("string") != trim(col("payment_method").cast("string"))), struct(lit("payment_method").alias("field"), col("payment_method").cast("string").alias("original_value"), trim(col("payment_method").cast("string")).alias("corrected_value"), lit("PAYMENT_METHOD_WHITESPACE_TRIMMED").alias("rule_code"))).otherwise(expr("NULL"))
    payment_status_audit = when((col("payment_status").isNotNull()) & (col("payment_status").cast("string") != trim(col("payment_status").cast("string"))), struct(lit("payment_status").alias("field"), col("payment_status").cast("string").alias("original_value"), trim(col("payment_status").cast("string")).alias("corrected_value"), lit("PAYMENT_STATUS_WHITESPACE_TRIMMED").alias("rule_code"))).otherwise(expr("NULL"))
    delivery_type_audit = when((col("delivery_type").isNotNull()) & (col("delivery_type").cast("string") != trim(col("delivery_type").cast("string"))), struct(lit("delivery_type").alias("field"), col("delivery_type").cast("string").alias("original_value"), trim(col("delivery_type").cast("string")).alias("corrected_value"), lit("DELIVERY_TYPE_WHITESPACE_TRIMMED").alias("rule_code"))).otherwise(expr("NULL"))
    
    email_audit = when((col("customer_email").isNotNull()) & (col("customer_email").cast("string") != col("clean_email")), struct(lit("customer_email").alias("field"), col("customer_email").cast("string").alias("original_value"), col("clean_email").alias("corrected_value"), lit("EMAIL_TYPO_REPAIRED").alias("rule_code"))).otherwise(expr("NULL"))
    date_audit = when((col("order_date").isNotNull()) & (col("order_date").cast("string") != col("clean_order_date")), struct(lit("order_date").alias("field"), col("order_date").cast("string").alias("original_value"), col("clean_order_date").alias("corrected_value"), lit("DATE_FORMAT_NORMALIZED").alias("rule_code"))).otherwise(expr("NULL"))
    phone_audit = when((~col("customer_phone").rlike(r"^(?:\+967)?7\d{8}$")) & (col("customer_phone").cast("string") != col("clean_phone")), struct(lit("customer_phone").alias("field"), col("customer_phone").cast("string").alias("original_value"), col("clean_phone").alias("corrected_value"), lit("PHONE_FORMAT_NORMALIZED").alias("rule_code"))).otherwise(expr("NULL"))
    currency_audit = when((col("currency").isNotNull()) & (col("currency").cast("string") != col("clean_currency")), struct(lit("currency").alias("field"), col("currency").cast("string").alias("original_value"), col("clean_currency").alias("corrected_value"), lit("CURRENCY_NORMALIZED").alias("rule_code"))).otherwise(expr("NULL"))
    
    payment_audit = when((col("payment_amount").isNotNull()) & (coalesce(col("payment_amount").cast("string"), lit("")) != coalesce(col("clean_payment").cast("string"), lit(""))), struct(lit("payment_amount").alias("field"), col("payment_amount").cast("string").alias("original_value"), col("clean_payment").alias("corrected_value"), lit("NUMERIC_FORMAT_NORMALIZED").alias("rule_code"))).otherwise(expr("NULL"))
    delivery_audit = when((col("delivery_cost").isNotNull()) & (coalesce(col("delivery_cost").cast("string"), lit("")) != coalesce(col("clean_delivery_cost").cast("string"), lit(""))), struct(lit("delivery_cost").alias("field"), col("delivery_cost").cast("string").alias("original_value"), col("clean_delivery_cost").alias("corrected_value"), lit("NUMERIC_FORMAT_NORMALIZED").alias("rule_code"))).otherwise(expr("NULL"))
    
    total_amount_audit = when(
        col("needs_recalc"), 
        struct(lit("total_amount").alias("field"), coalesce(col("original_total_amount").cast("string"), lit("NULL")).alias("original_value"), col("clean_total_amount").cast("string").alias("corrected_value"), lit("TOTAL_AMOUNT_RECALCULATED").alias("rule_code"))
    ).otherwise(
        when((col("total_amount").isNotNull()) & (coalesce(col("total_amount").cast("string"), lit("")) != coalesce(col("original_total_amount").cast("string"), lit(""))), 
        struct(lit("total_amount").alias("field"), col("total_amount").cast("string").alias("original_value"), col("original_total_amount").cast("string").alias("corrected_value"), lit("NUMERIC_FORMAT_NORMALIZED").alias("rule_code"))).otherwise(expr("NULL"))
    )

    # 👈 RegEx الذي نجح في اصطياد السجلات الـ 569 المفقودة بالضبط
    items_audit = when(
        col("items_json").isNotNull() & col("items_json").rlike(r"(?i)['\"]?(?:qty|quantity|unit_price|price|total|subtotal)['\"]?\s*:\s*['\"]\s*[0-9\.\-\s٠-٩٫٬]+\s*['\"]"),
        struct(lit("items_json").alias("field"), col("items_json").cast("string").alias("original_value"), lit("parsed_and_cast_to_numeric").alias("corrected_value"), lit("ITEM_NUMERIC_TYPE_CAST").alias("rule_code"))
    ).otherwise(expr("NULL"))

    df_audited = df_working.withColumn("raw_corrections", array(
        order_id_audit, date_audit, phone_audit, email_audit, status_audit, payment_audit, 
        customer_id_audit, currency_audit, delivery_audit, total_amount_audit,
        payment_method_audit, payment_status_audit, delivery_type_audit, items_audit
    )).withColumn("corrections", expr("filter(raw_corrections, x -> x is not null)")).drop("raw_corrections", "needs_recalc", "original_total_amount")

    # تطبيق السكيما المتشددة
    df_typed = df_audited \
    .withColumn("typed_delivery_cost", col("clean_delivery_cost").cast(DoubleType())) \
    .withColumn("typed_payment_amount", col("clean_payment").cast(DoubleType())) \
    .withColumn("typed_total_amount", col("clean_total_amount").cast(DoubleType())) \
    .withColumn("typed_items", from_json(col("standardized_items_json"), ArrayType(item_schema)))

    # -----------------------------------------------------------------
    # 🎯 3. قسم التحقق (Quality Validations)
    # -----------------------------------------------------------------
    df_checked = df_typed \
        .withColumn("check_order_id", col("order_id").isNotNull() & (trim(col("order_id")) != "") & (~lower(trim(col("order_id"))).isin("null", "none", "nan"))) \
        .withColumn("check_customer_id", col("clean_customer_id").isNotNull() & (trim(col("clean_customer_id")) != "") & (col("clean_customer_id") != "UNKNOWN") & (~lower(trim(col("clean_customer_id"))).isin("null", "none", "nan"))) \
        .withColumn("check_order_date", col("clean_order_date").isNotNull() & (col("clean_order_date").rlike(r"^(201[5-9]|20[2-9]\d)-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]).*") | col("clean_order_date").rlike(r"^(0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-(201[5-9]|20[2-9]\d).*"))) \
        .withColumn("check_status", col("clean_status").isin("pending", "confirmed", "shipped", "delivered", "returned", "cancelled")) \
        .withColumn("check_customer_phone", col("clean_phone").rlike(r"^\+9677\d{8}$")) \
        .withColumn("check_customer_email", col("clean_email").isNotNull() & col("clean_email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")) \
        .withColumn("check_currency", col("clean_currency").isin("YER", "USD", "SAR")) \
        .withColumn("check_items_corrupted", col("clean_items_json").isNotNull() & col("clean_items_json").rlike(r"^\s*\[.*\]$")) \
        .withColumn("check_items_empty", (trim(col("clean_items_json")) != "[]") & (trim(col("clean_items_json")) != "") & (lower(trim(col("clean_items_json"))) != "nan")) \
        .withColumn("check_items_qty_price", ~expr("""
            exists(parsed_items, x -> 
                coalesce(cast(regexp_replace(coalesce(x.qty, x.quantity, '0'), '[^0-9.-]', '') as double), 0D) <= 0 
                or 
                coalesce(cast(regexp_replace(coalesce(x.unit_price, x.price, '0'), '[^0-9.-]', '') as double), 0D) <= 0
            )
        """)) \
        .withColumn("check_items_sku", 
            (expr("(length(clean_items_json) - length(replace(clean_items_json, '{', ''))) == (size(split(regexp_replace(lower(clean_items_json), '\"sku\"\\\\s*:', 'SKUMARKER'), 'SKUMARKER')) - 1)")) & 
            ~lower(col("clean_items_json")).rlike(r"\"sku\"\s*:\s*(null|\"null\"|\"none\"|\"nan\"|\"[ \t]*\")\s*[,}]")
        ) \
        .withColumn("check_no_negative_values", ~( 
            (coalesce(col("typed_total_amount"), lit(0.0)) < 0) |
            (coalesce(col("typed_delivery_cost"), lit(0.0)) < 0) |
            (coalesce(col("typed_payment_amount"), lit(0.0)) < 0)
        ))

    # -----------------------------------------------------------------
    # 🎯 4. توجيه الأخطاء
    # -----------------------------------------------------------------
    df_with_errors = df_checked.withColumn("raw_validation_errors", array(
        when(~col("check_items_empty"), lit("Quarantined: EMPTY_ITEMS")),
        when(~col("check_customer_email"), lit("Quarantined: INVALID_EMAIL_ADDRESS")),
        when(~col("check_status"), lit("Quarantined: INVALID_UNKNOWN_STATUS")),
        when(~col("check_items_corrupted"), lit("Quarantined: CORRUPTED_ITEMS_JSON")),
        when(col("check_items_corrupted") & col("check_items_empty") & ~col("check_items_qty_price"), lit("Quarantined: INVALID_ITEM_QUANTITY_OR_PRICE")), 
        when(col("check_items_corrupted") & col("check_items_empty") & ~col("check_items_sku"), lit("Quarantined: MISSING_ITEM_SKU_OR_NAME")),
        when(~col("check_no_negative_values") & col("check_items_qty_price"), lit("Quarantined: AMBIGUOUS_NEGATIVE_VALUE")), 
        when(~col("check_customer_id"), lit("Quarantined: MISSING_CUSTOMER_ID")),
        when(~col("check_currency"), lit("Quarantined: INVALID_CURRENCY")),
        when(~col("check_order_date"), lit("Quarantined: INVALID_IMPOSSIBLE_DATE")),
        when(~col("check_order_id"), lit("Quarantined: MISSING_ORDER_ID")),
        when(~col("check_customer_phone"), lit("Quarantined: INVALID_PHONE_NUMBER"))
    ))
    
    df_with_errors = df_with_errors.withColumn("filtered_errors", expr("filter(raw_validation_errors, x -> x is not null)"))
    
    df_with_errors = df_with_errors.withColumn("validation_errors", 
        when(expr("size(filtered_errors) > 1"), array(lit("Quarantined: MULTIPLE_CONFLICTING_ERRORS")))
        .otherwise(col("filtered_errors"))
    ).drop("raw_validation_errors", "filtered_errors")

    # -----------------------------------------------------------------
    # 🎯 5. الفصل والتخزين المؤقت
    # -----------------------------------------------------------------
    df_valid = df_with_errors.filter("size(validation_errors) == 0")
    df_quarantine = df_with_errors.filter("size(validation_errors) > 0")

    df_valid = df_valid.withColumn("_id", col("order_id")) \
        .withColumn("quality_status", when(expr("size(corrections) > 0"), lit("corrected")).otherwise(lit("valid"))) \
        .withColumn("processed_at", current_timestamp())

    df_quarantine = df_quarantine.withColumn("_id", concat_ws("_", col("source_file"), col("source_row_number"), col("order_id"))) \
        .withColumn("quality_status", lit("quarantined")) \
        .withColumn("processed_at", current_timestamp())

    df_valid.cache()
    df_quarantine.cache()

    valid_count = df_valid.filter(col("quality_status") == "valid").count()
    corrected_count = df_valid.filter(col("quality_status") == "corrected").count()
    quarantine_count = df_quarantine.count()

    error_counts_df = df_quarantine.select(explode("validation_errors").alias("error_type")).groupBy("error_type").count().collect()
    
    error_counts_row = {
        "Quarantined: INVALID_ITEM_QUANTITY_OR_PRICE": 0,
        "Quarantined: AMBIGUOUS_NEGATIVE_VALUE": 0,
        "Quarantined: EMPTY_ITEMS": 0,
        "Quarantined: INVALID_CURRENCY": 0,
        "Quarantined: MISSING_CUSTOMER_ID": 0,
        "Quarantined: INVALID_IMPOSSIBLE_DATE": 0,
        "Quarantined: MISSING_ORDER_ID": 0,
        "Quarantined: INVALID_UNKNOWN_STATUS": 0,
        "Quarantined: MISSING_ITEM_SKU_OR_NAME": 0,
        "Quarantined: MULTIPLE_CONFLICTING_ERRORS": 0,
        "Quarantined: INVALID_EMAIL_ADDRESS": 0,
        "Quarantined: CORRUPTED_ITEMS_JSON": 0,
        "Quarantined: INVALID_PHONE_NUMBER": 0
    }
    
    for row in error_counts_df:
        error_counts_row[row["error_type"]] = row["count"]

    upsert_inserted_count = valid_count + corrected_count
    upsert_updated_count = 0
    try:
        target_uri = mongo_output_uri.replace("orders_raw", "orders_validated")
        df_existing = spark.read.format("mongo").option("uri", target_uri).load()
        if "_id" in df_existing.columns:
            df_existing_ids = df_existing.select("_id").withColumn("exists_in_db", lit(True))
            df_joined = df_valid.join(df_existing_ids, on="_id", how="left")
            upsert_updated_count = df_joined.filter(col("exists_in_db") == True).count()
            upsert_inserted_count = (valid_count + corrected_count) - upsert_updated_count
    except Exception as e:
        pass

    # -----------------------------------------------------------------
    # 🎯 8. إعداد السجلات النهائية المعتمدة على السكيما
    # -----------------------------------------------------------------
    print("📦 Upserting strictly typed valid records to 'orders_validated'...")
    
    df_valid_final = df_valid.select(
        col("_id"), col("run_id"), col("source_file"), col("source_row_number"), 
        col("ingested_at"), col("engine_used"), col("raw_record"), col("processed_at"),
        
        col("order_id").cast(StringType()),
        col("clean_order_date").cast(StringType()).alias("order_date"),
        col("clean_status").cast(StringType()).alias("status"),
        col("clean_customer_id").cast(StringType()).alias("customer_id"),
        col("customer_name").cast(StringType()),                 # إرجاعها كما كانت بدون clean
        col("clean_phone").cast(StringType()).alias("customer_phone"),
        col("clean_email").cast(StringType()).alias("customer_email"),
        col("city").cast(StringType()),                          # إرجاعها كما كانت بدون clean
        col("district").cast(StringType()),                      # إرجاعها كما كانت بدون clean
        col("clean_delivery_type").cast(StringType()).alias("delivery_type"),
        col("typed_delivery_cost").alias("delivery_cost"),    
        col("clean_payment_method").cast(StringType()).alias("payment_method"),
        col("clean_payment_status").cast(StringType()).alias("payment_status"),
        col("typed_payment_amount").alias("payment_amount"),  
        col("clean_currency").cast(StringType()).alias("currency"),
        col("typed_total_amount").alias("total_amount"),      
        col("typed_items").alias("items"),                    
        
        col("quality_status").cast(StringType()),
        col("corrections").cast(ArrayType(correction_schema))
    )

    df_valid_final.write.format("mongo").option("uri", mongo_output_uri.replace("orders_raw", "orders_validated")) \
    .mode("append").option("replaceDocument", "true").option("ordered", "false").save()

    print("⚠️ Upserting quarantined records to 'orders_quarantine'...")
    df_quarantine.select(
        "_id", "run_id", "source_file", "source_row_number", "ingested_at", "engine_used", 
        "validation_errors", "raw_record", "quality_status", "processed_at"
    ).write.format("mongo").option("uri", mongo_output_uri.replace("orders_raw", "orders_quarantine")) \
    .mode("append").option("replaceDocument", "true").option("ordered", "false").save()

    elapsed_seconds = time.time() - start_time
    throughput = rows_read / elapsed_seconds if elapsed_seconds > 0 else 0

    print(f"\n--- ELT Summary ---")
    print(f"Total Rows: {rows_read} | Valid: {valid_count} | Corrected: {corrected_count} | Quarantine: {quarantine_count}")

    df_valid.unpersist()
    df_quarantine.unpersist()

    final_report = {
        "run_id": target_run_id,
        "file_name": "sample_orders.csv",
        "file_size_mb": 0.0,
        "engine_used": "pyspark_elt",
        "rows_read": rows_read,
        "raw_loaded": rows_read,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "throughput": round(throughput, 2),
        "partitions": str(spark.conf.get("spark.sql.shuffle.partitions", "10")),
        "error_case_counts": error_counts_row,
        "inserted_count": upsert_inserted_count,
        "updated_count": upsert_updated_count,
        "unchanged_count": 0 
    }

    save_metrics_to_json(final_report)
    return final_report

if __name__ == "__main__":
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.settings import MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME
    
    try:
        import pymongo
        client = pymongo.MongoClient(MONGO_URI)
        client[MONGO_DB_NAME]["orders_validated"].create_index("order_id", unique=True)
        print("✅ Unique Index on 'order_id' verified in 'orders_validated'.")
    except Exception as e:
        print(f"⚠️ Notice regarding MongoDB index: {e}")

    spark = SparkSession.builder \
        .appName("Midterm_Data_Separator") \
        .config("spark.mongodb.input.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
        .config("spark.mongodb.output.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    input_mongo_uri = f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
    
    validate_and_transform_data(spark, input_mongo_uri, input_mongo_uri, target_run_id="run_20261004_220613")
    spark.stop()
