import time
import os
import sys
import json
from datetime import datetime
from pyspark.sql.functions import col, when, expr, sum as _sum, regexp_replace, abs, lit, struct, array, current_timestamp, trim, concat_ws
from pyspark.sql import SparkSession

# =====================================================================
# 1. دالة حفظ التقارير 
# =====================================================================
def save_metrics_to_json(report_data, output_path="reports/results.json"):
    """تقوم بحفظ أو إضافة التقرير إلى ملف JSON دون مسح القديم"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    all_reports = []
    # قراءة الملف القديم إن وجد
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    # إذا كان الملف يحتوي على بيانات، نحملها
                    if content.startswith('['):
                        all_reports = json.loads(content)
                    else:
                        # في حال كان التقرير القديم عبارة عن Object واحد وليس List
                        all_reports = [json.loads(content)]
        except Exception as e:
            print(f"⚠️ Could not read existing results.json: {e}")

    # إضافة التقرير الجديد للقائمة
    all_reports.append(report_data)

    # حفظ القائمة بأكملها (الكتابة فوق الملف القديم بالقائمة المحدثة)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_reports, f, ensure_ascii=False, indent=4)
    print(f"✅ Metrics successfully appended and saved to {output_path}")

# =====================================================================
# 2. دالة التنظيف والتحقق والعزل (ELT Validation Pipeline)
# =====================================================================
def validate_and_transform_data(spark, mongo_input_uri, mongo_output_uri, id_run="standalone_run"):
    print(f"\n--- Starting ELT Validation & Separation Phase ---")
    start_time = time.time()

    # قراءة البيانات
    df_raw = spark.read.format("mongo").option("uri", mongo_input_uri).load()
    if id_run != "standalone_run":
        df_raw = df_raw.filter(col("id_run") == id_run)

    rows_read = df_raw.count()
    partitions = df_raw.rdd.getNumPartitions()
   
    if rows_read == 0:
        print("⚠️ No records found to process.")
        return None

    # استخراج الحقول
    df_flattened = df_raw.select(
        col("id_run"), col("file_source"), col("number_row_source"),
        col("at_ingested"), col("engine_used"),
        col("record_raw.*")
    )

    # -----------------------------------------------------------------
    # 🎯 1. تنظيف المفتاح الأساسي وإزالة التكرارات من المنبع
    # -----------------------------------------------------------------
    df_flattened = df_flattened.withColumn("clean_order_id", regexp_replace(col("order_id"), r"[^\d]", ""))
    df_flattened = df_flattened.drop("order_id").withColumnRenamed("clean_order_id", "order_id")
    df_flattened = df_flattened.dropDuplicates(["order_id"])

    # -----------------------------------------------------------------
    # 🎯 2. مرحلة التصحيح (Correction Rules)
    # -----------------------------------------------------------------
    df_working = df_flattened \
        .withColumn("clean_phone", regexp_replace(col("customer_phone"), r"[^\d\+]", "")) \
        .withColumn("clean_email", regexp_replace(col("customer_email"), r"@{2,}", "@")) \
        .withColumn("clean_payment", when(col("payment_amount").cast("double") < 0, abs(col("payment_amount").cast("double")).cast("string")).otherwise(col("payment_amount"))) \
        .withColumn("clean_status", trim(col("status")))

    # تسجيل التصحيحات باستخدام الهيكل الدقيق المطلوب
    phone_audit = when(
        col("customer_phone") != col("clean_phone"),
        struct(
            lit("customer_phone").alias("field"),
            col("customer_phone").cast("string").alias("original_value"),
            col("clean_phone").cast("string").alias("corrected_value"),
            lit("PHONE_FORMAT_CLEANED").alias("rule_code")
        )
    ).otherwise(expr("NULL"))

    email_audit = when(
        col("customer_email") != col("clean_email"),
        struct(
            lit("customer_email").alias("field"),
            col("customer_email").cast("string").alias("original_value"),
            col("clean_email").cast("string").alias("corrected_value"),
            lit("EMAIL_REPEATED_SYMBOLS").alias("rule_code")
        )
    ).otherwise(expr("NULL"))

    payment_audit = when(
        col("payment_amount") != col("clean_payment"),
        struct(
            lit("payment_amount").alias("field"),
            col("payment_amount").cast("string").alias("original_value"),
            col("clean_payment").cast("string").alias("corrected_value"),
            lit("NEGATIVE_PAYMENT_FIXED").alias("rule_code")
        )
    ).otherwise(expr("NULL"))

    status_audit = when(
        col("status") != col("clean_status"),
        struct(
            lit("status").alias("field"),
            col("status").cast("string").alias("original_value"),
            col("clean_status").cast("string").alias("corrected_value"),
            lit("STATUS_SPACES_TRIMMED").alias("rule_code")
        )
    ).otherwise(expr("NULL"))

    # إضافة كل التصحيحات للمصفوفة
    df_audited = df_working.withColumn("raw_corrections", array(phone_audit, email_audit, payment_audit, status_audit)) \
        .withColumn("corrections", expr("filter(raw_corrections, x -> x is not null)")).drop("raw_corrections")

    # -----------------------------------------------------------------
    # 🎯 3. مرحلة التحقق من البيانات (Validation Phase)
    # -----------------------------------------------------------------
    df_checked = df_audited \
        .withColumn("check_order_id", col("order_id").isNotNull() & (col("order_id") != "")) \
        .withColumn("check_order_date", col("order_date").isNotNull() & (col("order_date") != "")) \
        .withColumn("check_status", col("clean_status").isin("مؤكد", "قيد الانتظار", "مرتجع", "قيد الشحن","ملغي", "تم التسليم")) \
        .withColumn("check_customer_id", col("customer_id").isNotNull() & (col("customer_id") != "")) \
        .withColumn("check_customer_phone", col("clean_phone").rlike(r"^\+?[0-9]{7,15}$")) \
        .withColumn("check_customer_email", col("clean_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")) \
        .withColumn("check_delivery_cost", col("delivery_cost").cast("double") >= 0) \
        .withColumn("check_payment_amount", col("clean_payment").cast("double") > 0) \
        .withColumn("check_total_amount", col("total_amount").cast("double") >= col("delivery_cost").cast("double"))

    # تجميع أسباب الخطأ في مصفوفة
    df_with_errors = df_checked.withColumn(
        "validation_errors",
        expr("""
            filter(
                array(
                    IF(not check_order_id, 'Invalid Order ID', null),
                    IF(not check_order_date, 'Invalid Order Date', null),
                    IF(not check_status, 'Invalid or Missing Status', null),
                    IF(not check_customer_id, 'Missing Customer ID', null),
                    IF(not check_customer_phone, 'Invalid Phone Format', null),
                    IF(not check_customer_email, 'Invalid Email Format', null),
                    IF(not check_delivery_cost, 'Negative or Invalid Delivery Cost', null),
                    IF(not check_payment_amount, 'Zero/Negative or Invalid Payment', null),
                    IF(not check_total_amount, 'Invalid Total or Less Than Delivery', null)
                ),
                x -> x is not null
            )
        """)
    )

    error_counts_row = df_checked.agg(
        _sum(when(col("check_order_id") == False, 1).otherwise(0)).alias("Invalid Order ID"),
        _sum(when(col("check_order_date") == False, 1).otherwise(0)).alias("Invalid Order Date"),
        _sum(when(col("check_status") == False, 1).otherwise(0)).alias("Invalid or Missing Status"),
        _sum(when(col("check_customer_id") == False, 1).otherwise(0)).alias("Missing Customer ID"),
        _sum(when(col("check_customer_phone") == False, 1).otherwise(0)).alias("Invalid Phone Format"),
        _sum(when(col("check_customer_email") == False, 1).otherwise(0)).alias("Invalid Email Format"),
        _sum(when(col("check_delivery_cost") == False, 1).otherwise(0)).alias("Negative or Invalid Delivery Cost"),
        _sum(when(col("check_payment_amount") == False, 1).otherwise(0)).alias("Zero/Negative or Invalid Payment"),
        _sum(when(col("check_total_amount") == False, 1).otherwise(0)).alias("Invalid Total or Less Than Delivery")
    ).collect()[0].asDict()

    # -----------------------------------------------------------------
    # 🎯 4. إعادة بناء السجل بالقيم النظيفة
    # -----------------------------------------------------------------
    df_rebuilt = df_with_errors.withColumn("record_raw", struct(
        col("order_id"), col("order_date"), col("clean_status").alias("status"), col("customer_id"), col("customer_name"),
        col("clean_phone").alias("customer_phone"), col("clean_email").alias("customer_email"),
        col("city"), col("district"), col("delivery_type"), col("delivery_cost"), col("payment_method"), col("payment_status"),
        col("clean_payment").alias("payment_amount"), col("currency"), col("total_amount"), col("items_json")
    ))

    # -----------------------------------------------------------------
    # 🎯 5. الفصل (التوزيع الثلاثي) وتهيئة الـ Upsert الشامل
    # -----------------------------------------------------------------
    df_valid = df_rebuilt.filter("size(validation_errors) == 0")
    df_quarantine = df_rebuilt.filter("size(validation_errors) > 0")

    # تحديد الـ _id للسجلات السليمة لمنع تكرار الطلبات (Idempotency)
    df_valid = df_valid.withColumn("_id", col("order_id")) \
                       .withColumn("quality_status", when(expr("size(corrections) > 0"), lit("corrected")).otherwise(lit("valid"))) \
                       .withColumn("processed_at", current_timestamp())

    # تحديد الـ _id لسجلات العزل باستخدام اسم الملف ورقم الصف لمنع تضاعف العزل
    df_quarantine = df_quarantine.withColumn("_id", concat_ws("_", col("file_source"), col("number_row_source"))) \
                                 .withColumn("quality_status", lit("quarantined")) \
                                 .withColumn("processed_at", current_timestamp())

    valid_count = df_valid.filter(col("quality_status") == "valid").count()
    corrected_count = df_valid.filter(col("quality_status") == "corrected").count()
    quarantine_count = df_quarantine.count()

    # -----------------------------------------------------------------
    # 🎯 6. الحفظ في قواعد البيانات باستخدام Upsert
    # -----------------------------------------------------------------
    print("📦 Upserting valid records to 'validated_orders' (Using native _id)...")
    df_valid.select("_id", "id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "record_raw", "quality_status", "corrections", "processed_at") \
        .write.format("mongo").option("uri", mongo_output_uri.replace("order_raw", "validated_orders")) \
        .mode("append").option("replaceDocument", "true").option("ordered", "false").save()

    print("⚠️ Upserting quarantined records to 'quarantine_orders' (Using source-based _id)...")
    df_quarantine.select("_id", "id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "validation_errors", "record_raw", "quality_status", "processed_at") \
        .write.format("mongo").option("uri", mongo_output_uri.replace("order_raw", "quarantine_orders")) \
        .mode("append").option("replaceDocument", "true").option("ordered", "false").save()

    elapsed_seconds = time.time() - start_time
    throughput = rows_read / elapsed_seconds if elapsed_seconds > 0 else 0

    print(f"\n--- ELT Summary ---")
    print(f"Total Rows: {rows_read} | Valid: {valid_count} | Corrected: {corrected_count} | Quarantine: {quarantine_count}")
   
    # -----------------------------------------------------------------
    # 🎯 7. بناء التقرير الشامل والحفظ
    # -----------------------------------------------------------------
    final_report = {
        "_id": id_run,
        "file_name": "Loaded_from_MongoDB",
        "file_size_mb": "N/A_in_ELT_Phase",
        "engine_used": "PySpark_ELT",
        "rows_read": rows_read,
        "raw_loaded": rows_read,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "throughput": round(throughput, 2),
        "batch_size_partitions": partitions,
        "error_case_counts": error_counts_row,
        "upsert_inserted_count": 0,
        "upsert_updated_count": 0,
        "upsert_unchanged_count": 0
    }

    save_metrics_to_json(final_report)
   
    return final_report

# =====================================================================
# 3. التشغيل المستقل (Main Execution)
# =====================================================================
if __name__ == "__main__":
    import os
    import sys
    from datetime import datetime
   
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
   
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.settings import MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME
   
    print("🚀 Starting Standalone ELT Pipeline with JSON Reporting...")
   
    spark = SparkSession.builder \
        .appName("Midterm_Data_Separator") \
        .config("spark.mongodb.input.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
        .config("spark.mongodb.output.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.validated_orders") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .getOrCreate()
       
    spark.sparkContext.setLogLevel("ERROR")
    input_mongo_uri = f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
   
    validate_and_transform_data(spark, input_mongo_uri, input_mongo_uri, id_run="standalone_run")
   
    spark.stop()
    print("✅ Process Completed!")
#
# 
#  صحيح كامل ولكن خطا في شكل التصحيحات مع زيادة العزل 
# 
# import time
# import os
# import sys
# import json
# from datetime import datetime
# # إضافة trim هنا
# from pyspark.sql.functions import col, when, expr, sum as _sum, regexp_replace, abs, lit, struct, array, current_timestamp, trim
# from pyspark.sql import SparkSession

# # =====================================================================
# # 1. دالة حفظ التقارير (مدمجة داخل الملف)
# # =====================================================================
# def save_metrics_to_json(report_data, output_path="reports/results.json"):
#     """تقوم بحفظ أو إضافة التقرير إلى ملف JSON دون مسح القديم"""
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
#     all_reports = []
#     # قراءة الملف القديم إن وجد
#     if os.path.exists(output_path):
#         try:
#             with open(output_path, 'r', encoding='utf-8') as f:
#                 content = f.read().strip()
#                 if content:
#                     if content.startswith('['):
#                         all_reports = json.loads(content)
#                     else:
#                         all_reports = [json.loads(content)]
#         except Exception as e:
#             print(f"⚠️ Could not read existing results.json: {e}")

#     # إضافة التقرير الجديد للقائمة
#     all_reports.append(report_data)

#     # حفظ القائمة بأكملها
#     with open(output_path, 'w', encoding='utf-8') as f:
#         json.dump(all_reports, f, ensure_ascii=False, indent=4)
#     print(f"✅ Metrics successfully appended and saved to {output_path}")

# # =====================================================================
# # 2. دالة التنظيف والتحقق والعزل (ELT Validation Pipeline)
# # =====================================================================
# def validate_and_transform_data(spark, mongo_input_uri, mongo_output_uri, id_run="standalone_run"):
#     print(f"\n--- Starting ELT Validation & Separation Phase ---")
#     start_time = time.time()

#     # قراءة البيانات
#     df_raw = spark.read.format("mongo").option("uri", mongo_input_uri).load()
#     if id_run != "standalone_run":
#         df_raw = df_raw.filter(col("id_run") == id_run)

#     rows_read = df_raw.count()
#     partitions = df_raw.rdd.getNumPartitions()
   
#     if rows_read == 0:
#         print("⚠️ No records found to process.")
#         return None

#     # استخراج الحقول
#     df_flattened = df_raw.select(
#         col("id_run"), col("file_source"), col("number_row_source"),
#         col("at_ingested"), col("engine_used"),
#         col("record_raw.*")
#     )

#     # -----------------------------------------------------------------
#     # 🎯 1. تنظيف المفتاح الأساسي وإزالة التكرارات من المنبع
#     # -----------------------------------------------------------------
#     df_flattened = df_flattened.withColumn("clean_order_id", regexp_replace(col("order_id"), r"[^\d]", ""))
#     df_flattened = df_flattened.drop("order_id").withColumnRenamed("clean_order_id", "order_id")
#     df_flattened = df_flattened.dropDuplicates(["order_id"])

#     # -----------------------------------------------------------------
#     # 🎯 2. مرحلة التصحيح (Correction Rules)
#     # -----------------------------------------------------------------
#     df_working = df_flattened \
#         .withColumn("clean_phone", regexp_replace(col("customer_phone"), r"[^\d\+]", "")) \
#         .withColumn("clean_email", regexp_replace(col("customer_email"), r"@{2,}", "@")) \
#         .withColumn("clean_payment", when(col("payment_amount").cast("double") < 0, abs(col("payment_amount").cast("double")).cast("string")).otherwise(col("payment_amount"))) \
#         .withColumn("clean_status", trim(col("status"))) # 🎯 تنظيف حالة الطلب من المسافات

#     # تسجيل ما تم تصحيحه (Audit Trail)
#     phone_audit = when(col("customer_phone") != col("clean_phone"), struct(lit("customer_phone").alias("field"), lit("PHONE_FORMAT_CLEANED").alias("rule_code"))).otherwise(expr("NULL"))
#     email_audit = when(col("customer_email") != col("clean_email"), struct(lit("customer_email").alias("field"), lit("EMAIL_FIXED").alias("rule_code"))).otherwise(expr("NULL"))
#     payment_audit = when(col("payment_amount") != col("clean_payment"), struct(lit("payment_amount").alias("field"), lit("NEGATIVE_PAYMENT_FIXED").alias("rule_code"))).otherwise(expr("NULL"))
#     status_audit = when(col("status") != col("clean_status"), struct(lit("status").alias("field"), lit("STATUS_SPACES_TRIMMED").alias("rule_code"))).otherwise(expr("NULL"))

#     # إضافة كل التصحيحات للمصفوفة
#     df_audited = df_working.withColumn("raw_corrections", array(phone_audit, email_audit, payment_audit, status_audit)) \
#         .withColumn("corrections", expr("filter(raw_corrections, x -> x is not null)")).drop("raw_corrections")

#     # -----------------------------------------------------------------
#     # 🎯 3. مرحلة التحقق من البيانات (Validation Phase)
#     # -----------------------------------------------------------------
#     # 🎯 تم استبدال col("status") بـ col("clean_status") لكي يمر الفحص بنجاح
#     df_checked = df_audited \
#         .withColumn("check_order_id", col("order_id").isNotNull() & (col("order_id") != "")) \
#         .withColumn("check_order_date", col("order_date").isNotNull() & (col("order_date") != "")) \
#         .withColumn("check_status", col("clean_status").isin("مؤكد", "قيد الانتظار", "مرتجع", "قيد الشحن","ملغي", "تم التسليم")) \
#         .withColumn("check_customer_id", col("customer_id").isNotNull() & (col("customer_id") != "")) \
#         .withColumn("check_customer_phone", col("clean_phone").rlike(r"^\+?[0-9]{7,15}$")) \
#         .withColumn("check_customer_email", col("clean_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")) \
#         .withColumn("check_delivery_cost", col("delivery_cost").cast("double") >= 0) \
#         .withColumn("check_payment_amount", col("clean_payment").cast("double") > 0) \
#         .withColumn("check_total_amount", col("total_amount").cast("double") >= col("delivery_cost").cast("double"))

#     # تجميع أسباب الخطأ في مصفوفة
#     df_with_errors = df_checked.withColumn(
#         "validation_errors",
#         expr("""
#             filter(
#                 array(
#                     IF(not check_order_id, 'Invalid Order ID', null),
#                     IF(not check_order_date, 'Invalid Order Date', null),
#                     IF(not check_status, 'Invalid or Missing Status', null),
#                     IF(not check_customer_id, 'Missing Customer ID', null),
#                     IF(not check_customer_phone, 'Invalid Phone Format', null),
#                     IF(not check_customer_email, 'Invalid Email Format', null),
#                     IF(not check_delivery_cost, 'Negative or Invalid Delivery Cost', null),
#                     IF(not check_payment_amount, 'Zero/Negative or Invalid Payment', null),
#                     IF(not check_total_amount, 'Invalid Total or Less Than Delivery', null)
#                 ),
#                 x -> x is not null
#             )
#         """)
#     )

#     error_counts_row = df_checked.agg(
#         _sum(when(col("check_order_id") == False, 1).otherwise(0)).alias("Invalid Order ID"),
#         _sum(when(col("check_order_date") == False, 1).otherwise(0)).alias("Invalid Order Date"),
#         _sum(when(col("check_status") == False, 1).otherwise(0)).alias("Invalid or Missing Status"),
#         _sum(when(col("check_customer_id") == False, 1).otherwise(0)).alias("Missing Customer ID"),
#         _sum(when(col("check_customer_phone") == False, 1).otherwise(0)).alias("Invalid Phone Format"),
#         _sum(when(col("check_customer_email") == False, 1).otherwise(0)).alias("Invalid Email Format"),
#         _sum(when(col("check_delivery_cost") == False, 1).otherwise(0)).alias("Negative or Invalid Delivery Cost"),
#         _sum(when(col("check_payment_amount") == False, 1).otherwise(0)).alias("Zero/Negative or Invalid Payment"),
#         _sum(when(col("check_total_amount") == False, 1).otherwise(0)).alias("Invalid Total or Less Than Delivery")
#     ).collect()[0].asDict()

#     # -----------------------------------------------------------------
#     # 🎯 4. إعادة بناء السجل بالقيم النظيفة
#     # -----------------------------------------------------------------
#     # 🎯 تم استخدام col("clean_status").alias("status") لكي تُحفظ الحالة خالية من المسافات
#     df_rebuilt = df_with_errors.withColumn("record_raw", struct(
#         col("order_id"), col("order_date"), col("clean_status").alias("status"), col("customer_id"), col("customer_name"),
#         col("clean_phone").alias("customer_phone"), col("clean_email").alias("customer_email"),
#         col("city"), col("district"), col("delivery_type"), col("delivery_cost"), col("payment_method"), col("payment_status"),
#         col("clean_payment").alias("payment_amount"), col("currency"), col("total_amount"), col("items_json")
#     ))

#     # -----------------------------------------------------------------
#     # 🎯 5. الفصل (التوزيع الثلاثي) وتهيئة الـ Upsert
#     # -----------------------------------------------------------------
#     df_valid = df_rebuilt.filter("size(validation_errors) == 0")
#     df_quarantine = df_rebuilt.filter("size(validation_errors) > 0")

#     df_valid = df_valid.withColumn("_id", col("order_id")) \
#                        .withColumn("quality_status", when(expr("size(corrections) > 0"), lit("corrected")).otherwise(lit("valid"))) \
#                        .withColumn("processed_at", current_timestamp())

#     df_quarantine = df_quarantine.withColumn("quality_status", lit("quarantined")) \
#                                  .withColumn("processed_at", current_timestamp())

#     valid_count = df_valid.filter(col("quality_status") == "valid").count()
#     corrected_count = df_valid.filter(col("quality_status") == "corrected").count()
#     quarantine_count = df_quarantine.count()

#     # -----------------------------------------------------------------
#     # 🎯 6. الحفظ في قواعد البيانات
#     # -----------------------------------------------------------------
#     print("📦 Upserting valid records to 'validated_orders' (Using native _id)...")
#     df_valid.select("_id", "id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "record_raw", "quality_status", "corrections", "processed_at") \
#         .write.format("mongo").option("uri", mongo_output_uri.replace("order_raw", "validated_orders")) \
#         .mode("append").option("replaceDocument", "true").option("ordered", "false").save()

#     print("⚠️ Saving quarantined records to 'quarantine_orders'...")
#     df_quarantine.select("id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "validation_errors", "record_raw", "quality_status", "processed_at") \
#         .write.format("mongo").option("uri", mongo_output_uri.replace("order_raw", "quarantine_orders")) \
#         .mode("append").option("ordered", "false").save()

#     elapsed_seconds = time.time() - start_time
#     throughput = rows_read / elapsed_seconds if elapsed_seconds > 0 else 0

#     print(f"\n--- ELT Summary ---")
#     print(f"Total Rows: {rows_read} | Valid: {valid_count} | Corrected: {corrected_count} | Quarantine: {quarantine_count}")
   
#     # -----------------------------------------------------------------
#     # 🎯 7. بناء التقرير الشامل والحفظ
#     # -----------------------------------------------------------------
#     final_report = {
#         "_id": id_run,
#         "file_name": "Loaded_from_MongoDB",
#         "file_size_mb": "N/A_in_ELT_Phase",
#         "engine_used": "PySpark_ELT",
#         "rows_read": rows_read,
#         "raw_loaded": rows_read,
#         "valid_count": valid_count,
#         "corrected_count": corrected_count,
#         "quarantine_count": quarantine_count,
#         "elapsed_seconds": round(elapsed_seconds, 2),
#         "throughput": round(throughput, 2),
#         "batch_size_partitions": partitions,
#         "error_case_counts": error_counts_row,
#         "upsert_inserted_count": 0,
#         "upsert_updated_count": 0,
#         "upsert_unchanged_count": 0
#     }

#     save_metrics_to_json(final_report)
   
#     return final_report

# # =====================================================================
# # 3. التشغيل المستقل (Main Execution)
# # =====================================================================
# if __name__ == "__main__":
#     import os
#     import sys
#     from datetime import datetime
   
#     os.environ['HADOOP_HOME'] = 'C:\\hadoop'
   
#     sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#     from config.settings import MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME
   
#     print("🚀 Starting Standalone ELT Pipeline with JSON Reporting...")
   
#     spark = SparkSession.builder \
#         .appName("Midterm_Data_Separator") \
#         .config("spark.mongodb.input.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
#         .config("spark.mongodb.output.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.validated_orders") \
#         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#         .config("spark.driver.memory", "12g") \
#         .config("spark.executor.memory", "12g") \
#         .getOrCreate()
       
#     spark.sparkContext.setLogLevel("ERROR")
#     input_mongo_uri = f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
   
#     validate_and_transform_data(spark, input_mongo_uri, input_mongo_uri, id_run="standalone_run")
   
#     spark.stop()
#     print("✅ Process Completed!")

# 
# ///////  قبل الاخير
# 
# import time
# import os
# import sys
# import json
# from datetime import datetime
# from pymongo import MongoClient
# from pyspark.sql.functions import col, when, expr, sum as _sum, lit, struct, regexp_replace, abs, array, current_timestamp, trim
# from pyspark.sql import SparkSession

# # =====================================================================
# # 1. دالة حفظ التقارير 
# # =====================================================================
# def save_metrics_to_json(report_data, output_path="reports/results.json"):
#     """تقوم بحفظ قاموس المقاييس الشامل في ملف JSON"""
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
#     all_reports = []
#     if os.path.exists(output_path):
#         try:
#             with open(output_path, 'r', encoding='utf-8') as f:
#                 content = f.read().strip()
#                 if content:
#                     if content.startswith('['):
#                         all_reports = json.loads(content)
#                     else:
#                         all_reports = [json.loads(content)]
#         except Exception as e:
#             print(f"⚠️ Could not read existing results.json: {e}")

#     all_reports.append(report_data)

#     with open(output_path, 'w', encoding='utf-8') as f:
#         json.dump(all_reports, f, ensure_ascii=False, indent=4)
#     print(f"✅ Metrics successfully saved to {output_path}")

# # =====================================================================
# # 2. دالة التنظيف، التحقق، والعزل في تمريرة واحدة (Single-Pass ELT)
# # =====================================================================
# def validate_and_transform_data(spark, mongo_input_uri, mongo_output_uri, id_run="standalone_run"):
#     print(f"\n--- 🚀 Starting Single-Pass ELT Validation & Correction Phase ---")
#     start_time = time.time()

#     # -----------------------------------------------------------------
#     # [شرط الدكتور]: إنشاء Unique Index على order_id في مجموعة validated_orders
#     # -----------------------------------------------------------------
#     from config.settings import MONGO_URI, MONGO_DB_NAME
#     client = MongoClient(MONGO_URI)
#     db = client[MONGO_DB_NAME]
#     validated_collection_name = "validated_orders"
#     print("🛠️ Enforcing Unique Index on 'record_raw.order_id' for Validated Orders...")
#     # تجاهل الأخطاء إذا كان الفهرس موجوداً مسبقاً
#     try:
#         db[validated_collection_name].create_index("record_raw.order_id", unique=True)
#     except Exception as e:
#         print(f"⚠️ Index might already exist or need manual rebuild: {e}")
#     client.close()

#     # 1. قراءة البيانات الخام
#     df_raw = spark.read.format("mongo").option("uri", mongo_input_uri).load()
#     if id_run != "standalone_run":
#         df_raw = df_raw.filter(col("id_run") == id_run)

#     rows_read = df_raw.count()
#     partitions = df_raw.rdd.getNumPartitions()
    
#     if rows_read == 0:
#         print("⚠️ No records found to process.")
#         return None

#     # -----------------------------------------------------------------
#     # 🌟 [الحل الجذري المطور]: تنظيف وإزالة التكرار من المنبع
#     # -----------------------------------------------------------------
#     # 1. فك الهيكل (Flatten) واستخراج الحقول
#     df_flattened = df_raw.select(
#         col("id_run"), col("file_source"), col("number_row_source"), 
#         col("at_ingested"), col("engine_used"),
#         col("record_raw.*")
#     )

#     # 2. تنظيف order_id فوراً بقوة (إزالة كل المسافات والأحرف غير المرئية)
#     # نستخدم regexp_replace لإزالة أي مسافة (سواء عادية أو tab أو سطر جديد) من البداية والنهاية
#     df_flattened = df_flattened.withColumn(
#         "clean_order_id", 
#         regexp_replace(col("order_id"), r"(^\s+|\s+$|[\u200B-\u200D\uFEFF])", "")
#     )
    
#     # استبدال العمود الأصلي بالعمود النظيف جداً
#     df_flattened = df_flattened.drop("order_id").withColumnRenamed("clean_order_id", "order_id")

#     # 3. إزالة التكرارات بشكل صارم بناءً على order_id النظيف
#     df_flattened = df_flattened.dropDuplicates(["order_id"])

#     print("🧹 Data cleaned and deduplicated at source...")

#     # -----------------------------------------------------------------
#     # 2. مرحلة التنظيف المبكر وبناء سجل التدقيق (Audit Trail)
#     # -----------------------------------------------------------------
#     df_working = df_flattened \
#         .withColumn("clean_phone", regexp_replace(col("customer_phone"), r"[^\d\+]", "")) \
#         .withColumn("clean_email", regexp_replace(col("customer_email"), r"@{2,}", "@")) \
#         .withColumn("clean_payment", when(col("payment_amount").cast("double") < 0, abs(col("payment_amount").cast("double")).cast("string")).otherwise(col("payment_amount")))

#     phone_audit = when(col("customer_phone") != col("clean_phone"),
#         struct(lit("customer_phone").alias("field"), col("customer_phone").cast("string").alias("original_value"),
#                col("clean_phone").cast("string").alias("corrected_value"), lit("PHONE_FORMAT_CLEANED").alias("rule_code"))
#     ).otherwise(expr("NULL"))

#     email_audit = when(col("customer_email") != col("clean_email"),
#         struct(lit("customer_email").alias("field"), col("customer_email").cast("string").alias("original_value"),
#                col("clean_email").cast("string").alias("corrected_value"), lit("EMAIL_REPEATED_SYMBOLS").alias("rule_code"))
#     ).otherwise(expr("NULL"))

#     payment_audit = when(col("payment_amount") != col("clean_payment"),
#         struct(lit("payment_amount").alias("field"), col("payment_amount").cast("string").alias("original_value"),
#                col("clean_payment").cast("string").alias("corrected_value"), lit("NEGATIVE_PAYMENT_FIXED").alias("rule_code"))
#     ).otherwise(expr("NULL"))

#     df_audited = df_working.withColumn("raw_corrections", array(phone_audit, email_audit, payment_audit)) \
#         .withColumn("corrections", expr("filter(raw_corrections, x -> x is not null)")).drop("raw_corrections")

#     # -----------------------------------------------------------------
#     # 3. مرحلة التحقق من البيانات (بعد التنظيف)
#     # -----------------------------------------------------------------
#     df_checked = df_audited \
#         .withColumn("check_order_id", col("order_id").isNotNull() & (col("order_id") != "")) \
#         .withColumn("check_order_date", col("order_date").isNotNull() & (col("order_date") != "")) \
#         .withColumn("check_status", col("status").isin("مؤكد","ملغي", "قيد الانتظار", "مرتجع", "قيد الشحن", "تم التسليم")) \
#         .withColumn("check_customer_id", col("customer_id").isNotNull() & (col("customer_id") != "")) \
#         .withColumn("check_customer_phone", col("clean_phone").rlike(r"^\+?[0-9]{7,15}$")) \
#         .withColumn("check_customer_email", col("clean_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")) \
#         .withColumn("check_delivery_cost", col("delivery_cost").cast("double") >= 0) \
#         .withColumn("check_payment_amount", col("clean_payment").cast("double") > 0) \
#         .withColumn("check_total_amount", col("total_amount").cast("double") >= col("delivery_cost").cast("double"))

#     df_with_errors = df_checked.withColumn(
#         "validation_errors",
#         expr("""
#             filter(
#                 array(
#                     IF(not check_order_id, 'Invalid Order ID', null),
#                     IF(not check_order_date, 'Invalid Order Date', null),
#                     IF(not check_status, 'Invalid or Missing Status', null),
#                     IF(not check_customer_id, 'Missing Customer ID', null),
#                     IF(not check_customer_phone, 'Invalid Phone Format', null),
#                     IF(not check_customer_email, 'Invalid Email Format', null),
#                     IF(not check_delivery_cost, 'Negative or Invalid Delivery Cost', null),
#                     IF(not check_payment_amount, 'Zero/Negative or Invalid Payment', null),
#                     IF(not check_total_amount, 'Invalid Total or Less Than Delivery', null)
#                 ),
#                 x -> x is not null
#             )
#         """)
#     )

#     # حساب الأخطاء للتقرير
#     error_counts_row = df_checked.agg(
#         _sum(when(col("check_order_id") == False, 1).otherwise(0)).alias("Invalid Order ID"),
#         _sum(when(col("check_order_date") == False, 1).otherwise(0)).alias("Invalid Order Date"),
#         _sum(when(col("check_status") == False, 1).otherwise(0)).alias("Invalid or Missing Status"),
#         _sum(when(col("check_customer_id") == False, 1).otherwise(0)).alias("Missing Customer ID"),
#         _sum(when(col("check_customer_phone") == False, 1).otherwise(0)).alias("Invalid Phone Format"),
#         _sum(when(col("check_customer_email") == False, 1).otherwise(0)).alias("Invalid Email Format"),
#         _sum(when(col("check_delivery_cost") == False, 1).otherwise(0)).alias("Negative or Invalid Delivery Cost"),
#         _sum(when(col("check_payment_amount") == False, 1).otherwise(0)).alias("Zero/Negative or Invalid Payment"),
#         _sum(when(col("check_total_amount") == False, 1).otherwise(0)).alias("Invalid Total or Less Than Delivery")
#     ).collect()[0].asDict()

#     # 🎯 [تصحيح هيكلي]: هنا تأكدنا أننا نضع order_id النظيف جداً داخل السجل
#     df_rebuilt = df_with_errors.withColumn("record_raw", struct(
#         col("order_id"), col("order_date"), col("status"), col("customer_id"), col("customer_name"),
#         col("clean_phone").alias("customer_phone"), col("clean_email").alias("customer_email"),
#         col("city"), col("district"), col("delivery_type"), col("delivery_cost"), col("payment_method"), col("payment_status"),
#         col("clean_payment").alias("payment_amount"), col("currency"), col("total_amount"), col("items_json")
#     ))

#     # -----------------------------------------------------------------
#     # 4. التوزيع الثلاثي (Splitting) وإضافة تاريخ المعالجة
#     # -----------------------------------------------------------------
#     df_success_all = df_rebuilt.filter("size(validation_errors) == 0")
    
#     df_success_all = df_success_all.withColumn("quality_status", when(expr("size(corrections) > 0"), lit("corrected")).otherwise(lit("valid")))
#     df_success_all = df_success_all.withColumn("processed_at", current_timestamp())
    
#     valid_count = df_success_all.filter(col("quality_status") == "valid").count()
#     corrected_count = df_success_all.filter(col("quality_status") == "corrected").count()

#     df_quarantine = df_rebuilt.filter("size(validation_errors) > 0").withColumn("quality_status", lit("quarantined"))
#     df_quarantine = df_quarantine.withColumn("processed_at", current_timestamp())
#     quarantine_count = df_quarantine.count()

#     # -----------------------------------------------------------------
#     # 5. الحفظ في MongoDB (استراتيجية Upsert الصارمة)
#     # -----------------------------------------------------------------
#     print(f"📦 Upserting Valid & Corrected records to 'validated_orders'...")
    
#     # تأكد من استخدام هذه الخيارات بالضبط:
#     df_success_all.select("id_run", "file_source", "number_row_source", "at_ingested", "processed_at", "engine_used", "record_raw", "quality_status", "corrections") \
#         .write.format("mongo") \
#         .option("uri", mongo_output_uri.replace("order_raw", "validated_orders")) \
#         .mode("append") \
#         .option("replaceDocument", "true") \
#         .option("upsert", "true") \
#         .option("idFieldList", "record_raw.order_id") \
#         .option("ordered", "false") \
#         .save()

#     print("⚠️ Appending Quarantined records to 'quarantine_orders'...")
#     df_quarantine.select("id_run", "file_source", "number_row_source", "at_ingested", "processed_at", "engine_used", "validation_errors", "record_raw") \
#         .write.format("mongo") \
#         .option("uri", mongo_output_uri.replace("order_raw", "quarantine_orders")) \
#         .mode("append") \
#         .save()

#     elapsed_seconds = time.time() - start_time
#     throughput = rows_read / elapsed_seconds if elapsed_seconds > 0 else 0

#     print(f"\n--- ELT Summary ---")
#     print(f"Total Rows Evaluated: {rows_read} | Valid: {valid_count} | Corrected: {corrected_count} | Quarantine: {quarantine_count}")
    
#     # -----------------------------------------------------------------
#     # 6. بناء التقرير الشامل
#     # -----------------------------------------------------------------
#     final_report = {
#         "_id": id_run,
#         "file_name": "Loaded_from_MongoDB",
#         "file_size_mb": "N/A_in_ELT_Phase",
#         "engine_used": "PySpark_SinglePass_ELT",
#         "rows_read": rows_read,
#         "raw_loaded": rows_read,
#         "valid_count": valid_count,
#         "corrected_count": corrected_count,
#         "quarantine_count": quarantine_count,
#         "elapsed_seconds": round(elapsed_seconds, 2),
#         "throughput": round(throughput, 2),
#         "batch_size_partitions": partitions,
#         "error_case_counts": error_counts_row,
#         "upsert_inserted_count": 0, 
#         "upsert_updated_count": 0,
#         "upsert_unchanged_count": 0
#     }

#     save_metrics_to_json(final_report)
#     return final_report

# # =====================================================================
# # 3. التشغيل المستقل (Main Execution)
# # =====================================================================
# if __name__ == "__main__":
#     import os
#     import sys
#     from datetime import datetime
    
#     os.environ['HADOOP_HOME'] = 'C:\\hadoop'
#     sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#     from config.settings import MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME
    
#     print("🚀 Starting Standalone Single-Pass ELT Pipeline...")
    
#     spark = SparkSession.builder \
#         .appName("Midterm_Data_ELT_Processor") \
#         .config("spark.mongodb.input.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
#         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#         .config("spark.driver.memory", "12g") \
#         .config("spark.executor.memory", "12g") \
#         .getOrCreate()
        
#     spark.sparkContext.setLogLevel("ERROR")
#     input_mongo_uri = f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
#     output_mongo_uri = input_mongo_uri
    
#     validate_and_transform_data(spark, input_mongo_uri, output_mongo_uri, id_run="standalone_run")
    
#     spark.stop()
#     print("✅ Process Completed!")
# 
# 
# \\\\\\\\\\\\\\\\\\\\\ القديم 

# import time
# import os
# import sys
# import json
# from datetime import datetime
# from pyspark.sql.functions import col, when, expr, sum as _sum
# from pyspark.sql import SparkSession

# # =====================================================================
# # 1. دالة حفظ التقارير (مدمجة داخل الملف كما طلبت)
# # =====================================================================

# def save_metrics_to_json(report_data, output_path="reports/results.json"):
#     """تقوم بحفظ أو إضافة التقرير إلى ملف JSON دون مسح القديم"""
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
#     all_reports = []
#     # قراءة الملف القديم إن وجد
#     if os.path.exists(output_path):
#         try:
#             with open(output_path, 'r', encoding='utf-8') as f:
#                 content = f.read().strip()
#                 if content:
#                     # إذا كان الملف يحتوي على بيانات، نحملها
#                     if content.startswith('['):
#                         all_reports = json.loads(content)
#                     else:
#                         # في حال كان التقرير القديم عبارة عن Object واحد وليس List
#                         all_reports = [json.loads(content)]
#         except Exception as e:
#             print(f"⚠️ Could not read existing results.json: {e}")

#     # إضافة التقرير الجديد للقائمة
#     all_reports.append(report_data)

#     # حفظ القائمة بأكملها (الكتابة فوق الملف القديم بالقائمة المحدثة)
#     with open(output_path, 'w', encoding='utf-8') as f:
#         json.dump(all_reports, f, ensure_ascii=False, indent=4)
#     print(f"✅ Metrics successfully appended and saved to {output_path}")
# # =====================================================================
# # 2. دالة التنظيف والتحقق والعزل (ELT Validation Pipeline)
# # =====================================================================
# def validate_and_transform_data(spark, mongo_input_uri, mongo_output_uri, id_run="standalone_run"):
#     print(f"\n--- Starting ELT Validation & Separation Phase ---")
#     start_time = time.time()

#     # قراءة البيانات
#     df_raw = spark.read.format("mongo").option("uri", mongo_input_uri).load()
#     if id_run != "standalone_run":
#         df_raw = df_raw.filter(col("id_run") == id_run)

#     rows_read = df_raw.count()
#     partitions = df_raw.rdd.getNumPartitions()
   
#     if rows_read == 0:
#         print("⚠️ No records found to process.")
#         return None

#     # استخراج الحقول
#     df_flattened = df_raw.select(
#         col("id_run"), col("file_source"), col("number_row_source"),
#         col("at_ingested"), col("engine_used"),
#         col("record_raw.*"), col("record_raw")
#     )

#     # 🛠️ الشروط الـ 9 مخصصة للبيانات العربية والمشاكل التي ظهرت في العينة
#     # ملاحظة: الأرقام التي تحتوي فواصل أو أرقام عربية ستفشل في cast("double") وستذهب تلقائياً للعزل لتنظيفها لاحقاً.
#     df_checked = df_flattened \
#         .withColumn("check_order_id", col("order_id").isNotNull() & (col("order_id") != "")) \
#         .withColumn("check_order_date", col("order_date").isNotNull() & (col("order_date") != "")) \
#         .withColumn("check_status", col("status").isin("مؤكد", "قيد الانتظار", "مرتجع", "قيد الشحن","ملغي", "تم التسليم")) \
#         .withColumn("check_customer_id", col("customer_id").isNotNull() & (col("customer_id") != "")) \
#         .withColumn("check_customer_phone", col("customer_phone").rlike(r"^\+?[0-9]{7,15}$")) \
#         .withColumn("check_customer_email", col("customer_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")) \
#         .withColumn("check_delivery_cost", col("delivery_cost").cast("double") >= 0) \
#         .withColumn("check_payment_amount", col("payment_amount").cast("double") > 0) \
#         .withColumn("check_total_amount", col("total_amount").cast("double") >= col("delivery_cost").cast("double"))

#     # تجميع أسباب الخطأ في مصفوفة
#     df_with_errors = df_checked.withColumn(
#         "validation_errors",
#         expr("""
#             filter(
#                 array(
#                     IF(not check_order_id, 'Invalid Order ID', null),
#                     IF(not check_order_date, 'Invalid Order Date', null),
#                     IF(not check_status, 'Invalid or Missing Status', null),
#                     IF(not check_customer_id, 'Missing Customer ID', null),
#                     IF(not check_customer_phone, 'Invalid Phone Format', null),
#                     IF(not check_customer_email, 'Invalid Email Format', null),
#                     IF(not check_delivery_cost, 'Negative or Invalid Delivery Cost', null),
#                     IF(not check_payment_amount, 'Zero/Negative or Invalid Payment', null),
#                     IF(not check_total_amount, 'Invalid Total or Less Than Delivery', null)
#                 ),
#                 x -> x is not null
#             )
#         """)
#     )

#     # حساب إحصائيات كل خطأ
#     error_counts_row = df_checked.agg(
#         _sum(when(col("check_order_id") == False, 1).otherwise(0)).alias("Invalid Order ID"),
#         _sum(when(col("check_order_date") == False, 1).otherwise(0)).alias("Invalid Order Date"),
#         _sum(when(col("check_status") == False, 1).otherwise(0)).alias("Invalid or Missing Status"),
#         _sum(when(col("check_customer_id") == False, 1).otherwise(0)).alias("Missing Customer ID"),
#         _sum(when(col("check_customer_phone") == False, 1).otherwise(0)).alias("Invalid Phone Format"),
#         _sum(when(col("check_customer_email") == False, 1).otherwise(0)).alias("Invalid Email Format"),
#         _sum(when(col("check_delivery_cost") == False, 1).otherwise(0)).alias("Negative or Invalid Delivery Cost"),
#         _sum(when(col("check_payment_amount") == False, 1).otherwise(0)).alias("Zero/Negative or Invalid Payment"),
#         _sum(when(col("check_total_amount") == False, 1).otherwise(0)).alias("Invalid Total or Less Than Delivery")
#     ).collect()[0].asDict()

#     # الفصل
#     df_valid = df_with_errors.filter("size(validation_errors) == 0")
#     df_quarantine = df_with_errors.filter("size(validation_errors) > 0")

#     # الحفظ في قواعد البيانات
#     print("📦 Saving valid records to 'validated_orders'...")
#     df_valid.select("id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "record_raw") \
#         .write.format("mongo").option("uri", mongo_output_uri.replace("order_raw", "validated_orders")).mode("append").save()

#     print("⚠️ Saving quarantined records to 'quarantine_orders'...")
#     df_quarantine.select("id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "validation_errors", "record_raw") \
#         .write.format("mongo").option("uri", mongo_output_uri.replace("order_raw", "quarantine_orders")).mode("append").save()

#     valid_count = df_valid.count()
#     quarantine_count = df_quarantine.count()
#     elapsed_seconds = time.time() - start_time
#     throughput = rows_read / elapsed_seconds if elapsed_seconds > 0 else 0

#     print(f"\n--- ELT Summary ---")
#     print(f"Total Rows: {rows_read} | Valid: {valid_count} | Quarantine: {quarantine_count}")
   
#     # 3. بناء هيكل التقرير الشامل بالقياسات المطلوبة حصراً
#     final_report = {
#         "_id": id_run,
#         "file_name": "Loaded_from_MongoDB",
#         "file_size_mb": "N/A_in_ELT_Phase",
#         "engine_used": "PySpark_ELT",
#         "rows_read": rows_read,
#         "raw_loaded": rows_read,
#         "valid_count": valid_count,
#         "corrected_count": 0,
#         "quarantine_count": quarantine_count,
#         "elapsed_seconds": round(elapsed_seconds, 2),
#         "throughput": round(throughput, 2),
#         "batch_size_partitions": partitions,
#         "error_case_counts": error_counts_row,
#         "upsert_inserted_count": 0,
#         "upsert_updated_count": 0,
#         "upsert_unchanged_count": 0
#     }

#     # حفظ التقرير
#     # save_metrics_to_json(final_report)
   
#     return final_report

# # =====================================================================
# # 3. التشغيل المستقل (Main Execution)
# # =====================================================================


# if __name__ == "__main__":
#     import os
#     import sys
#     from datetime import datetime
   
#     os.environ['HADOOP_HOME'] = 'C:\\hadoop'
   
#     # 1. استدعاء الإعدادات الحقيقية من ملف config الخاص بك
#     sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#     from config.settings import MONGO_URI, MONGO_DB_NAME, RAW_COLLECTION_NAME
   
#     print("🚀 Starting Standalone ELT Pipeline with JSON Reporting...")
   
#     spark = SparkSession.builder \
#         .appName("Midterm_Data_Separator") \
#         .config("spark.mongodb.input.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}") \
#         .config("spark.mongodb.output.uri", f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.validated_orders") \
#         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#         .config("spark.driver.memory", "12g") \
#         .config("spark.executor.memory", "12g") \
#         .getOrCreate()
       
#     spark.sparkContext.setLogLevel("ERROR")
#     input_mongo_uri = f"{MONGO_URI.rstrip('/')}/{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"
   
#     # 2. تمرير "standalone_run" لكي يتجاوز الفلترة ويقرأ كل البيانات الموجودة في order_raw
#     validate_and_transform_data(spark, input_mongo_uri, input_mongo_uri, id_run="standalone_run")
   
#     spark.stop()
#     print("✅ Process Completed!")