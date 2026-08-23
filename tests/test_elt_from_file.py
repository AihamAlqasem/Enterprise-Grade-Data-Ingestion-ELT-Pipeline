import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, expr, regexp_replace, abs, lit, struct, array, current_timestamp, trim

# إعداد البيئة (تأكد من مسار Hadoop لديك)
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

def run_elt_test(file_path):
    print(f"\n--- 🚀 Starting ELT Logic Test on File: {file_path} ---")
    start_time = time.time()

    # 1. تهيئة جلسة Spark للاختبار
    spark = SparkSession.builder \
        .appName("ELT_File_Tester") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # 2. قراءة الملف بنفس طريقة الـ Raw (كل الحقول كنصوص للحفاظ على التشوهات)
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

    print("📖 Reading Data...")
    df_raw = spark.read.csv(file_path, header=True, schema=csv_schema, enforceSchema=False)
    
    if df_raw.count() == 0:
        print("⚠️ الملف فارغ!")
        spark.stop()
        return

    # -----------------------------------------------------------------
    # 🎯 1. تنظيف المفتاح الأساسي وإزالة التكرارات
    # -----------------------------------------------------------------
    df_flattened = df_raw.withColumn("clean_order_id", regexp_replace(col("order_id"), r"[^\d]", ""))
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

    phone_audit = when(col("customer_phone") != col("clean_phone"), struct(lit("customer_phone").alias("field"), col("customer_phone").cast("string").alias("original_value"), col("clean_phone").cast("string").alias("corrected_value"), lit("PHONE_FORMAT_CLEANED").alias("rule_code"))).otherwise(expr("NULL"))
    email_audit = when(col("customer_email") != col("clean_email"), struct(lit("customer_email").alias("field"), col("customer_email").cast("string").alias("original_value"), col("clean_email").cast("string").alias("corrected_value"), lit("EMAIL_REPEATED_SYMBOLS").alias("rule_code"))).otherwise(expr("NULL"))
    payment_audit = when(col("payment_amount") != col("clean_payment"), struct(lit("payment_amount").alias("field"), col("payment_amount").cast("string").alias("original_value"), col("clean_payment").cast("string").alias("corrected_value"), lit("NEGATIVE_PAYMENT_FIXED").alias("rule_code"))).otherwise(expr("NULL"))
    status_audit = when(col("status") != col("clean_status"), struct(lit("status").alias("field"), col("status").cast("string").alias("original_value"), col("clean_status").cast("string").alias("corrected_value"), lit("STATUS_SPACES_TRIMMED").alias("rule_code"))).otherwise(expr("NULL"))

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

    # -----------------------------------------------------------------
    # 🎯 4. الفصل والتوزيع الثلاثي
    # -----------------------------------------------------------------
    df_all_valid = df_with_errors.filter("size(validation_errors) == 0")
    df_quarantine = df_with_errors.filter("size(validation_errors) > 0")

    df_all_valid = df_all_valid.withColumn("quality_status", when(expr("size(corrections) > 0"), lit("corrected")).otherwise(lit("valid")))
    df_quarantine = df_quarantine.withColumn("quality_status", lit("quarantined"))

    # تقسيم النتائج للعرض
    df_pure_valid = df_all_valid.filter(col("quality_status") == "valid")
    df_corrected = df_all_valid.filter(col("quality_status") == "corrected")

    # -----------------------------------------------------------------
    # 🎯 5. عرض النتائج والإحصائيات
    # -----------------------------------------------------------------
    print(f"\n📊 --- إحصائيات الاختبار --- 📊")
    print(f"إجمالي الصفوف المقروءة: {df_raw.count()}")
    print(f"✅ السليم (Valid): {df_pure_valid.count()}")
    print(f"🛠️ المصحح (Corrected): {df_corrected.count()}")
    print(f"❌ المعزول (Quarantined): {df_quarantine.count()}")
    
    print("\n\n🟢 1. عينة من البيانات السليمة (Valid) - لا توجد بها أخطاء:")
    df_pure_valid.select("order_id", "quality_status", "clean_status", "clean_email").show(5, truncate=False)

    print("\n🟡 2. عينة من البيانات المصححة (Corrected) - تم تعديلها لتصبح سليمة:")
    df_corrected.select("order_id", "quality_status", "corrections").show(5, truncate=False)

    print("\n🔴 3. عينة من البيانات المعزولة (Quarantined) - فشلت في الفحص:")
    df_quarantine.select("order_id", "quality_status", "validation_errors").show(5, truncate=False)

    spark.stop()
    print(f"\n⏱️ استغرق الاختبار: {round(time.time() - start_time, 2)} ثانية.")

# =====================================================================
# التشغيل
# =====================================================================
if __name__ == "__main__":
    # ضع مسار ملف الـ CSV الذي تريد اختباره هنا
    TEST_FILE_PATH = "data/sample_orders.csv"  # استبدله بمسار الملف الذي لديك
    
    if os.path.exists(TEST_FILE_PATH):
        run_elt_test(TEST_FILE_PATH)
    else:
        print(f"❌ لم يتم العثور على الملف: {TEST_FILE_PATH}")
        print("يرجى التأكد من مسار الملف وإعادة المحاولة.")