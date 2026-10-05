import os

# المسار الرئيسي للمشروع (يقوم بحسابه تلقائياً)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# -----------------------------------------
# إعدادات الموجه التلقائي (File Router)
# -----------------------------------------
SMALL_FILE_THRESHOLD_MB = 200

# -----------------------------------------
# مسارات الملفات
# -----------------------------------------
# تأكد أن اسم الملف الضخم هنا يطابق الاسم الفعلي الموجود في مجلد data
HUGE_CSV_PATH = os.path.join(BASE_DIR, "data", "orders_huge_mixed_quality.csv")
SAMPLE_CSV_PATH = os.path.join(BASE_DIR, "data", "sample_orders.csv")

# عدد صفوف العينة
SAMPLE_ROWS = 100000



# -----------------------------------------
# إعدادات قاعدة البيانات MongoDB
# -----------------------------------------
MONGO_URI = "mongodb://localhost:27017/" # قم بتعديله إذا كانت قاعدتك سحابية
# MONGO_DB_NAME = "midterm_pipeline_db"
MONGO_DB_NAME = "for_test"
RAW_COLLECTION_NAME = "orders_raw"
# -----------------------------------------
# إعدادات محرك البايثون (Batch Loader)
# -----------------------------------------
BATCH_SIZE = 5000 # عدد السجلات في كل دفعة


# -----------------------------------------
# إعدادات محرك PySpark
# -----------------------------------------
# رابط الاتصال الخاص بموصل Spark-MongoDB
SPARK_MONGO_OUTPUT_URI = f"{MONGO_URI}{MONGO_DB_NAME}.{RAW_COLLECTION_NAME}"

# حزمة الموصل (Connector) التي سيحملها Spark للاتصال بـ MongoDB
# تم اختيار إصدار متوافق وشائع، ويمكن تغييره بناء على إصدار Spark لديك
SPARK_MONGO_PACKAGES = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"






