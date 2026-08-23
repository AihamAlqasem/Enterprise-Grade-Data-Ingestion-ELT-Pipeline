import csv
import time
import re
from datetime import datetime
from pymongo import MongoClient
import sys
import os

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, MONGO_DB_NAME, BATCH_SIZE
from src.metrics import save_run_metrics

# تحديد اسم المجموعة بشكل ثابت
TARGET_COLLECTION = "order_raw"

def load_with_python_batch(file_path, id_run):
    """
    يقرأ ملف CSV بصورة تدفقية (Streaming) ويرفعه إلى MongoDB باستخدام insert_many.
    """
    print(f"\n--- Starting Python Batch Streaming Load for Run ID: {id_run} ---")
    start_time = time.time()
    
    # 1. الاتصال بقاعدة البيانات
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    raw_collection = db[TARGET_COLLECTION] 
    
    file_name = os.path.basename(file_path)
    
    batch = []
    batch_number = 1
    total_inserted = 0
    
    # 2. القراءة التدفقية (Streaming) سطرًا بسطر لمنع استهلاك الذاكرة
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # 🎯 تنظيف أسماء الأعمدة (الترويسة) من المسافات الخفية
            if reader.fieldnames:
                reader.fieldnames = [field.strip() if field else field for field in reader.fieldnames]
            
            for row_number, row in enumerate(reader, start=1):
                # 🎯 تنظيف واستخراج الأرقام فقط من order_id
                if "order_id" in row and row["order_id"]:
                    row["order_id"] = re.sub(r'[^\d]', '', str(row["order_id"]))

                # 3. بناء الميتاداتا والسجل الخام 
                raw_document = {
                    "id_run": id_run,
                    "file_source": file_name,
                    "number_row_source": row_number,
                    "at_ingested": datetime.utcnow(),
                    "engine_used": "python_batch",
                    "record_raw": row 
                }
                
                batch.append(raw_document)
                
                # 4. إرسال الدفعة باستخدام insert_many
                if len(batch) == BATCH_SIZE:
                    inserted_in_batch = _insert_batch(raw_collection, batch, batch_number)
                    total_inserted += inserted_in_batch
                    batch.clear()
                    batch_number += 1
            
            # 5. إرسال الدفعة الأخيرة المتبقية
            if batch:
                inserted_in_batch = _insert_batch(raw_collection, batch, batch_number)
                total_inserted += inserted_in_batch
                
    except Exception as e:
        print(f"❌ CRITICAL ERROR during file reading/processing: {e}")
    finally:
        client.close()

    total_time = time.time() - start_time
    throughput = total_inserted / total_time if total_time > 0 else 0
    
    print("\n--- Batch Insertion Summary ---")
    print(f"Target Collection: {TARGET_COLLECTION}")
    print(f"Total Records Inserted: {total_inserted}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} records/second")
    print("-------------------------------\n")

    # 6. 📝 تحديث تقرير الـ Metrics
    metrics = {
        "total_records_inserted": total_inserted,
        "total_time_seconds": round(total_time, 2),
        "throughput_records_per_second": round(throughput, 2),
        "batch_size_partitions": BATCH_SIZE,
        "upsert_inserted_count": total_inserted, 
        "upsert_updated_count": 0,
        "upsert_unchanged_count": 0,
        "mode": "insert_many (Streaming Mode)",
        "collection": TARGET_COLLECTION
    }
    
    batch_report = {
        "id_run": id_run,
        "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "phase": "Ingestion_Python_Batch",
        "metrics": metrics,
        "status": "Success" if total_inserted > 0 else "Failed"
    }

    # حفظ التقرير باستخدام الدالة الأصلية الخاصة بك
    save_run_metrics(batch_report)
    print("✅ Batch Ingestion metrics successfully saved to reports/results.json")
    
    return metrics

def _insert_batch(collection, batch, batch_number):
    """دالة لمعالجة الدفعة باستخدام insert_many السريعة"""
    batch_start = time.time()
    try:
        # استخدام الإدراج المباشر والسريع
        result = collection.insert_many(batch) 
        
        inserted_count = len(result.inserted_ids)
        batch_time = time.time() - batch_start
        rate = inserted_count / batch_time if batch_time > 0 else 0
        
        print(f"Batch {batch_number} | Inserted: {inserted_count} | Time: {batch_time:.2f}s | Rate: {rate:.0f} rec/s")
        return inserted_count
            
    except Exception as e:
        print(f"❌ ERROR executing insert_many for Batch {batch_number}: {e}")
        return 0

if __name__ == "__main__":
    from config.settings import SAMPLE_CSV_PATH
    test_id_run = "batch_test_run_001"
    load_with_python_batch(SAMPLE_CSV_PATH, test_id_run)


# \\\\\\\\\\\\\\\\\\\\\
# import csv
# import time
# from datetime import datetime
# from pymongo import MongoClient
# import sys
# import os

# # استدعاء الإعدادات
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from config.settings import MONGO_URI, MONGO_DB_NAME, BATCH_SIZE
# from src.metrics import save_run_metrics

# # تحديد اسم المجموعة بشكل ثابت
# TARGET_COLLECTION = "order_raw"

# def load_with_python_batch(file_path, id_run):
#     """
#     يقرأ ملف CSV بصورة تدفقية (Streaming) ويرفعه إلى MongoDB باستخدام insert_many.
#     """
#     print(f"\n--- Starting Python Batch Streaming Load for Run ID: {id_run} ---")
#     start_time = time.time()
    
#     # 1. الاتصال بقاعدة البيانات
#     client = MongoClient(MONGO_URI)
#     db = client[MONGO_DB_NAME]
#     raw_collection = db[TARGET_COLLECTION] 
    
#     file_name = os.path.basename(file_path)
    
#     batch = []
#     batch_number = 1
#     total_inserted = 0
    
#     # 2. القراءة التدفقية (Streaming) سطرًا بسطر لمنع استهلاك الذاكرة
#     try:
#         with open(file_path, 'r', encoding='utf-8-sig') as f:
#             reader = csv.DictReader(f)
            
#             for row_number, row in enumerate(reader, start=1):
#                 # 3. بناء الميتاداتا والسجل الخام كما طلبت بالضبط
#                 raw_document = {
#                     "id_run": id_run,
#                     "file_source": file_name,
#                     "number_row_source": row_number,
#                     "at_ingested": datetime.utcnow(),
#                     "engine_used": "python_batch",
#                     "record_raw": row 
#                 }
                
#                 batch.append(raw_document)
                
#                 # 4. إرسال الدفعة باستخدام insert_many
#                 if len(batch) == BATCH_SIZE:
#                     inserted_in_batch = _insert_batch(raw_collection, batch, batch_number)
#                     total_inserted += inserted_in_batch
#                     batch.clear()
#                     batch_number += 1
            
#             # 5. إرسال الدفعة الأخيرة المتبقية
#             if batch:
#                 inserted_in_batch = _insert_batch(raw_collection, batch, batch_number)
#                 total_inserted += inserted_in_batch
                
#     except Exception as e:
#         print(f"❌ CRITICAL ERROR during file reading/processing: {e}")
#     finally:
#         client.close()

#     total_time = time.time() - start_time
#     throughput = total_inserted / total_time if total_time > 0 else 0
    
#     print("\n--- Batch Insertion Summary ---")
#     print(f"Target Collection: {TARGET_COLLECTION}")
#     print(f"Total Records Inserted: {total_inserted}")
#     print(f"Total Time: {total_time:.2f} seconds")
#     print(f"Throughput: {throughput:.2f} records/second")
#     print("-------------------------------\n")

#     # 6. 📝 تحديث تقرير الـ Metrics
#     metrics = {
#         "total_records_inserted": total_inserted,
#         "total_time_seconds": round(total_time, 2),
#         "throughput_records_per_second": round(throughput, 2),
#         "batch_size_partitions": BATCH_SIZE,
#         "upsert_inserted_count": total_inserted, 
#         "upsert_updated_count": 0,
#         "upsert_unchanged_count": 0,
#         "mode": "insert_many (Streaming Mode)",
#         "collection": TARGET_COLLECTION
#     }
    
#     batch_report = {
#         "id_run": id_run,
#         "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "phase": "Ingestion_Python_Batch",
#         "metrics": metrics,
#         "status": "Success" if total_inserted > 0 else "Failed"
#     }

#     save_run_metrics(batch_report)
#     print("✅ Batch Ingestion metrics successfully saved to reports/results.json")
    
#     return metrics

# def _insert_batch(collection, batch, batch_number):
#     """دالة لمعالجة الدفعة باستخدام insert_many السريعة"""
#     batch_start = time.time()
#     try:
#         # استخدام الإدراج المباشر والسريع
#         result = collection.insert_many(batch) 
        
#         inserted_count = len(result.inserted_ids)
#         batch_time = time.time() - batch_start
#         rate = inserted_count / batch_time if batch_time > 0 else 0
        
#         print(f"Batch {batch_number} | Inserted: {inserted_count} | Time: {batch_time:.2f}s | Rate: {rate:.0f} rec/s")
#         return inserted_count
            
#     except Exception as e:
#         print(f"❌ ERROR executing insert_many for Batch {batch_number}: {e}")
#         return 0

# if __name__ == "__main__":
#     from config.settings import SAMPLE_CSV_PATH
#     test_id_run = "batch_test_run_001"
#     load_with_python_batch(SAMPLE_CSV_PATH, test_id_run)


#\\\\\\\\\\
#
# import csv
# import time
# from datetime import datetime
# from pymongo import MongoClient, UpdateOne
# import sys
# import os

# # استدعاء الإعدادات
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from config.settings import MONGO_URI, MONGO_DB_NAME, BATCH_SIZE
# from src.metrics import save_run_metrics

# # تحديد اسم المجموعة بشكل ثابت
# TARGET_COLLECTION = "order_raw"

# def load_with_python_batch(file_path, id_run):
#     """
#     يقرأ ملف CSV الضخم ويرفعه إلى MongoDB.
#     يحافظ على 100% من السجلات دون نقصان، ويطبق الـ Upsert لمنع التكرار عند إعادة التشغيل.
#     """
#     print(f"\n--- Starting Python Batch Load (Full Fidelity Upsert) for Run ID: {id_run} ---")
#     start_time = time.time()
    
#     # 1. الاتصال بقاعدة البيانات
#     client = MongoClient(MONGO_URI)
#     db = client[MONGO_DB_NAME]
#     raw_collection = db[TARGET_COLLECTION] 
    
#     file_name = os.path.basename(file_path)

#     # ⚡ [تسريع Upsert]: بناء فهرس مركب (اسم الملف + رقم الصف) لضمان سرعة التحديث وعدم دمج الطلبات المكررة في نفس الملف
#     print("⚡ Creating Composite Index (file_source + row_number) to supercharge Upsert...")
#     raw_collection.create_index([("file_source", 1), ("number_row_source", 1)])

#     # 🧹 [تطبيق Idempotency لحماية التشغيلة الحالية من التعطل المفرط]
#     print(f"🧹 Enforcing Idempotency: Cleaning crashed data (if any) for Run ID: {id_run}...")
#     deleted_result = raw_collection.delete_many({"id_run": id_run})
#     print(f"🗑️ Cleared {deleted_result.deleted_count} incomplete records.")
    
#     batch = []
#     batch_number = 1
#     total_processed = 0
    
#     # 2. القراءة التدفقية الآمنة
#     try:
#         with open(file_path, 'r', encoding='utf-8-sig') as f:
#             reader = csv.DictReader(f)
            
#             for row_number, row in enumerate(reader, start=1):
#                 # 3. بناء الميتاداتا والسجل الخام
#                 raw_document = {
#                     "id_run": id_run,
#                     "file_source": file_name,
#                     "number_row_source": row_number,
#                     "at_ingested": datetime.utcnow(),
#                     "engine_used": "python_batch",
#                     "record_raw": row 
#                 }
                
#                 batch.append(raw_document)
                
#                 # 4. إرسال الدفعة
#                 if len(batch) == BATCH_SIZE:
#                     processed_in_batch = _upsert_batch(raw_collection, batch, batch_number, file_name)
#                     total_processed += processed_in_batch
#                     batch.clear()
#                     batch_number += 1
            
#             # 5. إرسال الدفعة الأخيرة
#             if batch:
#                 processed_in_batch = _upsert_batch(raw_collection, batch, batch_number, file_name)
#                 total_processed += processed_in_batch
                
#     except Exception as e:
#         print(f"❌ CRITICAL ERROR during file reading/processing: {e}")
#     finally:
#         client.close()

#     total_time = time.time() - start_time
#     throughput = total_processed / total_time if total_time > 0 else 0
    
#     print("\n--- Batch Upserting Summary ---")
#     print(f"Target Collection: {TARGET_COLLECTION}")
#     print(f"Total Records Processed: {total_processed}")
#     print(f"Total Time: {total_time:.2f} seconds")
#     print(f"Throughput: {throughput:.2f} records/second")
#     print("-------------------------------\n")

#     # 6. 📝 إنشاء التقرير وحفظه
#     metrics = {
#         "total_records_processed": total_processed,
#         "total_time_seconds": round(total_time, 2),
#         "throughput_records_per_second": round(throughput, 2),
#         "batch_size": BATCH_SIZE,
#         "mode": "upsert_by_row (no data loss)",
#         "collection": TARGET_COLLECTION
#     }
    
#     batch_report = {
#         "id_run": id_run,
#         "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "phase": "Ingestion_Python_Batch",
#         "metrics": metrics,
#         "status": "Success" if total_processed > 0 else "Failed"
#     }

#     save_run_metrics(batch_report)
#     print("✅ Batch Ingestion metrics successfully saved to reports/results.json")
    
#     return metrics

# def _upsert_batch(collection, batch, batch_number, file_name):
#     """دالة لمعالجة الدفعة باستخدام Upsert الذكي بناءً على رقم الصف"""
#     batch_start = time.time()
#     operations = []
#     try:
#         for doc in batch:
#             row_number = doc["number_row_source"]
            
#             # Upsert: يبحث عن نفس الملف ونفس رقم الصف لكي لا ينقص أي سجل!
#             ops = UpdateOne(
#                 {
#                     "file_source": file_name,
#                     "number_row_source": row_number
#                 }, 
#                 {"$set": doc}, 
#                 upsert=True
#             )
#             operations.append(ops)

#         if operations:
#             result = collection.bulk_write(operations)
#             batch_time = time.time() - batch_start
#             rate = len(batch) / batch_time if batch_time > 0 else 0
            
#             upserted_count = result.upserted_count + result.inserted_count
#             modified_count = result.modified_count
            
#             print(f"Batch {batch_number} | New: {upserted_count} | Updated: {modified_count} | Time: {batch_time:.2f}s | Rate: {rate:.0f} rec/s")
            
#             return len(batch)
            
#     except Exception as e:
#         print(f"❌ ERROR executing Upsert for Batch {batch_number}: {e}")
#         return 0
#     return 0

# if __name__ == "__main__":
#     from config.settings import SAMPLE_CSV_PATH
#     test_id_run = "batch_test_run_001"
#     load_with_python_batch(SAMPLE_CSV_PATH, test_id_run)
 