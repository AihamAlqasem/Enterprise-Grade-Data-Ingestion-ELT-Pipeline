import csv
import time
from datetime import datetime
from pymongo import MongoClient
import sys
import os

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, MONGO_DB_NAME, BATCH_SIZE
from src.metrics import save_run_metrics

# تم تعديل اسم المجموعة ليتطابق تماماً مع متطلبات الوثيقة
TARGET_COLLECTION = "orders_raw"

def load_with_python_batch(file_path, run_id):
    """
    يقرأ ملف CSV بصورة تدفقية (Streaming) ويرفعه إلى MongoDB باستخدام insert_many.
    """
    print(f"\n--- Starting Python Batch Streaming Load for Run ID: {run_id} ---")
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
            
            # 🎯 تنظيف أسماء الأعمدة (الترويسة) من المسافات الخفية (مقبول لأنه يخص الهيكل وليس البيانات)
            if reader.fieldnames:
                reader.fieldnames = [field.strip() if field else field for field in reader.fieldnames]
            
            for row_number, row in enumerate(reader, start=1):
                # ❌ تم إزالة كود تنظيف order_id من هنا ليتم تطبيقه في طبقة Transform & Quality لاحقاً
                
                # 3. بناء الميتاداتا والسجل الخام (تم توحيد الأسماء لتتطابق مع الوثيقة)
                raw_document = {
                    "run_id": run_id, 
                    "source_file": file_name,
                    "source_row_number": row_number,
                    "ingested_at": datetime.utcnow(),
                    "engine_used": "python_batch",
                    "raw_record": row 
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
        "run_id": run_id,
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
    test_run_id = "batch_test_run_001"
    load_with_python_batch(SAMPLE_CSV_PATH, test_run_id)