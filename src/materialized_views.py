import os
import sys
from pymongo import MongoClient
from datetime import datetime

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, MONGO_DB_NAME

TARGET_COLLECTION = "validated_orders"

class MaterializedViewManager:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB_NAME]
        self.source_col = self.db[TARGET_COLLECTION]

    def _convert_to_double(self, field_name):
        """دالة مساعدة لتحويل النصوص إلى أرقام"""
        return {"$convert": {"input": f"${field_name}", "to": "double", "onError": 0, "onNull": 0}}

    def _get_last_sync_time(self, view_name):
        """1. دالة لجلب آخر وقت تم فيه التحديث من العرض المادي نفسه"""
        view_col = self.db[view_name]
        last_doc = view_col.find().sort("last_processed_at", -1).limit(1)
        last_doc = list(last_doc)
        
        if last_doc and "last_processed_at" in last_doc[0]:
            return last_doc[0]["last_processed_at"]
        return None

    def refresh_daily_sales_summary(self):
        """العرض المادي الأول: ملخص المبيعات اليومية (تزايدي)"""
        view_name = "mv_daily_sales_summary"
        last_sync = self._get_last_sync_time(view_name)
        
        print(f"\n🔄 Refreshing {view_name}...")
        print(f"   Last sync time: {last_sync if last_sync else 'Never (Full Build)'}")

        # 2. فلترة البيانات: جلب السجلات التي دخلت النظام "بعد" آخر تحديث فقط
        match_stage = {"record_raw.status": {"$nin": ["ملغي", "مرتجع"]}, "record_raw.order_date": {"$ne": None}}
        if last_sync:
            match_stage["processed_at"] = {"$gt": last_sync}

        # حساب عدد السجلات الجديدة لتجنب تشغيل محرك قاعدة البيانات إذا لم يكن هناك جديد
        records_processed = self.source_col.count_documents(match_stage)
        if records_processed == 0:
            print("⚡ View is already up to date. No new records found.")
            return {"view": view_name, "new_records_processed": 0}

        pipeline = [
            {"$match": match_stage},
            {"$group": {
                "_id": "$record_raw.order_date",
                "total_sales": {"$sum": self._convert_to_double("record_raw.total_amount")},
                "orders_count": {"$sum": 1},
                "last_processed_at": {"$max": "$processed_at"}
            }},
            # 3. سحر التحديث التزايدي: دمج الأرقام الجديدة مع القديمة
            {"$merge": {
                "into": view_name,
                "on": "_id",
                "whenMatched": [
                    {"$set": {
                        # إضافة المبيعات الجديدة إلى المبيعات الموجودة مسبقاً
                        "total_sales": {"$add": ["$total_sales", "$$new.total_sales"]},
                        "orders_count": {"$add": ["$orders_count", "$$new.orders_count"]},
                        "last_processed_at": {"$max": ["$last_processed_at", "$$new.last_processed_at"]}
                    }}
                ],
                "whenNotMatched": "insert" # إذا كان اليوم جديداً كلياً، قم بإدراجه
            }}
        ]
        
        self.source_col.aggregate(pipeline)
        print(f"✅ Successfully merged delta ({records_processed} new/updated records).")
        return {"view": view_name, "new_records_processed": records_processed}

    def refresh_payment_method_summary(self):
        """العرض المادي الثاني: ملخص أداء طرق الدفع (تزايدي)"""
        view_name = "mv_payment_method_summary"
        last_sync = self._get_last_sync_time(view_name)
        
        print(f"\n🔄 Refreshing {view_name}...")
        print(f"   Last sync time: {last_sync if last_sync else 'Never (Full Build)'}")

        match_stage = {"record_raw.payment_method": {"$ne": None}, "record_raw.status": {"$nin": ["ملغي", "مرتجع"]}}
        if last_sync:
            match_stage["processed_at"] = {"$gt": last_sync}

        records_processed = self.source_col.count_documents(match_stage)
        if records_processed == 0:
            print("⚡ View is already up to date. No new records found.")
            return {"view": view_name, "new_records_processed": 0}

        pipeline = [
            {"$match": match_stage},
            {"$group": {
                "_id": "$record_raw.payment_method",
                "total_sales": {"$sum": self._convert_to_double("record_raw.total_amount")},
                "orders_count": {"$sum": 1},
                "last_processed_at": {"$max": "$processed_at"}
            }},
            {"$merge": {
                "into": view_name,
                "on": "_id",
                "whenMatched": [
                    {"$set": {
                        "total_sales": {"$add": ["$total_sales", "$$new.total_sales"]},
                        "orders_count": {"$add": ["$orders_count", "$$new.orders_count"]},
                        "last_processed_at": {"$max": ["$last_processed_at", "$$new.last_processed_at"]}
                    }}
                ],
                "whenNotMatched": "insert"
            }}
        ]
        
        self.source_col.aggregate(pipeline)
        print(f"✅ Successfully merged delta ({records_processed} new/updated records).")
        return {"view": view_name, "new_records_processed": records_processed}

# =====================================================================
# التشغيل المباشر
# =====================================================================
if __name__ == "__main__":
    print("🚀 Starting Materialized Views Incremental Refresh...")
    mv_manager = MaterializedViewManager()
    
    # تحديث العرضين الماديين
    mv_manager.refresh_daily_sales_summary()
    mv_manager.refresh_payment_method_summary()
    print("\n🎉 Materialized Views Updated Successfully!")