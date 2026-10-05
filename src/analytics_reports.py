import os
import sys
from pymongo import MongoClient

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, MONGO_DB_NAME

TARGET_COLLECTION = "orders_validated"

class AggregationReports:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB_NAME]
        self.collection = self.db[TARGET_COLLECTION]

    def _convert_to_double(self, field_name):
        """دالة مساعدة لتحويل النصوص إلى أرقام داخل MongoDB"""
        # تمت إضافة المسار الصحيح للحقل المتداخل
        return {"$convert": {"input": f"${field_name}", "to": "double", "onError": 0, "onNull": 0}}

    def report_orders_by_status(self):
        """التقرير الأول: توزيع الطلبات حسب الحالة"""
        pipeline = [
            {"$group": {"_id": "$record_raw.status", "total_orders": {"$sum": 1}}},
            {"$sort": {"total_orders": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def report_sales_by_city(self):
        """التقرير الثاني: إجمالي المبيعات والطلبات حسب المدينة"""
        pipeline = [
            {"$match": {"record_raw.status": {"$nin": ["ملغي", "مرتجع"]}}},
            {"$group": {
                "_id": "$record_raw.city",
                "total_sales": {"$sum": self._convert_to_double("record_raw.total_amount")},
                "total_orders": {"$sum": 1}
            }},
            {"$sort": {"total_sales": -1}},
            {"$limit": 10}
        ]
        return list(self.collection.aggregate(pipeline))

    def report_top_customers(self):
        """التقرير الثالث: أفضل 5 عملاء (Top Customers)"""
        pipeline = [
            {"$match": {"record_raw.status": {"$nin": ["ملغي", "مرتجع"]}, "record_raw.customer_id": {"$ne": "UNKNOWN"}}},
            {"$group": {
                "_id": "$record_raw.customer_id",
                "total_spent": {"$sum": self._convert_to_double("record_raw.total_amount")},
                "orders_count": {"$sum": 1}
            }},
            {"$sort": {"total_spent": -1}},
            {"$limit": 5}
        ]
        return list(self.collection.aggregate(pipeline))

    def report_sales_by_payment_method(self):
        """التقرير الرابع: المبيعات حسب طريقة الدفع بدلاً من التاريخ"""
        pipeline = [
            {"$match": {"record_raw.payment_method": {"$ne": None}, "record_raw.status": {"$nin": ["ملغي", "مرتجع"]}}},
            {"$group": {
                "_id": "$record_raw.payment_method",
                "total_revenue": {"$sum": self._convert_to_double("record_raw.total_amount")},
                "orders_count": {"$sum": 1}
            }},
            {"$sort": {"total_revenue": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def report_sales_by_delivery_type(self):
        """التقرير الخامس: أداء أنواع التوصيل (Sales by Delivery Type)"""
        pipeline = [
            {"$match": {"record_raw.delivery_type": {"$ne": None}}},
            {"$group": {
                "_id": "$record_raw.delivery_type",
                "total_revenue": {"$sum": self._convert_to_double("record_raw.total_amount")},
                "total_orders": {"$sum": 1}
            }},
            {"$sort": {"total_revenue": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def run_all_reports(self):
        """تشغيل جميع التقارير وطباعتها للمناقشة"""
        print("\n📊 --- Report 1: Orders by Status ---")
        for row in self.report_orders_by_status(): print(row)

        print("\n🏙️ --- Report 2: Top 10 Cities by Sales ---")
        for row in self.report_sales_by_city(): print(row)

        print("\n👑 --- Report 3: Top 5 Customers ---")
        for row in self.report_top_customers(): print(row)

        print("\n💳 --- Report 4: Sales by Payment Method ---")
        for row in self.report_sales_by_payment_method(): print(row)

        print("\n🚚 --- Report 5: Delivery Type Performance ---")
        for row in self.report_sales_by_delivery_type(): print(row)

if __name__ == "__main__":
    print("🚀 Generating Aggregation Reports...")
    reports = AggregationReports()
    reports.run_all_reports()
    print("\n✅ Reports generated successfully!")