import os
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, MONGO_DB_NAME

TARGET_COLLECTION = "validated_orders"

class DatabaseQueriesAnalyzer:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB_NAME]
        self.collection = self.db[TARGET_COLLECTION]

    def get_five_queries(self):
        """1. تعريف 5 استعلامات عملية تناسب طبيعة المشروع"""
        return {
            "Customer_Search": {"customer_id": "C001"},
            "City_and_Status": {"city": "الرياض", "status": "مؤكد"},
            "Recent_Orders": {"order_date": {"$gte": "2023-10-01", "$lte": "2023-10-31"}},
            "Payment_Method": {"payment_method": "الدفع عند الاستلام"},
            "High_Value_Orders": {"total_amount": {"$gt": 500.0}}
        }

    def run_explain(self, query):
        """تشغيل explain للحصول على إحصائيات الأداء"""
        try:
            explanation = self.collection.find(query).explain()
            stats = explanation.get("executionStats", {})
            return {
                "execution_time_ms": stats.get("executionTimeMillis", 0),
                "docs_examined": stats.get("totalDocsExamined", 0),
                "index_used": explanation.get("queryPlanner", {}).get("winningPlan", {}).get("stage") != "COLLSCAN"
            }
        except OperationFailure as e:
            return {"error": str(e)}

    def create_project_indexes(self):
        """2. إنشاء 3 فهارس (منها واحد مركب)"""
        print("\n⚙️ Building Indexes...")
        
        # 1. فهرس مفرد على customer_id
        self.collection.create_index([("customer_id", ASCENDING)], name="idx_customer_id")
        
        # 2. فهرس مركب على city و status
        self.collection.create_index([("city", ASCENDING), ("status", ASCENDING)], name="idx_city_status")
        
        # 3. فهرس مفرد على order_date (تم التعديل لتسريع التقارير الزمنية)
        self.collection.create_index([("order_date", DESCENDING)], name="idx_order_date")
        
        print("✅ Indexes built successfully.")

    def drop_project_indexes(self):
        """حذف الفهارس لتجربة الأداء قبل الفهرسة"""
        try:
            self.collection.drop_index("idx_customer_id")
            self.collection.drop_index("idx_city_status")
            self.collection.drop_index("idx_order_date")
        except OperationFailure:
            pass 

    def analyze_performance(self):
        """3. تنفيذ Explain قبل وبعد الفهارس لـ 3 استعلامات"""
        queries = self.get_five_queries()
        
        # اختيار الاستعلامات الثلاثة المتوافقة مع فهارسنا الجديدة
        test_cases = {
            "Q1_Customer": queries["Customer_Search"],
            "Q2_Compound": queries["City_and_Status"],
            "Q3_DateRange": queries["Recent_Orders"]
        }

        self.drop_project_indexes()
        
        results = {}
        print("\n⏳ Running Explain BEFORE Indexes (Table Scan)...")
        for name, q in test_cases.items():
            results[name] = {"query": q, "before": self.run_explain(q)}

        self.create_project_indexes()

        print("⚡ Running Explain AFTER Indexes (Index Scan)...")
        for name, q in test_cases.items():
            results[name]["after"] = self.run_explain(q)

        return results

if __name__ == "__main__":
    analyzer = DatabaseQueriesAnalyzer()
    
    print("🚀 Starting Queries & Indexes Analysis...")
    analysis_report = analyzer.analyze_performance()
    
    print("\n📊 --- Performance Comparison Report --- 📊")
    for q_name, data in analysis_report.items():
        print(f"\n🔹 {q_name} | Query: {data['query']}")
        print(f"   🔴 BEFORE: Time = {data['before']['execution_time_ms']} ms | Docs Examined = {data['before']['docs_examined']} | Indexed = {data['before']['index_used']}")
        print(f"   🟢 AFTER : Time = {data['after']['execution_time_ms']} ms | Docs Examined = {data['after']['docs_examined']} | Indexed = {data['after']['index_used']}")

    print("\n💡 --- Justification & Impact (سبب الاختيار وأثره) ---")
    print("1. idx_customer_id (Single): يمنع محرك قاعدة البيانات من مسح ملايين السجلات (COLLSCAN) للبحث عن طلبات عميل واحد، الأثر: تقليص المستندات المفحوصة إلى عدد طلبات العميل فقط.")
    print("2. idx_city_status (Compound): يخدم مدراء الفروع عند فلترة الطلبات (مثلاً: طلبات الرياض التي حالتها 'مؤكد'). الأثر: الفهرس المركب يوفر وصولاً مباشراً للتقاطع بين الشرطين دون فرز إضافي في الذاكرة.")
    print("3. idx_order_date (Single): ضروري جداً لتوليد التقارير الدورية (يومية، شهرية، سنوية). الأثر: تسريع عمليات البحث بين نطاقين زمنيين ($gte, $lte) وعمليات الفرز الزمني بشكل هائل.")