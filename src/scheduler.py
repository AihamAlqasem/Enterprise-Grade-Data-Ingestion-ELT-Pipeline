import os
import sys
from datetime import datetime
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import time

# استدعاء الإعدادات والعروض المادية
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, MONGO_DB_NAME
from src.materialized_views import MaterializedViewManager

class JobSchedulerManager:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB_NAME]
        self.logs_col = self.db["job_logs"] # الكوليكشن الجديد لتسجيل المهام
        self.mv_manager = MaterializedViewManager()

    def _log_job(self, job_name, start_time, status, details=""):
        """دالة لتسجيل نتيجة تنفيذ المهمة في قاعدة البيانات (Job Logging)"""
        end_time = datetime.now()
        log_entry = {
            "job_name": job_name,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": round((end_time - start_time).total_seconds(), 2),
            "status": status,
            "details": details
        }
        self.logs_col.insert_one(log_entry)
        print(f"📝 Job Logged: {job_name} | Status: {status} | Duration: {log_entry['duration_seconds']}s")

    def job_refresh_daily_sales(self):
        """المهمة الأولى: تحديث مبيعات الأيام"""
        start_time = datetime.now()
        print(f"\n[{start_time.strftime('%H:%M:%S')}] ⚙️ EXECUTING JOB: Refresh Daily Sales...")
        try:
            result = self.mv_manager.refresh_daily_sales_summary()
            self._log_job("Refresh_Daily_Sales", start_time, "SUCCESS", result)
        except Exception as e:
            self._log_job("Refresh_Daily_Sales", start_time, "FAILED", str(e))

    def job_refresh_payment_methods(self):
        """المهمة الثانية: تحديث طرق الدفع"""
        start_time = datetime.now()
        print(f"\n[{start_time.strftime('%H:%M:%S')}] ⚙️ EXECUTING JOB: Refresh Payment Methods...")
        try:
            result = self.mv_manager.refresh_payment_method_summary()
            self._log_job("Refresh_Payment_Methods", start_time, "SUCCESS", result)
        except Exception as e:
            self._log_job("Refresh_Payment_Methods", start_time, "FAILED", str(e))

# =====================================================================
# واجهة التشغيل والاختبار (CLI Menu) لتلبية شرط المناقشة
# =====================================================================
if __name__ == "__main__":
    manager = JobSchedulerManager()
    
    # إعداد المجدول ليعمل في الخلفية (مثال: كل ساعة، وكل ساعتين)
    scheduler = BackgroundScheduler()
    scheduler.add_job(manager.job_refresh_daily_sales, 'interval', hours=1, id='job_sales')
    scheduler.add_job(manager.job_refresh_payment_methods, 'interval', hours=2, id='job_payments')
    scheduler.start()

    print("🚀 Background Scheduler Started! (Jobs will run automatically based on interval)")
    
    # قائمة تفاعلية لتشغيل المهام يدوياً وقت المناقشة
    try:
        while True:
            print("\n" + "="*40)
            print("🕒 MANUAL JOB TRIGGER MENU")
            print("="*40)
            print("1. Run 'Refresh Daily Sales' Job Now")
            print("2. Run 'Refresh Payment Methods' Job Now")
            print("3. View Last 5 Job Logs")
            print("4. Exit")
            
            choice = input("\nEnter your choice (1-4): ")
            
            if choice == '1':
                manager.job_refresh_daily_sales()
            elif choice == '2':
                manager.job_refresh_payment_methods()
            elif choice == '3':
                print("\n📊 --- Last 5 Job Executions ---")
                logs = manager.logs_col.find().sort("start_time", -1).limit(5)
                for log in logs:
                    print(f"[{log['start_time'].strftime('%Y-%m-%d %H:%M:%S')}] {log['job_name']} -> {log['status']} ({log['duration_seconds']}s)")
            elif choice == '4':
                print("👋 Shutting down scheduler...")
                scheduler.shutdown()
                break
            else:
                print("⚠️ Invalid choice. Try again.")
                
            time.sleep(1) # إيقاف مؤقت بسيط لتنظيم الشاشة
            
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("\nScheduler stopped.")