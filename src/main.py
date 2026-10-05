import os
import sys
import subprocess
import time  # <-- أضفنا مكتبة الوقت هنا لتوليد الـ run_id

# التأكد من المسارات لضمان توافق الاستيراد
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)

if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config import settings
import file_router

# <-- أضفنا run_id هنا كمتغير تستقبله الدالة
def run_script(script_name, target_file, run_id):
    """دالة تشغيل الملفات مع تمرير مسار المشروع ومسار الملف كمتغير بيئة"""
    script_path = os.path.join(CURRENT_DIR, script_name)
    
    if not os.path.exists(script_path):
        print(f"❌ خطأ: الملف {script_name} غير موجود في {script_path}")
        return False
        
    print(f"\n▶️ جاري تشغيل: {script_name} ...")
    
    # تجهيز بيئة التشغيل
    env = os.environ.copy()
    env["PYTHONPATH"] = PARENT_DIR + os.pathsep + env.get("PYTHONPATH", "")
    # نمرر مسار الملف كمتغير بيئة لتستفيد منه المحركات لاحقاً
    env["PIPELINE_INPUT_FILE"] = target_file 
    
    # <-- قمنا بتمرير run_id هنا لكي يصل للملفات الأخرى
    result = subprocess.run([sys.executable, script_path, run_id], env=env)
    
    if result.returncode == 0:
        print(f"✅ اكتمل تشغيل {script_name} بنجاح.")
        return True
    else:
        print(f"❌ فشل تشغيل {script_name}. (كود الخطأ: {result.returncode})")
        return False


def main():
    while True:
        print("\n" + "="*60)
        print("🚀 Hi , my dear in Aiham Program (System Orchestrator)")
        print("="*60)
        print("1. Run api (API & Browser)")
        print("2. ingest data  (Data Pipeline)")
        print("3. Exet from program (Exit)")
        print("="*60)
        
        choice = input("\nأدخل رقم الخيار (1 أو 2 أو 3):\n> ").strip()
        
        if choice == '1':
            # تشغيل ملف الواجهة (API)
            # ملاحظة: تأكد أن اسم ملف الواجهة هو "app.py" أو قم بتغييره أدناه
            api_file = "api.py" 
            api_path = os.path.join(CURRENT_DIR, api_file)
            
            if os.path.exists(api_path):
                print(f"\n🌐 جاري تشغيل الواجهة من ملف {api_file}...")
                subprocess.run([sys.executable, api_path])
            else:
                print(f"\n❌ خطأ: لم يتم العثور على ملف الواجهة ({api_file}) في المجلد الحالي.")
                
        elif choice == '2':
            # --- كود خط سير البيانات (Pipeline) ---
            target_file = input("\nأدخل المسار الكامل لملف البيانات لتبدأ المعالجة:\n> ").strip()
            
            # تنظيف المسار من علامات التنصيص
            target_file = target_file.strip('\'"')
            
            if not os.path.exists(target_file):
                print(f"\n❌ خطأ: لم أتمكن من العثور على الملف في المسار المذكور:\n{target_file}")
                print("تأكد من صحة المسار وحاول مجدداً.")
                continue # العودة للقائمة الرئيسية بدلاً من إيقاف البرنامج

            # إنشاء run_id للعملية بالكامل كما طلب الدكتور في القسم 4
            run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"

            # توجيه الملف باستخدام الموجه
            print("\n🔍 [المرحلة 1]: توجيه الملف (File Routing)...")
            try:
                engine_choice, size_mb = file_router.route_file(target_file)
            except Exception as e:
                print(f"❌ حدث خطأ أثناء فحص الملف: {e}")
                continue

            # 2. تشغيل محرك الرفع
            print("\n⏳ [المرحلة 2]: رفع البيانات الخام (Raw Load)...")
            if engine_choice == 'python_batch':
                success = run_script("load_batch.py", target_file, run_id)
            elif engine_choice == 'pyspark':
                success = run_script("spark_engine.py", target_file, run_id)
            else:
                print("❌ قرار توجيه غير معروف!")
                continue

            # 3. تشغيل المعالجة ELT
            if success:
                print("\n⏳ [المرحلة 3]: تطبيق الجودة والمعالجة (ELT Pipeline)...")
                run_script("spark_etl_pipeline.py", target_file, run_id)
            else:
                print("\n⚠️ توقف خط السير بسبب فشل مرحلة رفع البيانات.")

            print("\n🏆 انتهت جميع العمليات المجدولة لمعالجة هذا الملف!")
            input("\nاضغط Enter للعودة للقائمة الرئيسية...")

        elif choice == '3':
            print("\n👋 تم إغلاق النظام بنجاح. وداعاً!")
            break # إيقاف حلقة التكرار والخروج من البرنامج
            
        else:
            print("\n❌ خيار غير صحيح! الرجاء إدخال رقم 1 أو 2 أو 3 فقط.")


if __name__ == "__main__":
    main()