import json
import os
import sys
from datetime import datetime

# تحديد مسارات مجلد التقارير والملف
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
RESULTS_FILE = os.path.join(REPORTS_DIR, "results.json")

def save_run_metrics(metrics_data):
    """
    يستلم بيانات القياسات كقاموس (Dictionary) ويحفظها في ملف results.json.
    إذا كان الملف موجوداً، يضيف البيانات الجديدة إليه دون حذف القديمة.
    """
    # 1. التأكد من وجود مجلد التقارير، وإن لم يكن موجوداً نقوم بإنشائه
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)

    existing_data = []
    
    # 2. إذا كان الملف موجوداً، نقرأ محتواه القديم أولاً لكي لا نمسحه
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except json.JSONDecodeError:
            # إذا كان الملف فارغاً أو تالفاً، نبدأ بقائمة فارغة
            existing_data = []

    # 3. إضافة التقرير الجديد للقائمة
    existing_data.append(metrics_data)

    # 4. حفظ القائمة المحدثة في الملف بشكل مرتب (indent=4)
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)
    
    print(f"\n✅ تم حفظ تقرير العملية بنجاح في: reports/results.json")
