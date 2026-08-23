import os
import uuid
import sys

# استدعاء الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SMALL_FILE_THRESHOLD_MB, SAMPLE_CSV_PATH, HUGE_CSV_PATH

def route_file(file_path):
    """
    يفحص حجم الملف ويقرر محرك المعالجة المناسب، وينشئ id_run فريد.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return None

    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    id_run = str(uuid.uuid4())
    
    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = f"File size ({file_size_mb:.2f} MB) is <= threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
    else:
        engine = "pyspark"
        reason = f"File size ({file_size_mb:.2f} MB) exceeds threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
        
    print("\n--- File Routing Info ---")
    print(f"Run ID: {id_run}")
    print(f"File Size: {file_size_mb:.2f} MB")
    print(f"Selected Engine: {engine}")
    print(f"Reason: {reason}")
    print("-------------------------\n")
    
    return {
        "id_run": id_run,
        "file_path": file_path,
        "file_size_mb": file_size_mb,
        "engine": engine
    }

if __name__ == "__main__":
    # بمجرد الضغط على Run، سيتم اختبار الموجه على ملف العينة
    print("Running File Router on the sample file...")
    route_file(SAMPLE_CSV_PATH)
    
    # يمكنك تبديل SAMPLE_CSV_PATH بـ HUGE_CSV_PATH واختباره أيضاً متى شئت