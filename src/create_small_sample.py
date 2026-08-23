import os
import sys

# استدعاء المسارات من ملف الإعدادات
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import HUGE_CSV_PATH, SAMPLE_CSV_PATH, SAMPLE_ROWS

def create_sample(input_path, output_path, num_rows):
    """
    يستخرج عينة من ملف CSV ضخم دون تحميله بالكامل في الذاكرة.
    """
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' not found.")
        return

    try:
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            
            header = infile.readline()
            if header:
                outfile.write(header)
            
            count = 0
            for line in infile:
                if count >= num_rows:
                    break
                outfile.write(line)
                count += 1
                
        print(f"Successfully created sample with {count} rows at '{output_path}'.")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("Starting sample extraction...")
    create_sample(HUGE_CSV_PATH, SAMPLE_CSV_PATH, SAMPLE_ROWS)