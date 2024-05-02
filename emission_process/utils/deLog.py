import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import timedelta,datetime
import glob
import os ,re
import time

def deleter_log_files(root_dir):
    log_files = glob.glob(os.path.join(root_dir,'**','*.log.*'), recursive=True)
    pattern = r'\d{4}-\d{2}-\d{2}$'  
    
    if log_files:
        for log_file in log_files:
            log_time = re.findall(pattern,log_file)
            current_time = datetime.now()
            if (current_time - timedelta(days=7)).strftime('%Y-%m-%d') > log_time[0]:
                os.remove(log_file)
                print(f"Deleted old log file: {log_file}")
        return 'OK'
    else:
        return 'No log data'

print('---start---')
deleter_log_files('/home/ubuntu')
print('---end---')
