import os
import re
import logging
import pymysql
import glob
import datetime
from logging.handlers import TimedRotatingFileHandler
import time

                 
#確認是否存在log資料夾    
def dir(log_dir: str):

    try:
        os.makedirs(log_dir+'/log')
    except:
        pass
        
def get_logger(logPath: str, filename: str, level: str=logging.DEBUG):
    
    dir(logPath)
    logfile = f"{logPath}/log/{filename}"
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = TimedRotatingFileHandler(logfile , when='midnight', backupCount=2)
    formatter = CustomFormatter(
        '%(asctime)s %(name)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'   
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    user_home_dir = os.path.expanduser("~")

    return logger

class CustomFormatter(logging.Formatter):
    def format(self, record):
        timestamp = time.time()
        microsecond = datetime.datetime.now().microsecond
        timestamp_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        message = record.getMessage()  # 獲取日誌訊息
        return f'{timestamp_str}.{microsecond:06d} {record.name} [{record.levelname}] {message}'

class DatabaseLogHandler(logging.Handler):
    def __init__(self, db_env, path, filename, level=logging.DEBUG):
        self.filters = []
        self.lock = ''
        self.db_env = db_env
        self.path = path
        self.level = level
        self.filename = filename

    def emit(self, record):
        ts = datetime.datetime.fromtimestamp(record.created)
        levelname = record.levelname
        message = record.getMessage()

        # cursor = self.conn.cursor()
        # cursor.execute(f"""INSERT INTO logETL.logEntries(ts, fileName, path, level, message) VALUES ('{ts}', '{self.filename}', '{self.path}', '{levelname}', "{message.replace('"', "'")}")""")

        # cursor.close()
        write_log_to_database(ts, self.filename, self.path, levelname, self.db_env, message)

def write_log_to_database(ts, filename, path, levelname, db_env, message):
    try:
        conn = pymysql.connect(
            host=db_env['host'],
            port=int(db_env['port']),
            user=db_env['user'],
            password=db_env['password'],
            charset='utf8mb4',
        )
        cursor = conn.cursor()
        cursor.execute(f"""INSERT INTO logETL.logEntries(ts, fileName, path, level, message) VALUES ('{ts}', '{filename}', '{path}', '{levelname}', "{message.replace('"', "'")}")""")

        conn.commit()
        cursor.close()
        conn.close()
    except pymysql.Error as error:
        print(f"Error: {error}")

if __name__ == '__main__':                
    print('---start---')   
    #dir('/home/ubuntu/logPY') 
    #process_log_files('/home/ubuntu/kc')
    print('---end---')
