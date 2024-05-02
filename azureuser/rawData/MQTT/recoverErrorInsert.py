import os
import sys
rootPath = os.getcwd()+'/../../'
sys.path.append(rootPath)

import configparser
import datetime
import queue
import time
import logging

from connection.mysql_connection import MySQLConn
from utils import myLog

if __name__ == "__main__":
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()
    
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"], level_db=logging.ERROR)
    conn = MySQLConn(config['mysql_azureV2'])

    nowTime = datetime.datetime.now().replace(microsecond=0)
    startTime = nowTime-datetime.timedelta(hours=1)

    with conn.cursor() as cursor:
        sqlCommand = f"""
            SELECT message FROM logETL.logEntries WHERE fileName LIKE 'MQTT%' AND level='ERROR' 
            -- AND ts  >='{startTime}' 
            AND ts < '{nowTime}'
        """
        cursor.execute(sqlCommand)
        for (res,) in cursor.fetchall():
            if "INSERT" in res or "REPLACE" in res:
                sqlCommand = res[8:].replace("INSERT", "REPLACE")
                if "'{" in sqlCommand:
                    sqlCommand = sqlCommand.replace("'{", "{").replace("}'", "}")
                try:
                    logger.debug(sqlCommand)
                    cursor.execute(sqlCommand)
                    logger.info("Insert Succeed!")
                except Exception as ex:
                    logger.error(f"ERROR in {sqlCommand}")
                    logger.error(f"Error message: {ex}")