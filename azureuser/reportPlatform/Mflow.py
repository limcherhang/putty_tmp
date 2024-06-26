from datetime import datetime, timedelta
import time
import os
from pathlib import Path
import sys , os
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser


def main(conn,start):
    value_string = ''
    date = datetime.date(start)
    year = start.year
    month = start.month
    with conn.cursor() as cursor:
        sqlCommand="select siteId, name from mgmtETL.DataPlatform where name like 'flow#%' and gatewayId>0 "
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        for data in NameList:
            sId = data[0]
            name = data[1]
            logger.info(f"--------------- flow --------------- Took: {round(time.time()-s, 3)}s")
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand=f"SELECT totalFirst FROM reportPlatform{year}.Dflow where month(date) = {month} and siteId = {sId} and name = '{name}' order by date asc limit 1; "
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                first = cursor.fetchone()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                first = None

            if first is None:
                logger.warning(f"{name} in {date} has no data")
                continue
            totalFirst = first[0]
            
            sqlCommand=f"SELECT total FROM reportPlatform{year}.Dflow where month(date) = {month} and siteId = {sId} and name = '{name}' order by date desc limit 1 ; "
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                last = cursor.fetchone()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                last = None

            if last is None:
                logger.warning(f"{name} in {date} has no data")
                continue
            total = last[0]

            waterConsumption = total - totalFirst 
            value_string += f"({month},'{date}',{sId},'{name}',{waterConsumption},{totalFirst},{total}), "
                
    if value_string != '':
        with conn.cursor() as cursor:
            sql = f"REPLACE INTO reportPlatform{year}.Mflow(month,updateDate,siteId,name,waterConsumption,totalFirst,total) VALUES {value_string[:-2]}"
            cursor.execute(sql)
            logger.debug(sql)

if __name__ == '__main__':
    s = time.time()
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')
    conn = MySQLConn(config['mysql_azureV2'])
    logger = myLog.get_logger(os.getcwd(), f"{filename}.log",config['mysql_azureV2'])
    nowTime = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
    logger.info(f"---------- Program Starts! : {nowTime} ---------- ")
    start = nowTime-timedelta(days=1)
    end = nowTime
    main(conn,start)
    conn.close()
    logger.info(f"{conn.host} close")   
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")