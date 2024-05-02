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

def main(conn,start,end):
    value_string = ''
    date = datetime.date(start)
    year = start.year
    with conn.cursor() as cursor:
        sqlCommand="select siteId, name from mgmtETL.DataPlatform where name like 'power#%' and gatewayId>0 "
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- power --------------- Took: {round(time.time()-s, 3)}s")
        for rows in NameList:
            sId=rows[0]
            name=rows[1]
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand=f"SELECT ts,total FROM dataPlatform.power where siteId={sId} and name='{name}' and ts>='{start}' and ts < '{end}' order by ts asc limit 1"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                first=cursor.fetchone()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                first = None
        
            if first is None:
                continue
            else:
                sqlCommand=f"SELECT ts,total FROM dataPlatform.power where siteId={sId} and name='{name}' and ts>='{start}' and ts < '{end}' order by ts desc limit 1"
                try:
                    logger.debug(sqlCommand)
                    cursor.execute(sqlCommand)
                    last=cursor.fetchone()
                except Exception as ex:
                    logger.error(f"Error executing {sqlCommand}")
                    logger.error(f"Error message: {ex}")
                    continue
                first_total= ('NULL' if first[1] is None else first[1])
                last_total=('NULL' if last[1] is None else last[1])
                logger.debug(f"{last_total}-{first[1]}")
                if first_total == 'NULL' or last_total == 'NULL':
                    energyConsumption = 'NULL'
                else:
                    energyConsumption=last_total-first_total
                
                value_string += f"('{date}',{sId},'{name}',{energyConsumption},{first_total},{last_total}), "

    if value_string != '':
        value_string = value_string[:-2]

        with conn.cursor() as cursor:
            sqlCommand=f"Replace into `reportPlatform{year}`.`Dpower` (`date`,`siteId`,`name`,`energyConsumption`,`totalFirst`,`total`)values{value_string}"
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            logger.info(f"--------------- replace to {conn.host} success --------------- Took: {round(time.time()-s, 3)}s")              
          
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
    main(conn,start,end)
    conn.close()
    logger.info(f"{conn.host} close")   
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")