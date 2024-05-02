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
        sqlCommand = f"select siteId, name from mgmtETL.DataPlatform where name like 'gas#%' and gatewayId>0"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- gas --------------- Took: {round(time.time()-s, 3)}s")
        for rows in NameList:
            sId = rows[0]
            name = rows[1]
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select gasInm3, gasInmmBTU from dataPlatform.gas where siteId={sId} and name='{name}' and ts>='{start}' and ts<'{end}' order by ts asc limit 1"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                first=cursor.fetchone()
            except Exception as ex:
                logger.error(f"Error executing: {sqlCommand}")
                logger.error(f"Error message: {ex}")
                first = None
            if first is None:
                logger.warning(f"{name} in {date} has no data")
                continue
            else:
                sqlCommand = f"select gasInm3, gasInmmBTU from dataPlatform.gas where siteId={sId} and name='{name}' and ts>='{start}' and ts<'{end}' order by ts desc limit 1"
                try:
                    logger.debug(sqlCommand)
                    cursor.execute(sqlCommand)
                    last = cursor.fetchone()
                except Exception as ex:
                    logger.error(f"Error executing {sqlCommand}")
                    logger.error(f"Error message: {ex}")
                    continue
                gas_m3_first = ('null' if first[0] is None else round(float(first[0]),3))
                gas_mmBTU_first = ('null' if first[1] is None else round(float(first[1]),3))
                gas_m3 = ('null' if last[0] is None else round(float(last[0]),3))
                gas_mmBTU = ('null' if last[1] is None else round(float(last[1]),3))
                gasConsumption_m3 = gas_m3-gas_m3_first
                gasConsumption_mmBTU = gas_mmBTU-gas_mmBTU_first

                value_string += f"('{date}', {sId}, '{name}', {gasConsumption_m3}, {gas_m3_first}, {gas_m3}, {gasConsumption_mmBTU}, {gas_mmBTU_first}, {gas_mmBTU}), "

        if value_string != '':
            value_string = value_string[:-2]
            with conn.cursor() as cursor:
                replace_sql = f"replace into `reportPlatform{year}`.`Dgas` (`date`, `siteId`, `name`, `gasConsumptionInm3`,`totalInm3First`, `totalInm3`, `gasConsumptionInmmBTU`,`totalInmmBTUFirst`,`totalInmmBTU`) Values {value_string}"
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
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