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
    date = datetime.date(start)
    year = start.year
    month = start.month
    value_string_D = ''
    value_string_M = ''
    
    #Daily
    with conn.cursor() as cursor:
        sql = f"select siteId, name from mgmtETL.DataPlatform where name like 'car#%' and gatewayId>0"
        try:
            logger.debug(sql)
            cursor.execute(sql)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- ammonia --------------- Took: {round(time.time()-s, 3)}s")
        for rows in NameList:
            sId = rows[0]
            name = rows[1]
        
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sql_2 = f"SELECT mileage,distance,fuelConsumption,totalFuelConsumption FROM dataPlatform.car where name = '{name}' and ts >= '{start}' and ts < '{end}' and siteId = {sId} order by ts desc limit 1"
            try:
                logger.debug(sql_2)
                cursor.execute(sql_2)
                result = cursor.fetchone()
            except Exception as ex:
                logger.error(f"Error executing{sql_2}")
                logger.error(f"Error message: {ex}")
                result = ()
            if len(result) == 0:
                continue
            for row in result:
                mileage = row[0]
                distance = row[1]
                fuelConsumption = ('null'if row[2] is None else row[2])
                totalFuelConsumption =('null' if row[3] is None else row[3]) 

                value_string_D += f"('{date}',{sId},'{name}',{mileage},{distance},{fuelConsumption},{totalFuelConsumption}), "    
                    
        if value_string_D != '':
            value_string_D = value_string_D[:-2]
            replace_sql = f"replace into reportPlatform{year}.Dcar(ts,siteId,name,mileage,distance,fuelConsumption,totalFuelConsumption) Values {value_string_D}"
            try:
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
                logger.info(f"--------------- replace to {conn.host} Dcar success --------------- Took: {round(time.time()-s, 3)}s")
            except Exception as ex:
                logger.error(f"Error executing {replace_sql}")
                logger.error(f"Error message: {ex}")                
                    
    #Month
        sql = f"select name, max(mileage),sum(distance),sum(fuelConsumption),sum(totalFuelConsumption) from reportPlatform{year}.Dcar where month(ts) = {month} group by name"
        try:
            logger.debug(sql)
            cursor.execute(sql)
            result_month = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            result_month = ()
        for Mdata in result_month:
            M_name = Mdata[0]
            M_mileage = Mdata[1]
            M_distance = Mdata[2]
            M_fuelConsumption = ('null'if Mdata[3] is None else Mdata[3])
            M_totalFuelConsumption = ('null' if Mdata[4] is None else Mdata[4]) 
            
            value_string_M += f"({month},'{date}',{sId},'{M_name}',{M_mileage},{M_distance},{M_fuelConsumption},{M_totalFuelConsumption}), "

        if value_string_M != '':
            value_string_M = value_string_M[:-2]

            replace_sql = f"replace into reportPlatform{year}.Mcar(month,updateDate,siteId,name,mileage,distance,fuelConsumption,totalFuelConsumption) Values {value_string_M}"
            try:
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
                logger.info(f"--------------- replace to {conn.host} Mcar success --------------- Took: {round(time.time()-s, 3)}s")
            except Exception as ex:
                logger.error(f"Error executing {replace_sql}")
                logger.error(f"Error message: {ex}")
                        
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
