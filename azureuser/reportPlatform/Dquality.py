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
import numpy as np

def main(conn,start,end):
    value_string = ''
    date = datetime.date(start)
    year = start.year
    with conn.cursor() as cursor:
        sqlCommand = f"select siteId, name from mgmtETL.DataPlatform where name like 'quality#%' and gatewayId>0"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- quality --------------- Took: {round(time.time()-s, 3)}s")
        for rows in NameList:
            sId = rows[0]
            name = rows[1]
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select pH, ORP, TDS, EC from dataPlatform.quality where siteId={sId} and name='{name}' and ts>='{start}' and ts<'{end}' order by ts asc"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                result = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing: {sqlCommand}")
                logger.error(f"Error message: {ex}")
                result = []

            if len(result) == 0:
                logger.warning(f"SiteId: {sId} has no data on {date}")
                continue
            else:
                ph_list = []
                orp_list = []
                tds_list = []
                ec_list = []
                phMin, phMedian, phMax, orpMin, orpMedian, orpMax, tdsMin, tdsMedian, tdsMax, ecMin, ecMedian, ecMax = 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL'
                for data in result:
                    if data[0] is not None:
                        ph_list.append(data[0])
                    if data[1] is not None:
                        orp_list.append(data[1])
                    if data[2] is not None:
                        tds_list.append(data[2])
                    if data[3] is not None:
                        ec_list.append(data[3])
                
                if len(ph_list) != 0:
                    phMin = round(np.percentile(np.array(ph_list), 0), 2)
                    phMedian = round(np.percentile(np.array(ph_list), 50), 2)
                    phMax = round(np.percentile(np.array(ph_list), 100), 2)
                if len(orp_list) != 0:
                    orpMin = round(np.percentile(np.array(orp_list), 0), 2)
                    orpMedian = round(np.percentile(np.array(orp_list), 50), 2)
                    orpMax = round(np.percentile(np.array(orp_list), 100), 2)
                if len(tds_list) != 0:
                    tdsMin = round(np.percentile(np.array(tds_list), 0), 2)
                    tdsMedian = round(np.percentile(np.array(tds_list), 50), 2)
                    tdsMax = round(np.percentile(np.array(tds_list), 100), 2)
                if len(ec_list) != 0:
                    ecMin = round(np.percentile(np.array(ec_list), 0), 2)
                    ecMedian = round(np.percentile(np.array(ec_list), 50), 2)
                    ecMax = round(np.percentile(np.array(ec_list), 100), 2)

                value_string += f"('{date}', {sId}, '{name}', {phMin}, {phMedian}, {phMax}, {orpMin}, {orpMedian}, {orpMax}, {tdsMin}, {tdsMedian}, {tdsMax}, {ecMin}, {ecMedian}, {ecMax}), "


    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `reportPlatform{year}`.`Dquality` (`date`, `siteId`, `name`, `pHMin`, `pHMedian`, `pHMax`, `ORPMin`, `ORPMedian`, `ORPMax`, `TDSMin`, `TDSMedian`, `TDSMax`, `ECMin`, `ECMedian`, `ECMax`) Values {value_string}"
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