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

def sql(string,table,site,name,start,end):
	sqlCommand="""
	Select {} From dataPlatform.{}
	Where siteId={} and name='{}' and ts>='{}' and ts<'{}'
	""".format(string,table,site,name,start,end)
	return sqlCommand
	
def main(conn,start,end):
    value_string =''
    date = datetime.date(start)
    year = start.year
    with conn.cursor() as cursor:
        sqlCommand="select siteId, name from mgmtETL.DataPlatform where name like 'flow#%' and gatewayId>0"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- flow --------------- Took: {round(time.time()-s, 3)}s")
        for rows in NameList:
            sId = rows[0]
            name = rows[1]
            data_list,total_list = [],[]
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand=f"Select flowRate,total From dataPlatform.flow Where siteId={sId} and name='{name}' and ts>='{start}' and ts<'{end}' order by ts asc"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                result = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing: {sqlCommand}")
                logger.error(f"Error message: {ex}")
                result = []

            for data in result:
                if data[0] is not None: data_list.append(data[0]) #flowRate
                if data[1] is not None: total_list.append(data[1]) #flowRate

            if len(data_list) != 0:
                flowMin = round(np.percentile(np.array(data_list), 0), 2)
                flowMedian = round(np.percentile(np.array(data_list), 50), 2)
                flowMax = round(np.percentile(np.array(data_list), 100), 2)
                first_total = float(total_list[0])
                last_total = float(total_list[-1])
                waterConsumed = round(last_total-first_total,2)
                value_string += f"('{date}',{sId},'{name}',{waterConsumed},{first_total},{last_total},{flowMin},{flowMedian},{flowMax}), "

        if value_string != '':
            value_string = value_string[:-2]  
            with conn.cursor() as cursor:
                sqlCommand=f"Replace into `reportPlatform{year}`.`Dflow`(`date`,`siteId`,`name`,`waterConsumption`,`totalFirst`,`total`,`flowMin`,`flowMedian`,`flowMax`)values {value_string}"
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