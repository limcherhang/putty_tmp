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
import logging

def getTotal(conn,name,st):
    stime = datetime.date(st)
    etime = stime+timedelta(days=1)
    with conn.cursor() as cursor:
        sqlCommand=f"select total from dataPlatform.power where ts >= '{stime}' and ts < '{etime}' and name = '{name}'  and siteId = 83 order by ts asc limit 1"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            data=cursor.fetchone()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            data = None
        if data is None:
            total = 0
        else:
            total = data[0]
    return total

def main(conn,st,et):
    value_string = ''   
    with conn.cursor() as cursor:
        sqlCommand = f"SELECT siteId,gatewayId,name,dataETLName FROM mgmtETL.DataPlatform where siteId = 83 and name like 'power#%' and logicModel = 'normal' ;"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select sigfox Power Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            sId = rows[0]
            gId = rows[1]
            name = rows[2]
            etlName = rows[3]
            logger.info(f"--------------- select sId:{sId} gId:{gId} ETLname:{etlName}--------------- Took: {round(time.time()-s, 3)}s")
            if name == 'power#14':
                sqlCommand = f"select ts,ch1Watt,totalNegative from dataETL.power where ts>='{st}' and ts<'{et}' and name = '{etlName}' and gatewayId = '{gId}' order by ts asc"
            else:
                sqlCommand = f"select ts,ch1Watt,total from dataETL.power where ts>='{st}' and ts<'{et}' and name = '{etlName}' and gatewayId = '{gId}' order by ts asc"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.info(f'{name} has no data')
            else:
                total_last = getTotal(conn,name,st)
                for data in datas:
                    ts = data[0]
                    ch1watt = (0 if data[1] is None else (float(data[1])))
                    total = (0 if data[2] is None else (float(data[2])))/1000

                    if total_last == 0:
                        energy = 0
                        total_last = total
                    else :
                        energy = total-total_last

                    power = ch1watt/1000
                    value_string += f"('{ts}',{sId},'{name}',{ch1watt},{power}, {energy}, {total}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataPlatform`.`power` (`ts`, `siteId`, `name`,`ch1Watt`, `powerConsumed`,`energyConsumed`, `total`) Values {value_string}"
            try:
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
                logger.info(f"--------------- replace to {conn.host} success --------------- Took: {round(time.time()-s, 3)}s")
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
    logger = myLog.get_logger(os.getcwd(), f"{filename}.log",config['mysql_azureV2'], level=logging.ERROR)

    nowTime = datetime.now().replace(microsecond=0)
    logger.info(f"---------- Program Starts! : {nowTime} ---------- ")
    st = nowTime - timedelta(minutes=20)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")   
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
