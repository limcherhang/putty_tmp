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

def getTotal(conn, gId, name, time):
    start = datetime.date(time)
    end = start+timedelta(days=1)
    with conn.cursor() as cursor:
        sqlCommand=f"select total from dataETL.power where gatewayId={gId} and name='{name}' and ts>='{start}' and ts<'{end}' order by ts asc limit 1"
        try:
            cursor.execute(sqlCommand)
            data=cursor.fetchone()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            data = None
        if data is None:
            total0000=None
        else:
            total0000=data[0]
    return total0000

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sqlCommand = f"SELECT siteId,gatewayId,name,dataETLName FROM mgmtETL.DataPlatform where siteId not in (87,82,83) and name like 'power#%' and logicModel = 'normal' and logicModel is NOT NULL ;"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        for rows in infos:
            sId=rows[0]
            gId=rows[1]
            name=rows[2]
            ETLName=rows[3]
            
            
            logger.info(f"--------------- select GatewayId:{gId} siteId:{sId} Name:{ETLName}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand=f"select ts, ch1Watt, ch2Watt, ch3Watt, total, ch1Current, ch2Current, ch3Current from dataETL.power where gatewayId={gId} and name='{ETLName}' and ts>='{st}' and ts<'{et}'"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.warning(f"{name} has no data in {st} to {et}") 
                continue
            else:
                for data in datas:
                    ts=data[0]
                    ch1Watt=(0 if data[1] is None else data[1])
                    ch2Watt=(0 if data[2] is None else data[2])
                    ch3Watt=(0 if data[3] is None else data[3])
                    powerConsumed=(ch1Watt+ch2Watt+ch3Watt)
                    
                    if data[4] is None:    
                        energyConsumed='NULL'
                        total='NULL'
                    else:
                        total = data[4]
                        total0000 = getTotal(conn,gId,ETLName,ts)
                        
                        
                        if total0000 is None:
                            energyConsumed=0
                        else:
                            energyConsumed=(total-total0000)

                    
                    ch1Current=(0 if data[5] is None else data[5])
                    ch2Current=(0 if data[6] is None else data[6])
                    ch3Current=(0 if data[7] is None else data[7])
                    value_string += f"('{ts}', {sId}, '{name}', {ch1Watt}, {ch2Watt}, {ch3Watt},{powerConsumed}, {energyConsumed}, {total}, {ch1Current}, {ch2Current}, {ch3Current}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql=f"replace into `dataPlatform`.`power`(`ts`, `siteId`, `name`, `ch1Watt`, `ch2Watt`, `ch3Watt`,`powerConsumed`, `energyConsumed`, `total`,`ch1Current`, `ch2Current`, `ch3Current`) Values {value_string}"
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
    st = nowTime - timedelta(minutes=15)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")