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

def getTotal(conn,gId,name,string,time):
    lastTime = datetime.date(time)
    newTime = lastTime+timedelta(days=1)
    with conn.cursor() as cursor:
        if string == 'Flow':
            sqlCommand=f"Select flowTotalPositive From dataETL.flow Where gatewayId={gId} and name='{name}' and ts>='{lastTime}' and ts<'{newTime}' order by ts asc limit 1"
        elif string == 'FlowU':
            sqlCommand=f"Select netAccumulator From dataETL.flowU Where gatewayId={gId} and name='{name}' and ts>='{lastTime}' and ts<'{newTime}' order by ts asc limit 1"
        try:
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
        sqlCommand="Select siteId, gatewayId, name, dataETLName From mgmtETL.DataPlatform Where name like 'flow#%' and logicModel='normal' and logicModel is NOT NULL"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select flow-normal Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            sId = rows[0]
            gId = rows[1]
            name = rows[2]
            ETLName = rows[3]
            string = ETLName.split('#')[0]
            
            logger.info(f"--------------- select sId:{sId} gId:{gId} ETLname:{ETLName}--------------- Took: {round(time.time()-s, 3)}s")
            if string == 'Flow':
                sqlCommand=f"select ts, flowRate, flowTotalPositive from dataETL.flow where gatewayId={gId} and name='{ETLName}' and ts>='{st}' and ts<'{et}'"
            elif string == 'FlowU':
                sqlCommand=f"select ts, flowRate, netAccumulator from dataETL.flowU where gatewayId={gId} and name='{ETLName}' and ts>='{st}' and ts<'{et}'"
                
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.warning(f"has no data in '{st}' to '{et}'")
                continue
            for data in datas:
                ts = data[0]
                flowRate = data[1]
                flowTotal = data[2]
                flowTotal0000 = getTotal(conn,gId,ETLName,string,ts)
                if flowTotal0000 == 0:
                    waterConsumed = 0
                else:
                    waterConsumed=flowTotal-flowTotal0000
                
                value_string += f"('{ts}', {sId}, '{name}',{flowRate},null,{round(waterConsumed,2)},{flowTotal}), "

    if value_string != '':    
        value_string = value_string[:-2]            
        with conn.cursor() as cursor:
            replace_sql=f"Replace into `dataPlatform`.`flow` (`ts`,`siteId`,`name`,`flowRate`,`liquidLevel`,`waterConsumed`,`total`)values{value_string}"
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
    st = nowTime - timedelta(minutes=3)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")