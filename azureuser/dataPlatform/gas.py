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

def getGas(conn,  gId, etlName, ts):

    if (ts-timedelta(minutes=1)).day < ts.day:
        date_from = (ts-timedelta(minutes=1)).replace(hour=0, minute=0, second=0)
        date_to = (ts-timedelta(minutes=1)).replace(hour=23, minute=59, second=0)
    else:
        date_from = ts.replace(hour=0, minute=0, second=0)
        date_to = ts - timedelta(minutes=1)

    with conn.cursor() as cursor:
      
        sqlCommand = f"select gas from dataETL.gas where gatewayId={gId} and name='{etlName}' and ts>='{date_from}' and ts<='{date_to}' order by ts desc limit 1"
        logger.debug(sqlCommand)

        try:
            cursor.execute(sqlCommand)
        except Exception as ex:
            logger.error(f"[Select ERROR]: {str(ex)}")
            return None
        
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]

def getGas0000(conn,  gId, etlName, ts):

    date_from = ts.replace(hour=0, minute=0, second=0)
    date_to  = ts.replace(hour=23, minute=59, second=0)

    with conn.cursor() as cursor:

        sqlCommand = f"select gas from dataETL.gas where gatewayId={gId} and name='{etlName}' and ts>='{date_from}' and ts<='{date_to}' order by ts asc limit 1"
        logger.debug(sqlCommand)

        try:
            cursor.execute(sqlCommand)
        except Exception as ex:
            logger.error(f"[Select ERROR]: {str(ex)}")
            return None
        
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sqlCommand = f"select siteId,gatewayId,name,dataETLName from mgmtETL.DataPlatform where name like 'gas#%' and logicModel = 'normal' and logicModel is NOT NULL"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select gas Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            sId = rows[0]
            gId = rows[1]
            name = rows[2]
            ETLName = rows[3]

            logger.info(f"--------------- select sId:{sId} gId:{gId} ETLname:{ETLName}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select ts, gas from dataETL.gas where gatewayId={gId} and name='{ETLName}' and ts>='{st}' and ts<'{et}'"
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
            else:
                for data in datas:
                    ts = data[0]
                    gas = data[1] * 10
                    gas_mmBTU = gas * 0.08348
                    
                    prev_gas = getGas(conn,  gId, ETLName, ts) * 10
                    if prev_gas is not None: 
                        gasLoad = round(gas - prev_gas, 3)
                        gasLoad_mmBTU = gasLoad * 0.08348
                    else:
                        gasLoad = 'NULL'
                        gasLoad_mmBTU = gasLoad
                    
                    gas0000 = getGas0000(conn, gId, ETLName, ts) * 10
                    if gas0000 is not None:
                        gasConsumed = round(gas - gas0000, 3)
                        gasConsumed_mmBTU = gasConsumed * 0.08348  
                    else:
                        gasConsumed = 'NULL'
                        gasConsumed_mmBTU = gasConsumed
                    
                    value_string += f"('{ts}', {sId}, '{name}', {round(gas, 3)}, {round(gasLoad, 3)}, {round(gasConsumed, 3)}, {round(gas_mmBTU, 3)}, {round(gasLoad_mmBTU, 3)}, {round(gasConsumed_mmBTU, 3)}), "
                    
    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataPlatform`.`gas` (`ts`, `siteId`, `name`, `gasInm3`, `gasLoadInm3`, `gasConsumedInm3`, `gasInmmBTU`, `gasLoadInmmBTU`, `gasConsumedInmmBTU`) Values {value_string}"
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
    st = nowTime - timedelta(minutes=5)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
