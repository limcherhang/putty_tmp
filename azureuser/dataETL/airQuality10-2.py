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

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sqlCommand = f"SELECT name, deviceId, gatewayId FROM mgmtETL.DataETL where  deviceType = 10 and deviceLogic = 2"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []

        logger.info(f"--------------- select Type:10 Logic:2 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            name = rows[0]
            dId = rows[1]
            gId = rows[2]
            
            if dId != '00124b0019309bf5': continue

            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select ZBts, rawData from rawData.zigbee where GWts>='{st}' and GWts<'{et}' and ieee='{dId}' and gatewayId={gId}"
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
                rawData = data[1][6:-4]
                if data[1][:16] != '010300000001840a':
                    continue
                rawData = rawData[16:]
                co = int(rawData, 16)
            
                value_string += f"('{ts}', {gId}, '{name}', {co}), "  

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`airQuality` (`ts`, `gatewayId`, `name`, `CO`) Values {value_string}"
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
    st = nowTime - timedelta(minutes=2)
    et = nowTime 
    main(conn,st,et)
    conn.close()    
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
