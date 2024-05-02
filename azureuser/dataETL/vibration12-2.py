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
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL where deviceType = 12 and deviceLogic = 2" 
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:12 Logic:2 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for row in infos:
            gId = row[0]
            dId = row[1]
            name = row[2]
            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select receivedSync,responseData from iotmgmt.zigbeeRawModbus where ts >= '{st}' and ts < '{et}' and ieee = '{dId}' and gatewayId = {gId}  "    
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.warning(f"{name} has no data at {nowTime}")
                continue
            else:
                for data in datas:
                    ts = data[0]
                    rawdata = data[1]
                    xRMS = (int(rawdata[7:10],16))*0.00390625
                    yRMS = (int(rawdata[11:14],16))*0.00390625
                    zRMS = (int(rawdata[15:18],16))*0.00390625
                    xPeak = (int(rawdata[19:22],16))*0.00390625
                    yPeak = (int(rawdata[23:26],16))*0.00390625
                    zPeak = (int(rawdata[27:30],16))*0.00390625
                    
                    value_string += f"('{ts}', {gId}, '{name}', {xRMS} , {yRMS}, {zRMS}, {xPeak}, {yPeak}, {zPeak}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`vibration` (`ts`, `gatewayId`, `name`, `xRMS`, `yRMS`, `zRMS`,  `xPeak`, `yPeak`, `zPeak`) Values {value_string}"
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