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
import struct
import logging

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL where deviceType = 3 and deviceLogic = 1 "
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:3 Logic:1 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for row in infos:
            gId = row[0]
            dId = row[1]
            name = row[2]

            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select ZBts,rawdata from rawData.zigbee where GWts>='{st}' and GWts<'{et}' and ieee='{dId}' and gatewayId = {gId} "
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
                ts = data[0].replace(second=0)
                rawdata = data[1]
                if len(rawdata) == 12 : #8151
                    co2ppm = int(rawdata[0:4],16)
                    co2ppm = ('null' if co2ppm == 0 else co2ppm)
                    temp = int(rawdata[4:8],16)/10
                    temp = round((temp * 10 / 65535 * 175) - 45, 2)
                    humidity = int(rawdata[8:12],16)/10
                    humidity = round((humidity * 10 / 65535 * 100), 2)
                
                elif len(rawdata) == 16 : #8157
                    co2ppm = 'null'
                    temp = struct.unpack('!f', bytes.fromhex(rawdata[:8]))[0]
                    humidity = struct.unpack('!f', bytes.fromhex(rawdata[8:16]))[0]

                value_string += f"('{ts}','{gId}','{name}',{temp},{humidity},{co2ppm}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`threeInOne` (`ts`, `gatewayId`, `name`, `temp`, `humidity`, `co2`) Values {value_string}"
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

