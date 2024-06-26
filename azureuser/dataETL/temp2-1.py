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
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL where deviceType = 2 and deviceLogic = 1 "
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:2 Logic:1 Mapping --------------- Took: {round(time.time()-s, 3)}s")
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
                ts = data[0].replace(second = 0)
                rawdata = data[1]
                if len(rawdata) == 32 :
                    temp1 = struct.unpack('!f', bytes.fromhex(rawdata[:8]))[0]
                    temp2 = struct.unpack('!f', bytes.fromhex(rawdata[8:16]))[0]
                    temp3 = struct.unpack('!f', bytes.fromhex(rawdata[16:24]))[0]
                    temp4 = struct.unpack('!f', bytes.fromhex(rawdata[24:32]))[0]
                
                elif len(rawdata) == 8 :
                    temp3 = int(rawdata[5:8],16) /100
                    if(rawdata[4:5] == '0'):
                        temp3 *= -1
                    temp2 = int(rawdata[:4],16)
                    temp1 = temp2 / 64 - 256 + temp3
                    temp4 = 'null'

                value_string += f"('{ts}', {gId}, '{name}', {temp1}, {temp2}, {temp3}, {temp4}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`temp` (`ts`, `gatewayId`, `name`, `temp1`, `temp2`, `temp3`, `temp4`) Values {value_string}"
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

