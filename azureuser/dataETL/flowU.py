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
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 6 and deviceLogic = 1"
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []

        logger.info(f"--------------- select Type:6 Logic:1 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for row in infos:
            gId = row[0]
            dId = row[1]
            name = row[2]
            
            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"SELECT ZBts,rawdata from rawData.zigbee where ieee='{dId}' and gatewayId = {gId} and GWts >= '{st}' and GWts < '{et}'"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {sqlCommand}")
                datas = []
            if len(datas) == 0:
                logger.warning(f"{name} has no data at {nowTime}")
                continue
            else:
                for data in datas:
                    ts = data[0]
                    rawdata = data[1]
                    
                    if len(rawdata) != 96:
                        continue
                    flowRate = struct.unpack('!f', bytes.fromhex(rawdata[:8]))[0]
                    velocity = struct.unpack('!f', bytes.fromhex(rawdata[8:16]))[0]
                    netAccumulator = int(rawdata[16:24],16)
                    temp1Inlet = struct.unpack('!f', bytes.fromhex(rawdata[24:32]))[0]
                    temp2Outlet = struct.unpack('!f', bytes.fromhex(rawdata[32:40]))[0]
                    errorCode = rawdata[40:44]
                    signalQuality = int(rawdata[44:48],16)
                    upstreamStrength = int(rawdata[48:52],16)
                    downstreamStrength = int(rawdata[52:56],16)
                    calcRateMeasTravelTime = struct.unpack('!f', bytes.fromhex(rawdata[56:64]))[0]
                    reynoldsNumber = struct.unpack('!f', bytes.fromhex(rawdata[64:72]))[0]
                    pipeReynoldsFactor = struct.unpack('!f', bytes.fromhex(rawdata[72:80]))[0]
                    totalWorkingTime = int(rawdata[80:88],16)
                    totalPowerOnOffTime = int(rawdata[88:96],16)
                    
                    value_string += f"('{ts}','{gId}','{name}', {flowRate}, {velocity}, {netAccumulator}, {temp1Inlet}, {temp2Outlet}, '{errorCode}', {signalQuality}, {upstreamStrength}, {downstreamStrength}, {calcRateMeasTravelTime}, {reynoldsNumber}, {pipeReynoldsFactor}, {totalWorkingTime}, {totalPowerOnOffTime}), "
            
    

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`flowU` (ts,gatewayId,name,flowRate,velocity,netAccumulator,temp1Inlet,temp2Outlet,errorCode,signalQuality,upstreamStrength,downstreamStrength,calcRateMeasTravelTime,reynoldsNumber,pipeReynoldsFactor,totalWorkingTime,totalPowerOnOffTime) Values {value_string}" #要改
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
