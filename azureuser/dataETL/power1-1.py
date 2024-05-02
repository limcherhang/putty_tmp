from datetime import datetime, timedelta
import time
import os
from pathlib import Path
import sys , os
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog, util
from connection.mysql_connection import MySQLConn
import configparser
import logging

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 1 and deviceLogic = 1" #要改
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:1 Logic:1 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            gId = rows[0]
            dId = rows[1]
            name = rows[2]
            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select ZBts,rawData,clusterId from rawData.zigbee where GWts >= '{st}' and GWts < '{et}' and ieee = '{dId}' and gatewayId = {gId}  "
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
                    rawdata = data[1]
                    cId = data[2]
                    if cId != '8150' :
                        continue
                    watt1 = round(util.signed_hex2dec(rawdata[0:8])/1000,3) #w->kw
                    watt2 = round(util.signed_hex2dec(rawdata[8:16])/1000,3) #w->kw
                    watt3 = round(util.signed_hex2dec(rawdata[16:24])/1000,3) #w->kw
                    total = round(util.signed_hex2dec(rawdata[24:32])*100/1000,3) # wh->kwh
                    totalNegative = util.signed_hex2dec(rawdata[32:40])*100
                    ch1Current = util.signed_hex2dec(rawdata[40:44])/10
                    ch2Current = util.signed_hex2dec(rawdata[44:48])/10
                    ch3Current = util.signed_hex2dec(rawdata[48:52])/10
                    ch1Voltage = util.signed_hex2dec(rawdata[52:56])/10
                    ch2Voltage = util.signed_hex2dec(rawdata[56:60])/10
                    ch3Voltage = util.signed_hex2dec(rawdata[60:64])/10
                    ch1PowerFactor = util.signed_hex2dec(rawdata[64:68])/1000
                    ch2PowerFactor = util.signed_hex2dec(rawdata[68:72])/1000
                    ch3PowerFactor = util.signed_hex2dec(rawdata[72:76])/1000
                    voltage12 = util.signed_hex2dec(rawdata[76:80])/10
                    voltage23 = util.signed_hex2dec(rawdata[80:84])/10
                    voltage31 = util.signed_hex2dec(rawdata[84:88])/10
                    ch1Hz = util.signed_hex2dec(rawdata[88:92])/100
                    ch2Hz = util.signed_hex2dec(rawdata[92:96])/100
                    ch3Hz = util.signed_hex2dec(rawdata[96:100])/100
                    i1THD = util.signed_hex2dec(rawdata[100:104])/10
                    i2THD = util.signed_hex2dec(rawdata[104:108])/10
                    i3THD = util.signed_hex2dec(rawdata[108:112])/10
                    v1THD = util.signed_hex2dec(rawdata[112:116])/10
                    v2THD = util.signed_hex2dec(rawdata[116:120])/10
                    v3THD = util.signed_hex2dec(rawdata[120:124])/10 

                    value_string += ( f"('{ts}','{gId}','{name}',{watt1},{watt2},{watt3},{total},{totalNegative},{ch1Current},{ch2Current},{ch3Current},{ch1Voltage},{ch2Voltage},{ch3Voltage},{ch1PowerFactor},{ch2PowerFactor},{ch3PowerFactor},{voltage12},{voltage23},{voltage31},{ch1Hz},{ch2Hz},{ch3Hz},{i1THD},{i2THD},{i3THD},{v1THD},{v2THD},{v3THD}), ")

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into dataETL.power(ts, gatewayId, name, ch1Watt, ch2Watt, ch3Watt, total, totalNegative, ch1Current, ch2Current, ch3Current, ch1Voltage, ch2Voltage, ch3Voltage, ch1PowerFactor, ch2PowerFactor, ch3PowerFactor, voltage12, voltage23, voltage31, ch1Hz, ch2Hz, ch3Hz, i1THD, i2THD, i3THD, v1THD, v2THD, v3THD)  Values {value_string}" 
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