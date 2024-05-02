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
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 1 and deviceLogic = 8" #要改
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:1 Logic:8 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            gId = rows[0]
            dId = rows[1]
            name = rows[2]
            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"""select receivedSync, ch1Watt, ch2Watt, ch3Watt, totalPositiveWattHour, totalNegativeWattHour, 
            ch1Current, ch2Current, ch3Current, ch1Voltage, ch2Voltage, ch3Voltage, ch1PowerFactor, ch2PowerFactor, 
            ch3PowerFactor, voltage12, voltage23, voltage31, ch1Hz, ch2Hz, ch3Hz, i1THD, i2THD, i3THD, v1THD, v2THD, v3THD 
            from iotmgmt.pm where ts >= '{st}' and ts < '{et}' and ieee = '{dId}' and gatewayId = {gId}
            """
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
                    watt1 = ('null' if data[1] is None else round(float(data[1])/1000,3))
                    watt2 = ('null' if data[2] is None else round(float(data[2])/1000,3))
                    watt3 =('null' if data[3] is None else round(float(data[3])/1000,3))
                    total = ('null' if data[4] is None else round(float(data[4])/1000,3))
                    totalNegative = ('null' if data[5] is None else round(float(data[5])/1000,3))
                    ch1Current = ('null' if data[6] is None else round(float(data[6]),3))
                    ch2Current = ('null' if data[7] is None else round(float(data[7]),3))
                    ch3Current = ('null' if data[8] is None else round(float(data[8]),3))
                    ch1Voltage = ('null' if data[9] is None else round(float(data[9]),3))
                    ch2Voltage = ('null' if data[10] is None else round(float(data[10]),3))
                    ch3Voltage = ('null' if data[11] is None else round(float(data[11]),3))
                    ch1PowerFactor = ('null' if data[12] is None else round(float(data[12]),3))
                    ch2PowerFactor = ('null' if data[13] is None else round(float(data[13]),3))
                    ch3PowerFactor = ('null' if data[14] is None else round(float(data[14]),3))
                    voltage12 = ('null' if data[15] is None else round(float(data[15]),3))
                    voltage23 = ('null' if data[16] is None else round(float(data[16]),3))
                    voltage31 = ('null' if data[17] is None else round(float(data[17]),3))
                    ch1Hz = ('null' if data[18] is None else round(float(data[18]),3))
                    ch2Hz = ('null' if data[19] is None else round(float(data[19]),3))
                    ch3Hz = ('null' if data[20] is None else round(float(data[20]),3))
                    i1THD = ('null' if data[21] is None else round(float(data[21]),3))
                    i2THD = ('null' if data[22] is None else round(float(data[22]),3))
                    i3THD = ('null' if data[23] is None else round(float(data[23]),3))
                    v1THD = ('null' if data[24] is None else round(float(data[24]),3))
                    v2THD = ('null' if data[25] is None else round(float(data[25]),3))
                    v3THD = ('null' if data[26] is None else round(float(data[26]),3))

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