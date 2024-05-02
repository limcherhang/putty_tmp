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
        sqlCommand = f"SELECT name, deviceId, gatewayId FROM mgmtETL.DataETL where deviceType = 10 and deviceLogic = 1"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:10 Logic:1 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            name = rows[0]
            dId = rows[1]
            gId = rows[2]
            data_list = []
            op_list = []
            logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand = f"select ZBts, rawData from rawData.zigbee where GWts>='{st}' and GWts<'{et}' and ieee='{dId}' and  gatewayId={gId} "
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
                t = ()
                if data[0] is None: 
                    logger.warning(f"GatewayId: {gId} {dId} has No data in the minute")
                    continue
                ts = data[0]

                if data[1][:16] != '09030000000c4487':
                    continue

                rawData = data[1][16:]
                if data[0].replace(second=0) not in op_list: 
                    op_list.append(data[0].replace(second=0))
                if len(rawData) != 58: 
                    continue
            
                rawData = rawData[6:-4]
                ion = int(rawData[0:4], 16)
                pm2dot5 = int(rawData[4:8], 16)
                pm10 = int(rawData[8:12], 16)
                ch2o = int(rawData[12:16], 16)
                temp = int(rawData[16:20], 16)
                humidity = int(rawData[20:24], 16)
                co2 = int(rawData[24:28], 16)
                voc = int(rawData[28:32], 16)
                #eco = int(rawData[32:36], 16)
                #r = int(rawData[36:44], 16)
                NegPos_flag = int(rawData[44:48], 16)
                
                t = (ts, ion, pm2dot5, pm10, ch2o, temp, humidity, co2, voc, NegPos_flag)
                data_list.append(t)
            
            for op in op_list:
                negativeIon = 'NULL'
                positiveIon = 'NULL'
                for data in data_list:
                    if data[0].replace(second=0) == op:
                        logger.debug(data)
                        if data[9] == 0:
                            negativeIon = data[1]
                        elif data[9] == 1:
                            positiveIon = data[1]
                        pm2dot5 = data[2]
                        pm10 = data[3]
                        ch2o = round(data[4]/100, 3)
                        temp = round(data[5]/10 ,2)
                        humidity = round(data[6]/10, 2)
                        co2 = data[7]
                        voc = data[8] * 0.01

                value_string += f"('{op}', {gId}, '{name}', {pm2dot5}, {pm10}, {negativeIon}, {positiveIon}, {temp}, {humidity}, {ch2o}, {voc}, {co2}), "  


    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`airQuality` (`ts`, `gatewayId`, `name`, `ch1`, `ch2`, `negativeIon`, `positiveIon`, `temp`, `humidity`, `CH2O`, `VOC`, `co2`) Values {value_string}"
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
    st = (nowTime - timedelta(minutes=2)).replace(second=0)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")    
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")

