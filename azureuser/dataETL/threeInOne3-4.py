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
import json
import logging

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL where deviceType = 3 and deviceLogic = 4 "
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:3 Logic:4 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for row in infos:
            gId = row[0]
            dId = row[1]
            name = row[2]
            sqlCommand = f"select APIts, rawdata from rawData.sigfoxAPI where APIts>='{st}' and APIts<'{et}' and  gatewayId='{gId}' and deviceId='{dId}' order by APIts asc"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.info(f'{row} has no data')
                continue
            else:
                for data in datas:
                    ts = data[0]
                    rawdata = json.loads(data[1])
                    rawdata = rawdata['data']
                    
                    if len(rawdata) == 12:
                        temp = util.signed_hex2dec(rawdata[2:6]) 
                        h = util.signed_hex2dec(rawdata[6:8]) 
                        co2 = util.signed_hex2dec(rawdata[8:12]) 
                        temp = ('NULL' if temp is None else (float(temp)/10))
                        h = ('NULL' if h is None else float(h))
                        co2 = ('NULL' if co2 is None else (float(co2)))
                        value_string += f"('{ts}','{gId}','{name}', {temp}, {h}, {co2}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`threeInOne` (ts, gatewayId, name, temp, humidity, co2) Values {value_string}"
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
    st = nowTime - timedelta(minutes=20)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")    
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")