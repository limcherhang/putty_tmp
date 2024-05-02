from datetime import datetime, timedelta
import time
import os,json
from pathlib import Path
import sys , os
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser
import logging

def main(conn,st,et):
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name,columns FROM mgmtETL.DataETL_BACnet  where deviceType = 1 and deviceLogic = 7" 
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select Type:1 Logic:7 Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for row in infos:
            gId = row[0]
            dId = row[1]
            name = row[2]
            columns = json.loads(row[3])
            value = {}
            value_string,a_,b_ = '','',''
            for column in columns:
                logger.info(f"--------------- select GatewayId:{gId} DeviceId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
                sqlCommand = f"select GWts,rawData,name from rawData.bacnet where GWts >= '{st}' and GWts < '{et}' and gatewayId = {gId} and name = '{column}' "
                #sqlCommand = f"select GWts,rawData,name from rawData.bacnet where gatewayId = {gId} and name = {column} order by GWts desc limit 1"
                try:
                    with conn.cursor() as cursor:
                        logger.debug(sqlCommand)
                        cursor.execute(sqlCommand)
                        datas = cursor.fetchall()
                except Exception as ex:
                    logger.error(f"Error executing {sqlCommand}")
                    logger.error(f"Error message: {ex}")
                    datas = []
                if len(datas) == 0:
                    logger.warning(f"{name} has no data in {st} to {et}")
                    continue
                else:
                    for data in datas:
                        ts = data[0]
                        rawdata = data[1]
                        id = data[2]
                        
                        if columns[id] == 'ch1Watt':
                            value[columns[id]] = round(float(rawdata), 2)
                        elif columns[id] == 'total':
                            if gId == 174:
                                value[columns[id]] = float(rawdata)/1000
                            else:
                                value[columns[id]] = round(float(rawdata), 2)
                        else:
                            value[columns[id]] = float(rawdata)

            for a,b in zip(value.keys(),value.values()):
                a_ += f"{a},"
                b_ += f"{b},"     
            if a_ != '' and b_ != '':
                value_string = f"replace into dataETL.power(ts,gatewayId,name,{a_[:-1]}) value ('{ts}',{gId},'{name}',{b_[:-1]})"
                with conn.cursor() as cursor:
                    try:
                        logger.debug(value_string)
                        cursor.execute(value_string)
                    except Exception as ex:
                        logger.error(f"Error executing {value_string}")
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