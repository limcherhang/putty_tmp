from datetime import datetime, timedelta
import time
import os,sys,json
from pathlib import Path
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser
import logging

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 1 and deviceLogic = 3"
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        for row in infos:
            gId = row[0]
            mId = row[1]
            name = row[2]
            logger.debug(f"----- Prcoessing {gId} {name} {mId}-----")

            sqlCommand = f"select APIts,rawdata from rawData.tealeAPI where APIts>='{st}' and APIts<'{et}' and gatewayId ='{gId}' and meterId='{mId}' "
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
                    rawdata = json.loads(data[1])
                    ch1watt = ('NULL' if rawdata['Raw'] is None else rawdata['Raw']*1000*4)
                    totalPositiveWattHour = ('NULL' if rawdata['Cumulative'] is None else rawdata['Cumulative']*1000 )
                    value_string += f"('{ts}', {gId}, '{name}', {ch1watt}, {totalPositiveWattHour}), "


    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`power` (`ts`, `gatewayId`, `name`, `ch1Watt`, `total`) Values {value_string}"
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
    st = (nowTime - timedelta(minutes=30)).replace(second=0)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")