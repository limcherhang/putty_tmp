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

def UMG96_handle(conn, gId, name, st, et):
    logger.debug(f"hi UMG96_handle func.")
    value_string = ''
    with conn.cursor() as data_cursor:
        sqlCommand = f"select APIts,title,rawdata from rawData.sindconAPI where APIts>='{st}' and APIts<'{et}' and SN='5234002' order by APIts asc"
        try:
            logger.debug(sqlCommand)
            data_cursor.execute(sqlCommand)
            datas = data_cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            datas = []
        
        for data in datas:
            logger.debug(data)
            ts = data[0]
            title = data[1]
            rawdata = json.loads(data[2])
            value = rawdata['value']
            if 'kWh' in title:
                wh = round(float(value) , 3)

            logger.info(f"'{ts}', {gId}, '{name}', {wh}")
            value_string += f"('{ts}',  {gId}, '{name}', {wh}), "
    
    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`power` (`ts`, `gatewayId`, `name`, `total`) Values {value_string}"
            
            try:
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
                logger.info(f"Replacement Succeed")
            except Exception as ex:
                logger.error(f"Error executing {replace_sql}")
                logger.error(f"Error message: {str(ex)}")

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 1 and deviceLogic = 4 "
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
            sn = row[1]
            name = row[2]

            logger.info(f"----- Prcoessing GatewayId:{gId} {name} {sn} -----")
            if sn == '5234002':
                UMG96_handle(conn,gId,name,st, et)
                continue

            sqlCommand = f"select APIts,title,rawdata from rawData.sindconAPI where APIts>='{st}' and APIts<'{et}' and SN='{sn}' order by APIts asc"
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
                ts = data[0]
                title = data[1]
                rawdata = json.loads(data[2])
                value = rawdata['value']
                if 'kWh' in title :
                    wh = round(float(value), 3)
                elif 'Kw' in title :
                    w = round(float(value), 3)
                logger.info(f"'{ts}', {gId}, '{name}', {w}, {wh}")
                value_string += f"('{ts}', {gId}, '{name}', {w}, {wh}), "
                if wh != 0 and w != 0:
                    wh = 0
                    w = 0
    
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
    st = (nowTime - timedelta(minutes=60)).replace(second=0)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
