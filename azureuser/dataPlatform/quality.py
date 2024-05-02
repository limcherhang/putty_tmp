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
        sqlCommand = "SELECT siteId,gatewayId,name,dataETLName FROM mgmtETL.DataPlatform where name like 'quality%' and logicModel = 'normal' and logicModel is NOT NULL ;"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select quality Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            sId = rows[0]
            gId = rows[1]
            name = rows[2]
            etlName = rows[3]
            sqlCommand = f"select ts, ph, ORP, TDS, EC from dataETL.waterQuality where gatewayId={gId} and name='{etlName}' and ts>='{st}' and ts<'{et}'"
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
                logger.debug(data)
                ts = data[0]
                ph = ('NULL' if data[1] is None else data[1])
                orp = ('NULL' if data[2] is None else data[2])
                tds = ('NULL' if data[3] is None else data[3])
                ec = ('NULL' if data[4] is None else data[4])
                value_string += f"('{ts}', {sId}, '{name}', {ph}, {orp}, {tds}, {ec}), "

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataPlatform`.`quality` (`ts`, `siteId`, `name`, `pH`, `ORP`, `TDS`, `EC`) Values {value_string}"
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

    nowTime = datetime.now().replace(day=28,hour=14,minute=0,microsecond=0)
    logger.info(f"---------- Program Starts! : {nowTime} ---------- ")
    st = nowTime - timedelta(minutes=60)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")