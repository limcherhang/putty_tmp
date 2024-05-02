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

def main(from_conn,to_conn,st,et):
    with to_conn.cursor() as to_cursor:
        sql = f"SELECT gatewayId,from_table,to_table FROM mgmtETL.IOTmgmt "
        try:
            logger.debug(sql)
            to_cursor.execute(sql)
            NameList = to_cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- select iotmgmt Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for row in NameList:
            gId = row[0]
            from_table = row[1]
            to_table = row[2]
            with from_conn.cursor() as from_cursor:
                logger.info(f"--------------- select GatewayId:{gId}--------------- Took: {round(time.time()-s, 3)}s")
                sqlCommand = f"select * from iotmgmt.{from_table} where gatewayId={gId} and ts>='{st}' and ts<'{et}' "
                try:
                    logger.debug(sqlCommand)
                    from_cursor.execute(sqlCommand)
                except Exception as ex:
                    logger.error(f"Error executing {sqlCommand}")
                    logger.error(f"Error message: {ex}")
                
                if from_cursor.rowcount == 0:
                    logger.warning(f"has no data in '{st}' to '{et}'")
                    continue
                else:
                    for data in from_cursor:
                        data = tuple(
                        f"{value.strftime('%Y-%m-%d %H:%M:%S.%f')}" if isinstance(value, datetime) else value for value in data)
                        data = str(data).replace('None', 'NULL')
            
                        replace_sql = f"replace into `iotmgmt`.`{to_table}` values {data}"
                        logger.debug(replace_sql)
                        to_cursor.execute(replace_sql)
                        logger.info(f"--------------- replace to {to_conn.host} success --------------- Took: {round(time.time()-s, 3)}s")

if __name__ == '__main__':
    s = time.time()
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')
    from_conn = MySQLConn(config['mysql_tc'])
    to_conn = MySQLConn(config['mysql_azureV2'])
    logger = myLog.get_logger(os.getcwd(), f"{filename}.log",config['mysql_azureV2'], level=logging.ERROR)

    nowTime = datetime.now().replace(microsecond=0)
    logger.info(f"---------- Program Starts! : {nowTime} ---------- ")
    st = nowTime - timedelta(minutes=2)
    et = nowTime 
    main(from_conn,to_conn,st,et)
    from_conn.close()
    to_conn.close()
    logger.info(f"{from_conn.host} close")    
    logger.info(f"{to_conn.host} close")    
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")