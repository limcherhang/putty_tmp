import requests
import json
from datetime import datetime, timedelta
import time
import os
from pathlib import Path
import sys , os
rootPath = str(Path.cwd())+'/../../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser

def main(conn):
    value_string = ''
    date = datetime.now().strftime('%Y-%m-%d')
    try:
        with conn.cursor() as cursor:
            sql = "SELECT a.gatewayId,a.uId,b.username, b.password FROM mgmtETL.KiwiAPI a join mgmtETL.APIList b on a.keyId=b.keyId"
            cursor.execute(sql)
            logger.info(f"--------------- select kiwiAPI Mapping --------------- Took: {round(time.time()-s, 3)}s")
            for row in cursor:
                try:
                    gId = row[0]
                    uId = row[1]
                    user = row[2]
                    pwd = row[3]
                
                    data_url = f"https://custname.kiwi-alert.com/api/sensor/get-values?UID={uId}/Temp&fromDate={date}"
                    data_resp = requests.get(data_url, auth=(user, pwd), verify=False)
                    logger.debug(data_resp.url)
                    logger.info(f"--------------- select gId:{gId} uId:{uId}--------------- Took: {round(time.time()-s, 3)}s")
                    if data_resp.status_code == requests.codes.ok:
                        data_list = json.loads(data_resp.text)
                        if len(data_list) > 0:
                            for data in data_list:
                                APIts = data['DateTimeSGT'].replace('T', ' ').replace('Z', ' ')
                                rawdata = json.dumps(data)
                                value_string += f"('{datetime.now().replace(microsecond=0)}','{APIts}',{gId},'{uId}','{rawdata}'), "
                        else:
                            logger.warning(f"GatewayId:{gId} {uId} doesn't have data now")
                            
                except Exception as ex:
                    logger.error(f"[ERROR]GatewayId:{gId} Name:{uId}")

        if value_string != '':
            value_string = value_string[:-2]
            with conn.cursor() as cursor:
                replace_sql = f"replace into `rawData`.`kiwiAPI` (`DBts`,`APIts`, `gatewayId`, `uId`, `rawData`) Values {value_string}"
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
                logger.info(f"--------------- replace to {conn.host} success --------------- Took: {round(time.time()-s, 3)}s")

    except Exception as ex:
        logger.error(ex)
    
if __name__ == '__main__':
    s = time.time()
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')
    conn = MySQLConn(config['mysql_azureV2'])
    logger = myLog.get_logger(os.getcwd(), f"{filename}.log",config['mysql_azureV2'])
    main(conn)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")