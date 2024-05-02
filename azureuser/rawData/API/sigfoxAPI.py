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
from time import sleep
def main(conn):
    params = {
        "limit": 4
    }
    rawdata_string = ''
    try:
        with conn.cursor() as cursor:
            sql = f"SELECT a.gatewayId,a.deviceId,b.username, b.password FROM mgmtETL.SigfoxAPI a join mgmtETL.APIList b on a.keyId=b.keyid ;"
            cursor.execute(sql)
            logger.info(f"--------------- select sigfoxAPI Mapping --------------- Took: {round(time.time()-s, 3)}s")
            for row in cursor:
                try:
                    gId = row[0]
                    dId = row[1]
                    user = row[2]
                    pwd = row[3]
                    url = f"https://api.sigfox.com/v2/devices/{dId}/messages"
                    resp = requests.get(url, auth=(user, pwd), params=params)
                    logger.info(f"--------------- select gId:{gId} dId:{dId}--------------- Took: {round(time.time()-s, 3)}s")
                    if resp.status_code == requests.codes.ok:               
                        for item in resp.json()['data']:
                            if dId != item['device']['id']:
                                continue
                            time_str = item['time']
                            ts = datetime.fromtimestamp(time_str / 1000)
                            rawdata = json.dumps(item)
                            rawdata_string += f"('{datetime.now().replace(microsecond=0)}', '{ts}', {gId}, '{dId}', '{rawdata}'), "
                    
                    time.sleep(1)
                except Exception as ex:
                    logger.error(f"[ERROR]GatewayId:{gId} Name:{dId}")
                    
        if rawdata_string != '':
            rawdata_string = rawdata_string[:-2]
            with conn.cursor() as cursor:
                replace_sql = f"replace into `rawData`.`sigfoxAPI` (`DBts`, `APIts`, `gatewayId`, `deviceId`, `rawData`) Values {rawdata_string}"
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

   