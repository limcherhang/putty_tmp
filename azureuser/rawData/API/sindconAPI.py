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
    rawdata_string = ''
    try:
        with conn.cursor() as cursor:
            sql = f"SELECT a.gatewayId,a.SN,a.devEUI,b.username, b.password FROM mgmtETL.SindconAPI a JOIN mgmtETL.APIList b ON a.keyId=b.keyid;"
            cursor.execute(sql)
            logger.info(f"--------------- select sindconAPI Mapping --------------- Took: {round(time.time()-s, 3)}s")
            for row in cursor:
                try:
                    gId = row[0]
                    sn = row[1]
                    eui = row[2]
                    user = row[3]
                    pwd = row[4]
                    data = {
                            "username": user, 
                            "password": pwd
                        }
                    url = f"https://sindconiot.com/api/internal/login"
                    resp = requests.post(url, json=data)
                    if resp.status_code == requests.codes.ok:
                        token = resp.json()['jwt']
                        headers = {"Grpc-Metadata-Authorization": token}
                        url = f"https://sindconiot.com/api/dc/history/meter"
                        params = {
                        "devEUI": eui, 
                        "sn": sn,
                        "unit": "hour", 
                        "span": 0
                        }
                        resp = requests.get(url, headers=headers, params=params)
                        if resp.status_code == requests.codes.ok:
                            item_list = resp.json()['items']
                            for item in item_list:
                                data_list = item['data']
                                title = item['title']
                                for data in data_list:
                                    ts = data['time']
                                    rawdata = json.dumps(data)
                                    rawdata_string += f"('{datetime.now().replace(microsecond=0)}', '{ts}', {gId}, '{sn}', '{eui}', '{title}', '{rawdata}'), "
                    time.sleep(1) 
                except Exception as ex:
                    logger.error(f"[ERROR]GatewayId:{gId} SN:{sn} devEUI:{eui}")  
                                         
        if rawdata_string != '':
            rawdata_string = rawdata_string[:-2]
            with conn.cursor() as cursor:
                replace_sql = f"replace into `rawData`.`sindconAPI` (`DBts`, `APIts`, `gatewayId`, `SN` ,`devEUI`,`title`,`rawData`) Values {rawdata_string}"
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
    
