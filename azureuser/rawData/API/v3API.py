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
    try:
        with conn.cursor() as cursor:
            sql = "SELECT a.gatewayId,b.key FROM mgmtETL.V3API a join mgmtETL.APIList b on a.keyId=b.keyid limit 1 "
            cursor.execute(sql)
            for row in cursor:
                gId = row[0]
                key = row[1]

        params = {
            'key': key, 
            'type': 'track',
            'action':'last', 
            
        }
        
        url = f"https://www.v3nity.com/V3Nity4/api"
        resp = requests.get(url, params=params)
        if resp.status_code == requests.codes.ok:
            data_list = resp.json()
            for item in data_list['data']:
                try:
                    ts = item['timestamp']
                    ts = datetime.strptime(ts, '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    raw = json.dumps(item)
                    label = item['label']
                    if label == 'GR5221T':
                        continue
                    value_string += f"('{datetime.now().replace(microsecond=0)}','{ts}',{gId},'{label}','{raw}'), "

                except Exception as ex:
                    logger.error(f"[ERROR]GatewayId:{gId} Name:{label}")
            if value_string != '':
                value_string = value_string[:-2]
                with conn.cursor() as cursor:
                    replace_sql = f"replace into rawData.v3API(DBts,APIts,GatewayId,label,rawData) VALUES {value_string}"
                    cursor.execute(replace_sql)
                    logger.debug(replace_sql)
                       
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