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
from dateutil import parser

def main(conn,st):
    value_string = ''
    key = 0
    try:
        with conn.cursor() as cursor:
            sql = "SELECT a.gatewayId,a.meterId,a.path,b.username, b.password FROM mgmtETL.TealeAPI a join mgmtETL.APIList b on a.keyId=b.keyid "
            cursor.execute(sql)
            logger.info(f"--------------- select sindconAPI Mapping --------------- Took: {round(time.time()-s, 3)}s")
            for row in cursor:
                try:
                    gId = row[0]
                    mId = row[1]
                    path = ('null' if row[2] is None else row[2])
                    user = row[3]
                    pwd = row[4]

                    if key ==0:
                        clientAPI_url= "https://ems.teale.asia/api/login/request_token"
                        resp = requests.get(clientAPI_url, auth=(user, pwd))
                        if resp.status_code == requests.codes.ok:
                            token = json.loads(resp.text)['access_token']
                            logger.debug(token)
                            key = 1

                    if path == 'null':
                        logger.info(f"----- Processing {mId} -----")
                        
                        params = {'meterId':f'{mId}', 'getDataFrom':f'{st}'}
                        headers = {'Authorization': f'Bearer {token}'}

                        data_url = "https://ems.teale.asia/api/v2/MeterDataSync/GetData?"
                        data_resp = requests.get(data_url, headers = headers, params = params)
                        
                        if data_resp.status_code == requests.codes.ok:
                            data_list = data_resp.json()
                            for data in data_list:
                                APIts = parser.parse(data['Timestamp']).strftime("%Y-%m-%d %H:%M:%S") 
                                rawdata = json.dumps(data)
                                value_string += f"('{datetime.now().replace(microsecond=0)}', '{APIts}', {gId}, {mId}, '{rawdata}'), "

                    elif path != 'null':
                        data_url = f"https://www.pandogrid.com{path}"
                        data_resp = requests.get(data_url, auth=(user,pwd))
                    
                        if data_resp.status_code == requests.codes.ok:
                            data_list = data_resp.json()
                            APIts = parser.parse(data_list['time']).strftime("%Y-%m-%d %H:%M:%S") 
                            rawdata = json.dumps(data_list)
                            value_string += f"('{datetime.now().replace(microsecond=0)}', '{APIts}', {gId}, '{mId}', '{rawdata}'), "

                except Exception as ex:
                    logger.error(f"[ERROR]GatewayId:{gId} Name:{mId}")
                    
        if value_string != '':
            value_string = value_string[:-2]
            with conn.cursor() as cursor:
                replace_sql = f"replace into `rawData`.`tealeAPI` (`DBts`, `APIts`, `gatewayId`, `meterId`, `rawData`) Values {value_string}"
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
    st = datetime.now()-timedelta(minutes=30)
    main(conn,st)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")

