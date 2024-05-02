import os
import sys
rootPath = os.getcwd() + '/../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
import pymongo
from utils import myLog, util
from bson import ObjectId
import time
import json
    

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", config["mysql_azureV2"])

    conn = MySQLConn(config['mysql_azureV2'])

    with conn.cursor() as cursor:
        source = "EPA Taiwan"
        
        ever_schema = 'ever'
        ever_table = 'TW_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)
        
        unit_conversion = {
            '立方公尺(m3)': "m3", 
            '平方公尺(m2)': "m2", 
            '台': "a", 
            '公斤(kg)': "kg", 
            '延噸公里(tkm)': "ton-km",
            '瓶': "bottle", 
            '卷': "roll", 
            '個': "a",
            '雙': "a pair", 
            '件': "a piece", 
            '袋': "bag", 
            '每坪-每天': "Per ping-daily", 
            '每房-每天': "Per room-daily", 
            '張': "piece", 
            '公尺(m)': "m", 
            '延人公里(pkm)': "Person km", 
            '片': "piece", 
            '包':"pack", 
            '公噸(mt)': "metric tons", 
            '盒': "box", 
            '延頓公里(tkm)': "ton-km", 
            '每平方米˙每小時': "Per square meter per hour", 
            '每人': "Per person", 
            '每人˙每小時': "Per person per hour", 
            '每個20呎標準貨櫃-每公里(TEU-km)': "TEU-km", 
            '每人每餐': "Per person per meal", 
            '捲': "roll", 
            '桶': "barrel", 
            '度(kwh)': "kWh", 
            '盞': "cup", 
            '人次': "person-time", 
            '公秉(kl)': 'kL', 
            '公升(L)': "L", 
            '罐': "can", 
            '立方公尺m3': "m3", 
            '支': "branch", 
            '條': "bar", 
            '顆': "piece", 
            '公克(g)': "g", 
            '每服務人次': "per person served", 
            '杯': "cup"
        }

        for name, EF, EFUnit, unit, departmentname, announcementyear in cursor.fetchall():
            sourceOfEmission = name

            everId = name

            latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            unit = unit_conversion[unit]

            CO2Unit = 'kgCO₂'

            EFId = code+f"{latestId:05}"

            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {EF}, '{unit}', {announcementyear}, '{CO2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del sourceOfEmission, EFUnit, CO2Unit, replace_sql
            util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)