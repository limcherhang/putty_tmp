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
        source = "CN Power"
        
        ever_schema = 'ever'
        ever_table = 'CN_power_data_2021'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)
        # [['地區', '因子（kgCO /kWh）2'], ['地區', '因子（kgCO /kWh)2']]
        for ID, _class, rawdata, year in cursor.fetchall():
            
            logger.debug(rawdata)
            everId = ID
            _type = _class
            
            rawdata = eval(rawdata)
            sourceOfEmission = rawdata["地區"]
            co2 = rawdata.get("因子（kgCO /kWh）2", rawdata.get("因子（kgCO /kWh)2"))

            baseUnit = "kWh"
            CO2Unit = "kgCO₂"
            
            latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)
            EFId = code+f"{latestId:05}"

            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId, type) VALUES ('{EFId}', '{sourceOfEmission}', {co2}, '{baseUnit}', 2021, '{CO2Unit}', {sourceId}, '{ever_table}', '{everId}', '{_type}')"
            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del replace_sql
            util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)
        
    conn.close()
    endTime = time.time()                
    logger.info(f"Total time: {endTime - startTime:.2f} seconds")