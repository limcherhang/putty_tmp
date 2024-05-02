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
        source = 'EPA(Taiwan)'

        ever_schema = 'ever'
        ever_table = 'Excel_power'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"SELECT * FROM {ever_schema}.{ever_table};"

        cursor.execute(sqlCommand)

        for year, name, CO2eValue, unit in cursor.fetchall():
            everId = year

            latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            sourceOfEmission = year

            year = int(year.split('年')[0]) + 1911

            EFId = code+f"{latestId:05}"

            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', '{CO2eValue}', 'kWh', '{year}', 'kgCO₂', {sourceId}, '{ever_table}', '{everId}')"

            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)

            latestId += 1
            del sourceOfEmission, year, replace_sql
            util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)

    conn.close()