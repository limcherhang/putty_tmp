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
        source = 'MEIH'

        ever_schema = 'ever'
        ever_table = 'Malaysia_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"SELECT * FROM {ever_schema}.{ever_table}"

        cursor.execute(sqlCommand)

        for name, year, FuelType, FuelconsumptionTJ, co2Factor, GHGEmission(GgCO2e), NetElectricityGeneration(GWh), GEF(GgCO2e/GWh) in cursor.fetchall():