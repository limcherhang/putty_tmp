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

def convert_to_float(value):
    find_ = value.find('（')
    if find_!= -1:
        value = value[:find_]

    return value

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
        ever_table = 'CN_power_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)
        # ['省份', '2010年', '2012年', '2018年', '2020年']
        for ID, rawdata, year in cursor.fetchall():
            everId = ID
            
            rawdata = eval(rawdata)
            sourceOfEmission = rawdata["省份"]
            co2_2010 = rawdata["2010年"]
            co2_2012 = rawdata["2012年"]
            co2_2018 = rawdata["2018年"]
            co2_2020 = rawdata["2020年"]

            co2_2010 = convert_to_float(co2_2010)
            co2_2012 = convert_to_float(co2_2012)
            co2_2018 = convert_to_float(co2_2018)
            co2_2020 = convert_to_float(co2_2020)

            baseUnit = "kWh"
            CO2Unit = "kgCO₂"
            
            latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)
            EFId = code+f"{latestId:05}"

            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{CO2Unit}', {sourceId}, '{ever_table}', '{everId}')"
            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del replace_sql

            EFId = code+f"{latestId:05}"
            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{CO2Unit}', {sourceId}, '{ever_table}', '{everId}')"
            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del replace_sql

            EFId = code+f"{latestId:05}"
            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2018}, '{baseUnit}', 2018, '{CO2Unit}', {sourceId}, '{ever_table}', '{everId}')"
            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del replace_sql

            EFId = code+f"{latestId:05}"
            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2020}, '{baseUnit}', 2020, '{CO2Unit}', {sourceId}, '{ever_table}', '{everId}')"
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