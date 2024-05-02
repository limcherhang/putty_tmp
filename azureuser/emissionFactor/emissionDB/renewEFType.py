import os
import sys
rootPath = os.getcwd() + '/../../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
import pymongo
from utils import myLog, util
from bson import ObjectId
import time


if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", config["mysql_azureV2"])


    # Create mongo connection
    client = MongoConn(config['mongo_dev_v1_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    conn = MySQLConn(config['mysql_azureV2'])

    with conn.cursor() as cursor:
        
        sqlCommand = f"""
            SELECT typeId, type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, urlName, sheetName, file, link, year, CO2Unit, CH4Unit, N2OUnit, methodSourceType, sourceId
            FROM
            mgmtCarbon.EFtype2
        """

        cursor.execute(sqlCommand)
        EFType = cursor.fetchall()

        for typeId, type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, urlName, sheetName, file, link, year, CO2Unit, CH4Unit, N2OUnit, methodSourceType, sourceId in EFType:
            sqlCommand = f"""
                SELECT urlName, sheetName, file, link, year, CO2Unit, CH4Unit, N2OUnit, sourceType, sourceId FROM mgmtCarbon.EF2
                WHERE typeId={typeId} LIMIT 1;
            """

            cursor.execute(sqlCommand)
            if cursor.rowcount == 0:
                logger.warning(f"{typeId} has no data")
                continue

            urlName, sheetName, file, link, year, CO2Unit, CH4Unit, N2OUnit, sourceType, sourceId = cursor.fetchone()

            value_string = f"({typeId}, '{type}', '{scope}', '{typeOfEmission}', '{category}', '{categoryKey}', '{nameKey}', '{sourceType}', '{usedRegion}', '{sourceOfEmission}', '{urlName}', '{sheetName}', '{file}', '{link}', '{year}', '{CO2Unit}', '{CH4Unit}', '{N2OUnit}', '{methodSourceType}', {sourceId})".replace("None", "NULL").replace("'NULL'", "NULL")

            replace_sql = f"""REPLACE INTO mgmtCarbon.EFtype2 VALUES {value_string}"""

            logger.debug(replace_sql)

            cursor.execute(replace_sql)