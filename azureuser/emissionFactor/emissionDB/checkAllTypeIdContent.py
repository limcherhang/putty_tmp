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
        for i in range(1, 131):
            sqlCommand = f"""
                SELECT EFId, companyId, sourceOfEmission, vehicleType, fuelEfficiency, CO2, CH4, N2O, baseUnit, urlName, sheetName, type, file, link, year, CO2Unit, CH4Unit, N2OUnit, sourceType, sourceId, typeId
                FROM mgmtCarbon.EF2 WHERE typeId={i}
            """

            cursor.execute(sqlCommand)
            EF = cursor.fetchall()

            collect = []
            for EFId, companyId, sourceOfEmission, vehicleType, fuelEfficiency, CO2, CH4, N2O, baseUnit, urlName, sheetName, type, file, link, year, CO2Unit, CH4Unit, N2OUnit, sourceType, sourceId, typeId in EF:
                append_list = [baseUnit, urlName, sheetName, type, file, link, year, CO2Unit, CH4Unit, N2OUnit, sourceType, sourceId]

                if append_list in collect:
                    continue
                else:
                    collect.append(append_list)

        
            logger.debug(collect)
            logger.debug(len(collect))
            logger.debug("")

    conn.close()
    client.close()