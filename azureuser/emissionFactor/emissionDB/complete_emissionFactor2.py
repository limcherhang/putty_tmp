"""
    從emissionAsset完善mysql的emissionFactor
"""
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

def getEmissionAsset(cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT companyId, sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, sId FROM emissionFactor.emissionAsset;
    """

    cursor.execute(sqlCommand)
    return cursor.fetchall()

def check(sourceOfEmission: str, co2Factor: float, ch4Factor: float, n2oFactor: float, ar4Factor: float, ar5Factor: float, cursor:pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor FROM emissionFactor.emissionFactor WHERE sourceOfEmission = "{sourceOfEmission}" 
    """

    """
    AND co2Factor={co2Factor} AND ch4Factor={ch4Factor} AND n2oFactor={n2oFactor} AND ar4Factor={ar4Factor} AND ar5Factor={ar5Factor};
    """
    if co2Factor:
        sqlCommand += f" AND co2Factor={co2Factor}"
    if ch4Factor:
        sqlCommand += f" AND ch4Factor={ch4Factor}"
    if n2oFactor:
        sqlCommand += f" AND n2oFactor={n2oFactor}"
    if ar4Factor:
        sqlCommand += f" AND ar4Factor={ar4Factor}"
    if ar5Factor:
        sqlCommand += f" AND ar5Factor={ar5Factor}"

    cursor.execute(sqlCommand)
    return cursor.fetchall()

def getCategory(sId: int, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT category FROM emissionFactor.mongoInfo WHERE sId = {sId}
    """
    # logger.debug(sqlCommand)
    cursor.execute(sqlCommand)

    (category, ) = cursor.fetchone()

    sqlCommand = f"""
        SELECT category FROM emissionFactor.scopeAndCategories WHERE categoryName='{category}'
    """
    # logger.debug(sqlCommand)
    cursor.execute(sqlCommand)
    result = cursor.fetchall()
    logger.debug(result)
    categorys = []

    # for (res, ) in result:


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
        emissionAsset = getEmissionAsset(cursor)
        i = 1
        for companyId, sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, _file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, sId in emissionAsset:
            result = check(sourceOfEmission, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, cursor)

            if result != ():
                logger.debug(result)
                break

            categorys = getCategory(sId, cursor)
            
            # value_string = f"""({i}, "{sourceOfEmission}", '{companyId}', '{vehicleType}', {fuelEfficiency}, {co2Factor}, {ch4Factor}, {n2oFactor}, {ar4Factor}, {ar5Factor}, '{baseUnit}', '{urlName}', '{sheetName}', '{_file}', '{link}', {year}, '{emissionUnit}', '{ch4Unit}', '{n2oUnit}', '{sourceType}', {sId})""".replace("None", "NULL").replace("'NULL'","NULL").replace('"NULL"',"NULL")

            # replace_sql = f"""
            #     REPLACE INTO emissionFactor.emissionFactor(EFId, sourceOfEmission, companyId, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, sId) VALUES {value_string}
            # """
            
            # logger.debug(replace_sql)
            # cursor.execute(replace_sql)
            # logger.debug("REPLACE SUCCEED!")
            # i+=1
    
    conn.close()