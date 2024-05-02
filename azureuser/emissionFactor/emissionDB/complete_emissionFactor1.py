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

def truncate_table(cursor: pymysql.cursors.Cursor):
    truncateCommand = f"""
        TRUNCATE TABLE emissionFactor.emissionFactor;
    """

    cursor.execute(truncateCommand)
    logger.info("TRUNCATE Succeed!")

def getEmissionAsset(cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT companyId, sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, sId FROM emissionFactor.emissionAsset;
    """

    cursor.execute(sqlCommand)
    result = cursor.fetchall()
    return result

def check(EAIds: list, cursor:pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT EFId FROM emissionFactor.emissionFactor WHERE EAIds ='{EAIds}'
    """

    # if co2Factor:
    #     sqlCommand += f" AND co2Factor={co2Factor}"
    # if ch4Factor:
    #     sqlCommand += f" AND ch4Factor={ch4Factor}"
    # if n2oFactor:
    #     sqlCommand += f" AND n2oFactor={n2oFactor}"
    # if ar4Factor:
    #     sqlCommand += f" AND ar4Factor={ar4Factor}"
    # if ar5Factor:
    #     sqlCommand += f" AND ar5Factor={ar5Factor}"
    # if source:
    #     sqlCommand += f" AND source='{source}'"
    logger.debug(sqlCommand)
    cursor.execute(sqlCommand)
    result = cursor.fetchall()

    # processResult = []
    # for EFId, _co2Factor, _ch4Factor, _n2oFactor, _ar4Factor, _ar5Factor in result:
    #     if co2Factor == _co2Factor and ch4Factor == _ch4Factor and n2oFactor == _n2oFactor and ar4Factor == _ar4Factor and ar5Factor == _ar5Factor:
    #         processResult.append(EFId)

    return result

def getEAId(sourceOfEmission: str, co2Factor: float, ch4Factor: float, n2oFactor: float, ar4Factor: float, ar5Factor: float, source: str, cursor: pymysql.cursors.Cursor):

    sqlCommand = f"""
        SELECT EAId, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor FROM emissionFactor.emissionAsset WHERE sourceOfEmission="{sourceOfEmission}" AND source='{source}'
    """

    logger.debug(sqlCommand)
    cursor.execute(sqlCommand)
    result = cursor.fetchall()

    processResult = []

    for EAId, _co2Factor, _ch4Factor, _n2oFactor, _ar4Factor, _ar5Factor in result:
        if co2Factor == _co2Factor and ch4Factor == _ch4Factor and n2oFactor == _n2oFactor and ar4Factor == _ar4Factor and ar5Factor == _ar5Factor:
            processResult.append(EAId)

    return processResult

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
        truncate_table(cursor)
        emissionAsset = getEmissionAsset(cursor)
        i = 1
        for companyId, sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, _file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, sId in emissionAsset:
            # result = check(sourceOfEmission, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, source, cursor)

            # if result != ():
            #     logger.debug(result)
            #     break

            EAIds = getEAId(sourceOfEmission, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, source, cursor)

            checkEAIds = check(EAIds, cursor)
            if checkEAIds != ():
                logger.debug(f"Occur: {EAIds}, {checkEAIds}, {sourceOfEmission}, {source}, {co2Factor}, {ch4Factor}, {n2oFactor}, {ar4Factor}, {ar5Factor}")
                continue

            value_string = f"""({i}, "{sourceOfEmission}", '{companyId}', '{vehicleType}', {fuelEfficiency}, {co2Factor}, {ch4Factor}, {n2oFactor}, {ar4Factor}, {ar5Factor}, '{baseUnit}', '{source}', '{urlName}', '{sheetName}', '{_file}', '{link}', {year}, '{emissionUnit}', '{ch4Unit}', '{n2oUnit}', '{sourceType}', '{EAIds}', {sId})""".replace("None", "NULL").replace("'NULL'","NULL").replace('"NULL"',"NULL")

            replace_sql = f"""
                REPLACE INTO emissionFactor.emissionFactor(EFId, sourceOfEmission, companyId, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, EAIds, sId) VALUES {value_string}
            """
            
            logger.debug(replace_sql)
            cursor.execute(replace_sql)
            logger.debug("REPLACE SUCCEED!")
            i+=1
    
    conn.close()