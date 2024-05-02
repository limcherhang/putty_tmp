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

def processCategory(category: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT category FROM mgmtCarbon.EFCategory
        WHERE `key`='{category}';
    """
    
    cursor.execute(sqlCommand)
    (result, ) = cursor.fetchone()
    return result

def getTypeInfo(mongoId, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT typeId FROM mgmtCarbon.EFtype WHERE mongoId='{mongoId}'
    """
    cursor.execute(sqlCommand)
    if cursor.rowcount == 0:
        return -100
    (typeId, ) = cursor.fetchone()
    return typeId

def getSourceId(source: str, cursor: pymysql.cursors.Cursor):
    if source == 'EPA':
        source = 'EPA USA'
    sqlCommand = f"""
        SELECT sourceId, code FROM mgmtCarbon.EFsource
        WHERE name='{source}'
    """
    logger.debug(sqlCommand)
    cursor.execute(sqlCommand)
    if cursor.rowcount == 0:
        return -100

    (sourceId, code) = cursor.fetchone()
    return sourceId, code

def getSourceInfo(cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT sourceId, name, level, country, code, organize, link, file FROM mgmtCarbon.EFsource
    """

    cursor.execute(sqlCommand)
    return cursor.fetchall()

def updateLatestId(code: str, latestId: int, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT sourceId, name, level, country, code, organize, link, file FROM mgmtCarbon.EFsource WHERE code='{code}'
    """
    cursor.execute(sqlCommand)

    for sourceId, source, level, country, code, organize, link, _file in cursor.fetchall():
        replace_sql = f"""
            REPLACE INTO mgmtCarbon.EFsource(sourceId, name, level, country, code, organize, link, file, latestId) VALUES ({sourceId}, '{source}', '{level}', '{country}', '{code}', '{organize}', '{link}', '{_file}', {latestId})
        """.replace("None", "NULL").replace("'NULL'", "NULL")
        cursor.execute(replace_sql)

    logger.info("Update latestId Succeed!")


def getLatestId(sourceId: int, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT IFNULL(latestId, 1) FROM mgmtCarbon.EFsource WHERE sourceId={sourceId}
    """
    cursor.execute(sqlCommand)
    (latestId, ) = cursor.fetchone()
    return latestId    

def clearLatestId(cursor: pymysql.cursors.Cursor):
    sqlCommand = "SELECT COUNT(*) FROM mgmtCarbon.EFsource;"

    cursor.execute(sqlCommand)
    (_count, )  = cursor.fetchone()

    for i in range(_count):
        update_sql = f"UPDATE `mgmtCarbon`.`EFsource` SET `latestId` = NULL WHERE (`sourceId` = '{i+1}');"

        cursor.execute(update_sql)

    logger.info("Clear latestId Succeed!")

def clearEF(cursor: pymysql.cursors.Cursor):
    delete_sql = "DELETE FROM mgmtCarbon.EF;"

    confirm = input("Processing delete, type yes/YES if you agree, else no/NO: ")
    # confirm = 'yes'
    if confirm.lower() == 'yes':
        cursor.execute(delete_sql)
        logger.info("DELETE EF Succeed")
    else:
        logger.info("No processing delete")

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
    client = MongoConn(config['mongo_production_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    conn = MySQLConn(config['mysql_azureV2'])
    
    cursorMongo = db.cal_approaches.find()
    cal_approaches = []

    for _c in cursorMongo:
        cal_approaches.append(_c)

    logger.info("process cal_approaches succeed!")

    with conn.cursor() as cursor:
        clearLatestId(cursor)
        clearEF(cursor)
        for cal_approach in cal_approaches:
            _id = cal_approach['_id']
            _type = cal_approach.get('type')
            scope = cal_approach['scope']
            methods = cal_approach['methods']
            typeOfEmission = cal_approach.get('typeOfEmission')
            category = cal_approach['category']
            categoryKey = cal_approach.get('categoryKey')
            nameKey = cal_approach.get('nameKey')
            sourceType = cal_approach.get('sourceType')
            usedRegion = cal_approach.get('usedRegion')
            sourceOfEmission = cal_approach.get('sourceOfEmission')

            typeId = getTypeInfo(_id, cursor)
            logger.debug(f"{_id}, {typeId}")

            for method in methods:
                companyId = method.get('companyId')
                sourceOfEmission = method.get('sourceOfEmission')
                vehicleType = method.get('vehicleType')
                fuelEfficiency = method.get('fuelEfficiency')
                CO2 = method.get('co2Factor')
                CH4 = method.get('ch4Factor')
                N2O = method.get('n2oFactor')
                source = method['source']
                baseUnit = method['baseUnit']
                urlName = method.get('urlName')
                sheetName = method.get('sheetName')
                _type_ = method.get('type')
                _file = method['file']
                link = method['link']
                year = method.get('year')
                CO2Unit = method.get('emissionUnit')
                CH4Unit = method.get('ch4Unit')
                N2OUnit = method.get('n2oUnit')
                sourceType = method.get('sourceType')

                sourceId, code = getSourceId(source, cursor)

                latestId = getLatestId(sourceId, cursor)

                EFId = code+f"{latestId:05}"

                value_string = f"""('{EFId}', '{companyId}', "{sourceOfEmission}", '{vehicleType}', '{fuelEfficiency}', '{CO2}', '{CH4}', '{N2O}', '{baseUnit}', '{_type_}', '{year}', '{urlName}', '{sheetName}', '{CO2Unit}', '{CH4Unit}', '{N2OUnit}', {sourceId}, {typeId})""".replace("None", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"""
                    REPLACE INTO mgmtCarbon.EF(EFId, companyId, name, vehicleType, fuelEfficiency, CO2, CH4, N2O, unit, type, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, typeId) VALUES {value_string}
                """
                logger.debug(replace_sql)
                cursor.execute(replace_sql)

                latestId+=1

                updateLatestId(code, latestId, cursor)

    conn.close()
    client.close()