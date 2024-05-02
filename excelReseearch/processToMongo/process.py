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

def process_category(category: float, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT `key` FROM  emissionFactor.EFCategory WHERE ROUND(category,1)='{category}'
    """
    cursor.execute(sqlCommand)
    (result, ) = cursor.fetchone()

    return result

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

    # Create mongo connection
    client = MongoConn(config['mongo_azure'])
    client.connect()

    # Get database
    db = client.get_database()

    with conn.cursor() as cursor:
        sqlCommand = f"SELECT * FROM emissionFactor.EFType;"
        cursor.execute(sqlCommand)
        for typeId, _type, scope, typeOfEmission, category, sourceOfEmission, year, _file, link, sourceId, mongoId in cursor.fetchall():

            sqlCommand = f"SELECT * FROM emissionFactor.EFSource WHERE sourceId={sourceId}"
            cursor.execute(sqlCommand)

            result = cursor.fetchall()
            (sourceId, sourceName, level, country, code, organize, latestId) = result[0]

            sqlCommand = f"SELECT * FROM emissionFactor.EF WHERE typeId = {typeId}"

            cursor.execute(sqlCommand)

            methods = []
            for EFId, companyId, name, vehicleType, sourceType, fuelEfficiency, CO2, CH4, N2O, unit, __type, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, typeId, everTable, everId in cursor.fetchall():
                method = {}
                if name:
                    method['sourceOfEmission'] = name
                if vehicleType:
                    method['vehicleType'] = vehicleType
                if sourceType:
                    method['sourceType'] = sourceType
                if fuelEfficiency:
                    method['fuelEfficiency'] = fuelEfficiency
                if CO2:
                    method['co2Factor'] = CO2
                if CH4:
                    method['ch4Factor'] = CH4
                if N2O:
                    method['n2oFactor'] = N2O
                if unit:
                    method['baseUnit'] = unit
                if __type:
                    method["type"] = __type
                method["urlName"] = urlName
                method["sheetName"] = sheetName
                if CO2Unit:
                    method['CO2Unit'] = CO2Unit
                if CH4Unit:
                    method['CH4Unit'] = CH4Unit
                if N2OUnit:
                    method['N2OUnit'] = N2OUnit
                
                methods.append(method)

            insertData = {}
            if _type:
                insertData["type"] = _type
            insertData["methods"] = methods
            insertData["scope"] = f"scope{scope}"
            if typeOfEmission:
                insertData["typeOfEmission"] = typeOfEmission
            insertData["category"] = process_category(category, cursor)
            if sourceOfEmission:
                insertData["sourceOfEmission"] = sourceOfEmission
            insertData["source"] = sourceName
            insertData["year"] = year
            insertData["file"] = _file
            insertData["link"] = link

            # insert insertData to mongoDB with new _id to cal_approaches collection
            insertData['_id'] = ObjectId()
            db.cal_approaches.insert_one(insertData)

    conn.close()
    client.close()

    endTime = time.time()
    logger.info(f"Time taken to process: {endTime - startTime} seconds")