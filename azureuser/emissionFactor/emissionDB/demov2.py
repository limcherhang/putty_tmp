"""
    demo 符合MongoDB cal_approaches的結果
"""
import os
import sys
rootPath = os.getcwd() + '/../../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
from utils import myLog
from bson import ObjectId
from bson import json_util
import time
import json

def getMongoInfo(cursor: pymysql.cursors.Cursor):
    """
        If you add a language, you need to modify the query here
    """
    sqlCommand = f"""
        SELECT mi.*, 
        c.sourceName-- , ec.English, ec.Chinese, ec.Japanese, ec.Thai 
        FROM emissionFactor.mongoInfo mi
        JOIN emissionFactor.source c ON mi.EFSId=c.EFSId
        -- JOIN emissionFactor.eletricityCompany ec ON ec.languageId = mi.languageId;
    """

    cursor.execute(sqlCommand)
    return cursor.fetchall()

def getEmissionAsset(sId: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT EAId, companyId, sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType FROM emissionFactor.emissionAsset
        WHERE sId='{sId}'
    """

    cursor.execute(sqlCommand)
    return cursor.fetchall()

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]+'.log'

    logger = myLog.get_logger('./log/', logFile, config['mysql_azureV2'])


    # Create mongo connection
    client = MongoConn(config['mongo_dev_v1_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    conn = MySQLConn(config['mysql_azureV2'])
    
    with conn.cursor() as cursor:

        ans = []
        MongoInfo = getMongoInfo(cursor)

        # for sId, _id, _type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, EFSId, languageId, sourceName, English, Chinese, Japanese, Thai in MongoInfo:
        for sId, _id, _type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, EFSId, languageId, sourceName in MongoInfo:
            cal_approach = {
                "_id": ObjectId(_id),
                "type": _type,
                "methods": [],
                "scope": scope,
            }
            if typeOfEmission:
                cal_approach['typeOfEmission']= typeOfEmission
            if category:
                cal_approach['category'] = category
            if categoryKey:
                cal_approach['categoryKey'] = categoryKey
            if nameKey:
                cal_approach['nameKey'] = nameKey
            if sourceType:
                cal_approach['sourceType'] = sourceType
            if usedRegion:
                cal_approach['usedRegion'] = usedRegion
            if sourceOfEmission:
                cal_approach['sourceOfEmission'] = sourceOfEmission
        
            emissionAsset = getEmissionAsset(sId, cursor)

            methods = []
            for EAId, companyId, _sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, _source, urlName, sheetName, _file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType in emissionAsset:
                method = {}
                if companyId:
                    method['companyId'] = companyId
                if _sourceOfEmission:
                    method['sourceOfEmission'] = _sourceOfEmission
                if vehicleType:
                    method['vehicleType'] = vehicleType
                if fuelEfficiency:
                    method['fuelEfficiency'] = fuelEfficiency
                if co2Factor:
                    method['co2Factor'] = co2Factor
                if ch4Factor:
                    method['ch4Factor'] = ch4Factor
                if n2oFactor:
                    method['n2oFactor'] = n2oFactor
                if ar4Factor:
                    method['ar4Factor'] = ar4Factor
                if ar5Factor:
                    method['ar5Factor'] = ar5Factor
                if baseUnit:
                    method['baseUnit'] = baseUnit
                if _source:
                    method['source'] = _source
                if urlName:
                    method['urlName'] = urlName
                if sheetName:
                    method["sheetName"] = sheetName
                if _file:
                    method["file"] = _file
                if link:
                    method['link'] = link
                if year:
                    method['year'] = year
                if emissionUnit:
                    method['emissionUnit'] = emissionUnit
                if ch4Unit:
                    method['ch4Unit'] = ch4Unit
                if n2oUnit:
                    method['n2oUnit'] = n2oUnit
                if sourceType:
                    method['sourceType'] = sourceType
                
                methods.append(method)

            cal_approach['methods'] = methods
            if cal_approach['methods'] == []:
                continue
            json_data = json.dumps(cal_approach, default=json_util.default)
            with open(f"./json_data/{sId}.json", "w") as f:
                f.write(json_data)

            parsed_data = json.loads(json_data)
            logger.debug(parsed_data)
    client.close()
    conn.close()