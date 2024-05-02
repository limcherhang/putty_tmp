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
        sqlCommand = f"""
            SELECT typeId, type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, mongoId FROM mgmtCarbon.EFtype
        """
        cursor.execute(sqlCommand)

        for typeId, _type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, mongoId in cursor.fetchall():

            sqlCommand = f"""
                SELECT companyId, name, vehicleType, fuelEfficiency, CO2, CH4, N2O, unit, type, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId FROM mgmtCarbon.EF WHERE typeId={typeId}
            """
            cursor.execute(sqlCommand)

            EFList = cursor.fetchall()

            sqlCommand = f"""
                SELECT `key` FROM mgmtCarbon.EFCategory WHERE ROUND(category,1)={category};
            """

            cursor.execute(sqlCommand)
            (key, ) = cursor.fetchone()

            json_format_dict = {
                "_id": str(ObjectId(mongoId)),
                "type": _type,
                "scope": "scope"+f"{scope}",
                "methods": [],
                "typeOfEmission": typeOfEmission,
                'category' : key,
                'categoryKey': categoryKey,
                'nameKey': nameKey,
                'sourceType': sourceType,
            }
            if usedRegion:
                json_format_dict['usedRegion'] = usedRegion

            for ef in EFList:
                logger.debug(ef)
                companyId, name, vehicleType, fuelEfficiency, CO2, CH4, N2O, unit, _type_, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId = ef
                sqlCommand = f"""
                    SELECT name, link, file FROM mgmtCarbon.EFsource WHERE sourceId = {sourceId}
                """
                cursor.execute(sqlCommand)

                source, link, _file = cursor.fetchone()
                
                method = {
                    "sourceOfEmission": name
                }
                if companyId:
                    method['companyId'] = companyId
                
                if vehicleType:
                    method['vehicleType'] = vehicleType

                method['co2Factor'] = CO2
                method['ch4Factor'] = CH4
                method['n2oFactor'] = N2O
                method['baseUnit'] = unit
                method['source'] = source
                method['urlName'] = urlName
                method['sheetName'] = sheetName
                if _type_:
                    method['type'] = _type_
                method['file'] = _file
                method['link'] = link
                method['year'] = year
                method['emissionUnit'] = CO2Unit
                method['ch4Unit'] = CH4Unit
                method['n2oUnit'] = N2OUnit

                # logger.debug(json.dumps(method, indent=4, ensure_ascii=False))
                json_format_dict['methods'].append(method)
            
            logger.debug(json.dumps(json_format_dict, indent=4, ensure_ascii=False))