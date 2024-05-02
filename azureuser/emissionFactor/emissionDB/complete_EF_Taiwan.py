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

def getTypeInfo(type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, sourceId, cursor: pymysql.cursors.Cursor):
    category = processCategory(category, cursor)
    sqlCommand = f"""
        SELECT typeId
        FROM mgmtCarbon.EFtype
        WHERE type='{type}' AND scope='{scope[-1]}' 
        AND ROUND(category,1)={category} 
        AND categoryKey='{categoryKey}' AND nameKey='{nameKey}' AND sourceType='{sourceType}' AND sourceId={sourceId}
    """

    if typeOfEmission is not None:
        sqlCommand += f" AND typeOfEmission='{typeOfEmission}'"
    else:
        sqlCommand += f" AND typeOfEmission IS NULL"
    
    if usedRegion is not None:
        sqlCommand += f" AND usedRegion='{usedRegion}'"
    else:
        sqlCommand += f" AND usedRegion IS NULL"
    
    logger.debug(sqlCommand)
    cursor.execute(sqlCommand)
    if cursor.rowcount == 0:
        return -100
    (typeId, ) = cursor.fetchone()
    return typeId

def getSourceId(source: str, cursor: pymysql.cursors.Cursor):
    if source == 'EPA':
        source = 'EPA USA'
    sqlCommand = f"""
        SELECT sourceId FROM mgmtCarbon.EFsource
        WHERE name='{source}'
    """

    cursor.execute(sqlCommand)
    if cursor.rowcount == 0:
        return -100

    (sourceId, ) = cursor.fetchone()
    return sourceId

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
    
    cal_approaches = db.cal_approaches.find({'usedRegion': "Taiwan"})

    i = 1
    with conn.cursor() as cursor:
        for cal_approach in cal_approaches:
            
            methods = cal_approach['methods']
            # if methods[0]['sourceOfEmission'] in ('Taiwan Power Company 2022', 'Taiwan Power Company 2021','Taiwan Power Company 2020', 'Taiwan Power Company 2019', 'Taiwan Power Company 2018', 'Taiwan Power Company 2017', 'Taiwan government'):
            #     sourceId = 6
            # else:
            #     sourceId = getSourceId(methods[0]['source'], cursor)
            sourceId = getSourceId(methods[0]['source'], cursor)

            typeId = getTypeInfo(cal_approach['type'], cal_approach['scope'], cal_approach.get('typeOfEmission'), cal_approach['category'], cal_approach['categoryKey'], cal_approach['nameKey'], cal_approach['sourceType'], cal_approach.get('usedRegion'), cal_approach.get('sourceOfEmission'), sourceId, cursor)

            for method in methods:
                print(i)
                companyId = method.get('companyId')
                sourceOfEmission = method['sourceOfEmission']
                vehicleType = method.get('vehicleType')
                fuelEfficiency = method.get('fuelEfficiency')
                CO2 = method.get('co2Factor')
                CH4 = method.get('ch4Factor')
                N2O = method.get('n2oFactor')
                baseUnit = method['baseUnit']
                urlName = method.get('urlName')
                sheetName = method.get('sheetName')
                _type = method.get('type')
                _file = method['file']
                link = method['link']
                year = method.get('year')
                CO2Unit = method.get('emissionUnit')
                CH4Unit = method.get('ch4Unit')
                N2OUnit = method.get('n2oUnit')
                sourceType = method.get('sourceType')

                value_string = f"""({i}, '{companyId}', "{sourceOfEmission}", '{vehicleType}', '{fuelEfficiency}', '{CO2}', '{CH4}', '{N2O}', '{baseUnit}', '{_type}', '{year}', {sourceId}, {typeId})""".replace("None", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"""
                    REPLACE INTO mgmtCarbon.EF VALUES {value_string}
                """
                logger.debug(replace_sql)
                cursor.execute(replace_sql)

                i += 1


                