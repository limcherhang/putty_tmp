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

    with conn.cursor() as cursor:

        for i, cal_approach in enumerate(cal_approaches):
            typeId = i+1
            _type = cal_approach['type']
            scope = cal_approach['scope'][-1]
            typeOfEmission = cal_approach['typeOfEmission']
            category = processCategory(cal_approach['category'], cursor)
            categoryKey = cal_approach.get('categoryKey')
            nameKey = cal_approach['nameKey']
            sourceType = cal_approach['sourceType']
            usedRegion = cal_approach.get('usedRegion')
            sourceOfEmission = cal_approach.get('sourceOfEmission')

            method = cal_approach['methods'][0]
            source = method['source']
            urlName = method['urlName']
            sheetName = method['sheetName']
            CO2Unit = method['emissionUnit']
            CH4Unit = method['ch4Unit']
            N2OUnit = method['n2oUnit']
            sourceId = getSourceId(source, cursor)


            value_string = f"({typeId}, '{_type}', '{scope}', '{typeOfEmission}', '{category}', '{categoryKey}', '{nameKey}', '{sourceType}', '{usedRegion}', '{urlName}', '{sheetName}', '{CO2Unit}', '{CH4Unit}', '{N2OUnit}', {sourceId})".replace("None", "NULL").replace("'NULL'", "NULL")

            replace_sql = f"""
                REPLACE INTO mgmtCarbon.EFtype VALUES {value_string}
            """
            logger.debug(replace_sql)
            cursor.execute(replace_sql)

            i += 1