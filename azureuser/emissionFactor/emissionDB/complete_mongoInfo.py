"""
    從MongoDB完善mysql的mongoInfo
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

def getSource(cursor: pymysql.cursors.Cursor):
    source = {}
    sqlCommand = f"""
        SELECT EFSId, sourceName
        FROM emissionFactor.source
    """
    cursor.execute(sqlCommand)
    result = cursor.fetchall()
    for (EFSid, sourceName) in result:
        source[sourceName] = EFSid
    
    return source

def get_cal_approaches(db: pymongo.database.Database):
    pipeline = {}
    keys = {}

    return db.cal_approaches.find(pipeline, keys) if keys != {} else db.cal_approaches.find(pipeline)

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

    cal_approaches = get_cal_approaches(db)

    with conn.cursor() as cursor:
        source = getSource(cursor)
        cId = util.getCId("cal_approaches", cursor)
        
        i = 1
        for cal in cal_approaches:
            cal_source = cal['methods'][0]['source']

            
            value_string = f"({i}, '{cal['_id']}', '{cal['type']}', '{cal['scope']}', '{cal['typeOfEmission'] if cal['typeOfEmission'] is not None else 'NULL'}', '{cal['category']}', '{cal['categoryKey']}', '{cal['nameKey']}', '{cal['sourceType']}', '{cal.get('usedRegion') if 'usedRegion' in cal else 'NULL'}', '{cal['sourceOfEmission'] if 'sourceOfEmission' in cal else 'NULL'}',{source[cal_source]}, {cId})".replace("'NULL'", "NULL")
            replace_sql = f"""
                REPLACE INTO emissionFactor.mongoInfo(sId, _id, type, scope, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion, sourceOfEmission, EFSId, cId) VALUES {value_string}
            """
            logger.debug(replace_sql)
            cursor.execute(replace_sql)
            i += 1