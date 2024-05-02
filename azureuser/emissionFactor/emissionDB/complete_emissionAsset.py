"""
    從MongoDB完善mysql的emissionAsset
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

def getIdAndMethods(db: pymongo.database.Database):
    pipeline = {}
    keys = {"_id": 1, "methods": 1}

    return db.cal_approaches.find(pipeline, keys) if keys != [] else db.cal_approaches.find(pipeline)

def getSId(id: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT sId FROM emissionFactor.mongoInfo WHERE `_id`='{id}'
    """

    cursor.execute(sqlCommand)
    return cursor.fetchone()[0]

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"])

    # Create mongo connection
    client = MongoConn(config['mongo_dev_v1_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    conn = MySQLConn(config['mysql_azureV2'])


    idAndMethods = getIdAndMethods(db)
    i = 1
    with conn.cursor() as cursor:
        for idmethod in idAndMethods:
            _id = idmethod['_id']
            method = idmethod['methods']
            
            sId = getSId(_id, cursor)
            cId = util.getCId("cal_approaches", cursor) 
            
            for m in method:
                value_string = f"""({i},"{m.get('companyId',None)}","{m['sourceOfEmission']}",'{m.get('vehicleType',None)}',{m.get('fuelEfficiency',None)},{m.get('co2Factor',None)},{m.get('ch4Factor',None)},{m.get('n2oFactor', None)},{m.get('ar4Factor',None)},{m.get('ar5Factor',None)},'{m['baseUnit']}','{m['source']}','{m.get('urlName',None)}','{m.get('sheetName',None)}','{m['file']}','{m['link']}','{m.get('year',None)}','{m['emissionUnit']}','{m.get('ch4Unit',None)}','{m.get('n2oUnit',None)}','{m.get('sourceType',None)}',{sId},{cId})""".replace("None", "NULL").replace("'NULL'","NULL").replace('"NULL"',"NULL")

                replace_sql = f"""
                    REPLACE INTO emissionFactor.emissionAsset(EAId, companyId, sourceOfEmission, vehicleType, fuelEfficiency, co2Factor, ch4Factor, n2oFactor, ar4Factor, ar5Factor, baseUnit, source, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, sourceType, sId, cId) VALUES {value_string}
                """
                
                logger.debug(replace_sql)
                cursor.execute(replace_sql)

                i+=1
    client.close()
    conn.close()