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
    
    cal_approaches = db.cal_approaches.find()

    with conn.cursor() as cursor:

        for i, cal_approach in enumerate(cal_approaches):
            _id = cal_approach['_id']
            typeId = i+1
            _type = cal_approach.get('type')
            scope = cal_approach['scope'][-1]
            typeOfEmission = cal_approach['typeOfEmission']
            category = processCategory(cal_approach['category'], cursor)
            categoryKey = cal_approach.get('categoryKey')
            nameKey = cal_approach.get('nameKey')
            sourceType = cal_approach.get('sourceType')
            usedRegion = cal_approach.get('usedRegion')
            sourceOfEmission = cal_approach.get('sourceOfEmission')

            value_string = f"({typeId}, '{_type}', '{scope}', '{typeOfEmission}', '{category}', '{categoryKey}', '{nameKey}', '{sourceType}', '{usedRegion}', '{sourceOfEmission}', '{_id}')".replace("None", "NULL").replace("'NULL'", "NULL")

            replace_sql = f"""
                REPLACE INTO mgmtCarbon.EFtype VALUES {value_string}
            """
            logger.debug(replace_sql)
            cursor.execute(replace_sql)

            i += 1