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

    with conn.cursor() as cursor:
        sqlCommand = f"""
            SELECT ID, CName, EName, value, unit, YearOfAnnouncement, EFId FROM ever.TW_data_PDF;
        """

        cursor.execute(sqlCommand)
        for ID, CName, EName, value, unit, YearOfAnnouncement, EFId in cursor.fetchall():
            replace_sql = f"""
                replace into ever.TW_data_PDF VALUES ({ID}, '{CName}', '{EName}', '{value}', '{unit}', {YearOfAnnouncement}, {ID})
            """

            cursor.execute(replace_sql)