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

    conn = MySQLConn(config['mysql_azureV2'])

    with conn.cursor() as cursor:
        curr_EAIds = []

        sqlCommand = f"""
            SELECT EFId, EAIds FROM emissionFactor.emissionFactor;
        """

        cursor.execute(sqlCommand)
        for (EFId, EAIds) in cursor.fetchall():
            for r in eval(EAIds):
                if r not in curr_EAIds:
                    curr_EAIds.append(r)
                else:
                    logger.warning(f"Occur repeat EAId {curr_EAIds} - {EFId}")
        
        logger.debug(len(curr_EAIds))
    
    conn.close()