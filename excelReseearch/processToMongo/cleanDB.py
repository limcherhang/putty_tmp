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
    client = MongoConn(config['mongo_azure'])
    client.connect()

    # Get database
    db = client.get_database()

    # clean MongoDB collections call cal_approaches

    logger.info("Cleaning MongoDB collections call cal_approaches")

    # Get collection
    collection = db.cal_approaches

    # Delete all documents in collection
    collection.delete_many({})

    # Close mongo connection
    client.close()

    endTime = time.time()
    logger.info(f"Execution time: {endTime - startTime} seconds")