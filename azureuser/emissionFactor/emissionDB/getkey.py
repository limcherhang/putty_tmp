"""
    取得cal_approches的所有keys
    和
    methods的key
    以及
    methods底下有什麼source
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
import time

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]+'.log'
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}", config["mysql_azureV2"])


    # Create mongo connection
    client = MongoConn(config['mongo_dev_v1_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    pipeline = [
        {"$project": {"arrayofkeyvalue": {"$objectToArray": "$$ROOT"}}},
        {"$unwind": "$arrayofkeyvalue"},
        {"$group": {"_id": None, "allkeys": {"$addToSet": "$arrayofkeyvalue.k"}}}
    ]

    result = db.cal_approaches.aggregate(pipeline)
    for res in result:
        logger.debug(res)
        logger.debug("")

    
    data = db.cal_approaches.find({})

    method_keys = set()
    method_source = []

    for d in data:
        for method in d['methods']:
            method_keys.update(method.keys())
            if method['source'] not in method_source:
                method_source.append(method['source'])
    
    logger.debug(method_keys)
    logger.debug(method_source)

    categorys = []
    data2 = db.cal_approaches.find({})

    for d in data2:
        category = d['category']
        if category not in categorys:
            categorys.append(category)

    logger.debug(categorys)
    logger.debug(len(categorys))

    translations = db.translations.find()
    urls = []
    for tran in translations:
        urls.append(tran['url'])

    logger.debug(urls)


    client.close()