"""
    每個sId產生出來的methods裡面的sourceOfEmission是否一樣
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
from bson import json_util
import time
import json

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]+'.log'

    logger = myLog.get_logger('./log/', logFile, config['mysql_azureV2'])


    # Create mongo connection
    client = MongoConn(config['mongo_dev_v1_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    files = os.listdir('./json_data')

    for file in files:
        with open('./json_data/'+file, 'r')as file:
            data = json.load(file)

        new_cal = dict(data)
        new_method = new_cal['methods']

        new_sourceOfEmission = []
        for m in new_method:
            new_sourceOfEmission.append(m['sourceOfEmission'])

        cal = db.cal_approaches.find({'_id':ObjectId(new_cal['_id']['$oid'])})

        cal = cal[0]
        method = cal['methods']

        sourceOfEmission = [m['sourceOfEmission'] for m in method]

        logger.debug(new_cal['_id']['$oid'])
        logger.debug(new_sourceOfEmission==sourceOfEmission)
        logger.debug("")

    client.close()