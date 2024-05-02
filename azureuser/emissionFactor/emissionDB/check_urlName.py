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

    sources = ['EPA', 'EPA Taiwan', 'DEFRA', 'GHG', 'IPCC']

    for source in sources:
        cal_approaches = db.cal_approaches.find({"methods.source": source})

        urlNames = []
        sheetNames = []
        files = []
        links = []
        for cal in cal_approaches:
            for method in cal['methods']:
                urlName = method.get('urlName')
                if urlName not in urlNames:
                    urlNames.append(urlName)
                
                sheetName = method.get('sheetName')
                if sheetName not in sheetNames:
                    sheetNames.append(sheetName)

                _file = method['file']
                if _file not in files:
                    files.append(_file)

                link = method['link']
                if link not in links:
                    links.append(link)

        logger.debug(source)
        logger.debug(urlNames)
        logger.debug(sheetNames)
        logger.debug(files)
        logger.debug(links)

    client.close()