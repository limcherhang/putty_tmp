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

    conn = MySQLConn(config['mysql_azureV2'])

    translations = db.translations.find()

    with conn.cursor() as cursor:
        for i, translation in enumerate(translations):
            tran = translation['tran']
            de = tran[0].get('_id')
            en = tran[1].get('_id')
            it = tran[2].get('_id')
            jp = tran[3].get('_id')
            th = tran[4].get('_id')
            tw = tran[5].get('_id')
            zh = tran[6].get('_id')

            replace_sql = f"REPLACE INTO mgmtCarbon.url VALUES ({i+1}, '{translation['url']}', '{de}', '{en}', '{it}', '{jp}', '{th}', '{tw}', '{zh}')".replace("'None'", "NULL")

            logger.debug(replace_sql)
            cursor.execute(replace_sql)


    conn.close()
    client.close()