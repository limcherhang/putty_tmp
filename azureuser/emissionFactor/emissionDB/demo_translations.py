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

def getTranslations(cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT tr.en, tr.de, tr.it, tr.jp, tr.th, tr.tw, tr.zh, url.urlName FROM mgmtCarbon.translations tr
        JOIN mgmtCarbon.url ON url.urlId = tr.urlId;
    """

    cursor.execute(sqlCommand)
    return cursor.fetchall()


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

    with conn.cursor() as cursor:
        translations = getTranslations(cursor)
        
        result = {}

        for (en, de, it, jp, th, tw, zh, urlName) in translations:
            if urlName not in result:
                result[urlName] = []
            if en is None:
                continue
            result[urlName].append([en, de, it, jp, th, tw, zh])
    
    for urlName, translate in result.items():
        format_to_mongo = {
            "url": urlName,
            "tran": []
        }
        de_data = {}
        en_data = {}
        it_data = {}
        jp_data = {}
        th_data = {}
        tw_data = {}
        zh_data = {}
        for en, de, it, jp, th, tw, zh in translate:
            de_data[en] = de
            en_data[en] = en
            it_data[en] = it
            jp_data[en] = jp
            th_data[en] = th
            tw_data[en] = tw
            zh_data[en] = zh

        format_to_mongo['tran'].append({
            "name": "de",
            "data": de_data
        })
        format_to_mongo['tran'].append({
            "name": "en",
            "data": en_data
        })
        format_to_mongo['tran'].append({
            "name": "it",
            "data": it_data
        })
        format_to_mongo['tran'].append({
            "name": "jp",
            "data": jp_data
        })
        format_to_mongo['tran'].append({
            "name": "th",
            "data": th_data
        })
        format_to_mongo['tran'].append({
            "name": "tw",
            "data": tw_data
        })
        format_to_mongo['tran'].append({
            "name": "zh",
            "data": zh_data
        })
        logger.debug(format_to_mongo)



    conn.close()
    client.close()