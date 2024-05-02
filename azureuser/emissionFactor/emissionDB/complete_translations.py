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

def geturlId(url: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"SELECT urlId FROM mgmtCarbon.url WHERE urlName='{url}'"

    cursor.execute(sqlCommand)
    (url, ) = cursor.fetchone()
    
    return url

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

    eng_max = 0
    de_max = 0
    it_max = 0
    jp_max = 0
    tw_max = 0
    zh_max = 0
    th_max = 0

    with conn.cursor() as cursor:
        i = 1
        for translation in translations:
            tmp = {}
            tran = translation['tran']
            
            for t in tran:
                name = t['name']
                data = t['data']
                for d, value in data.items():
                    if d not in tmp:
                        tmp[d] = {}
                    if name == 'en':
                        tmp[d][name] = d
                    else:
                        tmp[d][name] = data[d]
            
            for en, translate_collection in tmp.items():
                urlId = geturlId(translation['url'], cursor)

                value_string = f"({i}, {urlId}, "
                
                # en = translate_collection.get('en')
                
                if en:
                    eng_max = max(eng_max, len(en))
                    if '"' in en:
                        en = en.replace('"', "'")
                value_string += f""""{en}", """.replace('"None"', "NULL")

                de = translate_collection.get('de')
                
                if de:
                    de_max = max(de_max, len(de))
                    if '"' in de:
                        de = de.replace('"', "'")
                value_string += f""""{de}", """.replace('"None"', "NULL")

                it = translate_collection.get('it')
                
                if it:
                    it_max = max(it_max, len(it))
                    if '"' in it:
                        it = it.replace('"', "'")
                value_string += f""""{it}", """.replace('"None"', "NULL")
                
                jp = translate_collection.get('jp')
                
                if jp:
                    jp_max = max(jp_max, len(jp))
                    if '"' in jp:
                        jp = jp.replace('"', "'")
                value_string += f""""{jp}", """.replace('"None"', "NULL")

                th = translate_collection.get('th')
                
                if th:
                    th_max = max(th_max, len(th))
                    if '"' in th:
                        th = th.replace('"', "'")
                value_string += f""""{th}", """.replace('"None"', "NULL")
                
                tw = translate_collection.get('tw')
                
                if tw:
                    tw_max = max(tw_max, len(tw))
                    if '"' in tw:
                        tw = tw.replace('"', "'")
                value_string += f""""{tw}", """.replace('"None"', "NULL")
                
                zh = translate_collection.get('zh')
                
                if zh:
                    zh_max = max(zh_max, len(zh))
                    if '"' in zh:
                        zh = zh.replace('"', "'")
                value_string += f""""{zh}")""".replace('"None"', "NULL")

                replace_sql = f"""REPLACE INTO mgmtCarbon.translations VALUES {value_string}"""

                logger.debug(replace_sql)
                cursor.execute(replace_sql)
            
                i += 1
    logger.debug(eng_max)
    logger.debug(de_max)
    logger.debug(it_max)
    logger.debug(jp_max)
    logger.debug(tw_max)
    logger.debug(zh_max)
    logger.debug(th_max)

    conn.close()
    client.close()