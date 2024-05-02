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

def processValue(value: str):
    return float(value.split(' ')[0])

def processUnit(unit: str):
    if unit == '片':
        return 'slice'
    elif unit == '袋':
        return 'bag'
    elif unit in ('張','件'):
        return 'piece'
    elif unit == '包':
        return 'pack'
    elif unit == '每人每餐':
        return 'per person per meal'
    elif unit in ('支', '盞', '顆', '條', '個','台'):
        return 'a'
    elif unit == '罐':
        return 'can'
    elif unit == '瓶':
        return 'bottle'
    elif unit == '杯':
        return 'cup'
    elif unit in ('卷','捲'):
        return 'roll'
    elif unit == '每服務人次':
        return 'per person served'
    elif unit == '每房-每天':
        return 'Per room - per day'
    elif unit == '每平方米˙每小時':
        return 'per square meter˙per hour'
    elif unit == '桶':
        return 'bucket'
    elif unit == '每人次':
        return 'per person'
    elif unit in ('盒','箱'):
        return 'box'
    elif unit == '雙':
        return 'pair'
    elif unit == '公斤':
        return 'kg'
    elif unit == '公升':
        return 'L'
    elif unit == '組':
        return 'group'
    elif unit == '每坪-每天':
        return 'per ping-daily'
    elif unit == '每平方米‧每小時':
        return 'per square meter‧per hour'
    elif unit == '每人':
        return 'per person'
    elif unit == '每人‧每小時':
        return 'Per person‧Hour'
    else:
        return unit.split('(')[1].split(')')[0]

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
    
    cal_approaches = db.cal_approaches.find()

    i = 1
    with conn.cursor() as cursor:
        sqlCommand = f"""
            SELECT ID, CName, EName, value, unit, YearOfAnnouncement, EFId FROM ever.TW_data_PDF
        """
        cursor.execute(sqlCommand)

        for ID, CName, EName, value, unit, YearOfAnnouncement, EFId in cursor.fetchall():
            value = processValue(value)
            unit = processUnit(unit)

            value_string = f"({i}, '{EName}', {value}, '{unit}', '{YearOfAnnouncement}', {2}, {i})".replace('None','NULL').replace("''", 'NULL').replace("'NULL'", "NULL")

            replace_sql = f"""REPLACE INTO mgmtCarbon.EF (EFID, name, CO2, unit, year, sourceId, typeId) VALUES {value_string}"""

            logger.debug(replace_sql)

            cursor.execute(replace_sql)
            i+=1