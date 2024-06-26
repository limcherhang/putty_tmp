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

def getLatestId(sourceId: int, write_schema: str, sourceTable: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT IFNULL(latestId, 1) FROM {write_schema}.{sourceTable} WHERE sourceId={sourceId}
    """
    cursor.execute(sqlCommand)
    (latestId, ) = cursor.fetchone()
    return latestId

def getSourceId(source: str, write_schema: str, sourceTable: str, cursor: pymysql.cursors.Cursor):
    if source == 'EPA':
        source = 'EPA USA'
    sqlCommand = f"""
        SELECT sourceId, code FROM {write_schema}.{sourceTable}
        WHERE name='{source}'
    """
    # logger.debug(sqlCommand)
    cursor.execute(sqlCommand)
    if cursor.rowcount == 0:
        return -100

    (sourceId, code) = cursor.fetchone()
    return sourceId, code

def updateLatestId(code: str, latestId: int, write_schema: str, sourceTable: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT sourceId, name, level, country, code, organize, link, file FROM {write_schema}.{sourceTable} WHERE code='{code}'
    """
    cursor.execute(sqlCommand)

    for sourceId, source, level, country, code, organize, link, _file in cursor.fetchall():
        replace_sql = f"""
            REPLACE INTO {write_schema}.{sourceTable}(sourceId, name, level, country, code, organize, link, file, latestId) VALUES ({sourceId}, '{source}', '{level}', '{country}', '{code}', '{organize}', '{link}', '{_file}', {latestId})
        """.replace("None", "NULL").replace("'NULL'", "NULL")
        cursor.execute(replace_sql)

    # logger.info("Update latestId Succeed!")

def resetLatestId(code: str, write_schema: str, sourceTable: str, cursor: pymysql.cursors.Cursor):
    confirm = input("Process reset latestId or not? Type Yes/yes or No/no: ")

    if confirm.lower() == 'yes':

    
        sqlCommand = f"SELECT * FROM {write_schema}.{sourceTable} WHERE code='{code}';"

        cursor.execute(sqlCommand)
        for sourceId, source, level, country, code, organize, link, _file, latestId in cursor.fetchall():
            replace_sql = f"""
                REPLACE INTO {write_schema}.{sourceTable}(sourceId, name, level, country, code, organize, link, file, latestId) VALUES ({sourceId}, '{source}', '{level}', '{country}', '{code}', '{organize}', '{link}', '{_file}', NULL)
            """.replace("None", "NULL").replace("'NULL'", "NULL")

            cursor.execute(replace_sql)
    else:
        return 0
    

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
        source = "EPA Taiwan"
        
        ever_schema = 'ever'
        ever_table = 'TW_data_PDF'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = getSourceId(source, write_schema, sourceTable, cursor)
        resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)

        for ID, CName, EName, value, unit, YearOfAnnouncement in cursor.fetchall():
            
            everId = ID

            sourceOfEmission = EName
            values = value.split(' ')
            
            if len(values) == 2:
                [value, EFunit] = values
                value = float(value)
            else:
                value = float(values[0])
                EFunit = 'g CO2e'
            value = float(value)

            latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

            EFId = code+f"{latestId:05}"

            value_string = f"('{EFId}', '{sourceOfEmission}', '{value}', '{unit}', '{YearOfAnnouncement}', '{EFunit}', {sourceId}, '{ever_table}', '{everId}')".replace("'None'", "NULL").replace("'NULL'", "NULL")

            replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

            try:
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del sourceOfEmission, value, EFunit, value_string, replace_sql
            updateLatestId(code, latestId, write_schema, sourceTable, cursor)