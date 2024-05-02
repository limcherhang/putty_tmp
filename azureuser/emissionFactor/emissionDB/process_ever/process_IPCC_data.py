import os
import sys
rootPath = os.getcwd() + '/../../../'
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
        source = "IPCC"
        
        ever_schema = 'ever'
        ever_table = 'IPCC_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = getSourceId(source, write_schema, sourceTable, cursor)
        resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table} WHERE Fuel2006 is not NULL;
        """

        cursor.execute(sqlCommand)
        units = []
        for EFID, IPCC1996SourceSinkCategory, IPCC2006SourceSinkCategory, Gas, Fuel1996, Fuel2006, Cpool, Typeofparameter, Description, TechnologiesPractices, ParametersConditions, RegionRegionalConditions, AbatementControlTechnologies, Otherproperties, Value, Unit, Equation, IPCCWorksheet, TechnicalReference, Sourceofdata, Dataprovider in cursor.fetchall():
            
            everID = EFID
            if Fuel2006:
                sourceOfEmission = Fuel2006
                if Otherproperties:
                    sourceOfEmission += f' - {Otherproperties}'

            else:
                logger.error(f"{EFID}")
            
            tmp_typeOfEmission = IPCC2006SourceSinkCategory.split('-') if IPCC2006SourceSinkCategory is not None else IPCC1996SourceSinkCategory.split('-')
            typeOfEmission_of_type = tmp_typeOfEmission[1].replace(' ', '')
            co2Factor = Value
            
            if Unit not in units:
                # logger.debug(f"{EFID}, {IPCC1996SourceSinkCategory}, {IPCC2006SourceSinkCategory}, {Gas}, {Fuel1996}, {Fuel2006}, {Cpool}, {Typeofparameter}, {Description}, {TechnologiesPractices}, {ParametersConditions}, {RegionRegionalConditions}, {AbatementControlTechnologies}, {Otherproperties}, {Value}, {Unit}, {Equation}, {IPCCWorksheet}, {TechnicalReference}, {Sourceofdata}, {Dataprovider}")
                logger.debug(f"{sourceOfEmission}, {Unit}")
                units.append(Unit)