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
        source = "DEFRA"
        
        ever_schema = 'ever'
        ever_table = 'UK_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = getSourceId(source, write_schema, sourceTable, cursor)
        resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT 
                LEFT(ID, LENGTH(ID) - 2) AS newID,
                Scope, Level1, Level2, 
                Level3, Level4, ColumnText, UOM AS baseUnit,
                GROUP_CONCAT(`GHG/Unit` SEPARATOR '|') as emissionUnits, -- √
                GROUP_CONCAT(ROUND(GHGConversionFactor,5) SEPARATOR '|') AS GHGConversionFactors -- √
            FROM
                {ever_schema}.{ever_table} GROUP BY newID;
        """

        cursor.execute(sqlCommand)

        for (ID, Scope, Level1, Level2, Level3, Level4, ColumnText, Unit, GHGUnits, GHGConversionFactors) in cursor.fetchall():
            everId = ID

            latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)
            # logger.debug(ColumnText)
            sourceOfEmission = ColumnText if ColumnText else Level3
            vehicleType = Level2 + ' - ' + Level3 if ColumnText else None

            _type = Level1
            
            GHGUnits = GHGUnits.split('|')
            if GHGConversionFactors:
                GHGConversionFactors = GHGConversionFactors.split('|')
            else:
                GHGConversionFactors = [None for i in range(len(GHGUnits))]
            
            co2Factor = None
            n2oFactor = None
            ch4Factor = None
            CO2Unit = None
            CH4Unit = None
            N2OUnit = None
            if len(GHGUnits) == 1:
                co2Factor = GHGConversionFactors[0]
                CO2Unit = 'kgCO₂'
            else:
                for i in range(len(GHGUnits)):
                    if "CH4" in GHGUnits[i]:
                        ch4Factor = GHGConversionFactors[i]
                       
                        CH4Unit = 'kgCH₄'
                    elif "N2O" in GHGUnits[i]:
                        n2oFactor = GHGConversionFactors[i]
                        
                        N2OUnit = 'kgN₂O'
                    elif GHGUnits[i] in ("kg CO2e of CO2 per unit", "kWh (Net CV)", "kWh (net)"):
                        co2Factor = GHGConversionFactors[i]
                    
                        CO2Unit = 'kgCO₂'
   
            Scope = Scope.replace(' ', '').lower()

            if Unit == 'tonnes':
                baseUnit = 'tonne'
            elif Unit == 'litres':
                baseUnit = 'Liter'
            elif "kwh" in Unit.lower():
                continue
            else:
                baseUnit = Unit
            
            EFId = code+f"{latestId:05}"

            replace_sql = f"""
                REPLACE INTO {write_schema}.{write_table}(EFId, name, vehicleType, CO2, CH4, N2O, unit, type, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', '{vehicleType}', '{co2Factor}', '{ch4Factor}', '{n2oFactor}', '{baseUnit}', '{_type}', '2023', 'DEFRA GHG reporting 2024', 'Factors by Category', '{CO2Unit}', '{CH4Unit}', '{N2OUnit}', '{sourceId}', '{ever_table}', '{everId}')
            """.replace("'None'", "NULL")

            try:
                logger.debug(replace_sql)
                cursor.execute(replace_sql)
            except Exception as ex:
                logger.error(f"Failed Replace {ex}")
                logger.error(replace_sql)
            latestId+=1

            del sourceOfEmission, co2Factor, ch4Factor, n2oFactor, CO2Unit, CH4Unit, N2OUnit, baseUnit
            updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            typeOfEmission = Level2

    conn.close()