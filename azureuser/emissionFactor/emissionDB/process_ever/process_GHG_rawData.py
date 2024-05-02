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

def getLatestId(sourceId: int, write_schema: str, sourceTable: str, cursor: pymysql.cursors.Cursor):
    sqlCommand = f"""
        SELECT IFNULL(latestId, 1) FROM {write_schema}.{sourceTable} WHERE sourceId={sourceId}
    """
    cursor.execute(sqlCommand)
    (latestId, ) = cursor.fetchone()
    return latestId

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
        source = 'GHG'

        ever_schema = 'ever'
        ever_table = 'GHG_rawData'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = getSourceId(source, write_schema, sourceTable, cursor)
        resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"SELECT * FROM {ever_schema}.{ever_table}"

        cursor.execute(sqlCommand)

        EF_content = {}
        # ['Fuel', 'FuelType', 'Gas basiskg/m3', 'Energy basiskg/TJ', 'Mass basiskg/tonne', 'Liquid basiskg/ litre', 'Lower heating Value TJ/Gg', 'Fuel density information1Of gases (kg/m3 of fuel)', 'Fuel density information1Of liquids (kg/litre fuel)']

        for ID, Type, rawData, Year, updateTime in cursor.fetchall():
            everId = ID

            rawData = eval(rawData)
            # key = [k for k in rawData]

            fuel = rawData['Fuel']
            
            if fuel not in EF_content:
                EF_content[fuel] = {}
            
            if 'CO2' in Type:
                EF_content[fuel]["CO2"] = {
                    "FuelType": rawData['FuelType'] if rawData['FuelType'] != 'Null' else None,
                    "Gas basiskg/m3": rawData['Gas basiskg/m3'] if rawData['Gas basiskg/m3'] != 'Null' else None,
                    "Energy basiskg/TJ": rawData['Energy basiskg/TJ'] if rawData['Energy basiskg/TJ'] != 'Null' else None,
                    "Mass basiskg/tonne": rawData['Mass basiskg/tonne'] if rawData['Mass basiskg/tonne'] != 'Null' else None,
                    "Liquid basiskg/ litre": rawData['Liquid basiskg/ litre'] if rawData['Liquid basiskg/ litre'] != 'Null' else None,
                    "Lower heating Value TJ/Gg": rawData['Lower heating Value TJ/Gg'] if rawData['Lower heating Value TJ/Gg'] != 'Null' else None,
                    "Fuel density information1Of gases (kg/m3 of fuel)": rawData['Fuel density information1Of gases (kg/m3 of fuel)'] if rawData['Fuel density information1Of gases (kg/m3 of fuel)'] != 'Null' else None,
                    "Fuel density information1Of liquids (kg/litre fuel)": rawData['Fuel density information1Of liquids (kg/litre fuel)'] if rawData['Fuel density information1Of liquids (kg/litre fuel)'] != 'Null' else None,
                    "everId": everId
                }
            elif "CH4" in Type:
                EF_content[fuel]["CH4"] = {
                    "FuelType": rawData['FuelType'] if rawData['FuelType'] != 'Null' else None,
                    "Gas basiskg/m3": rawData['Gas basiskg/m3'] if rawData['Gas basiskg/m3'] != 'Null' else None,
                    "Energy basiskg/TJ": rawData['Energy basiskg/TJ'] if rawData['Energy basiskg/TJ'] != 'Null' else None,
                    "Mass basiskg/tonne": rawData['Mass basiskg/tonne'] if rawData['Mass basiskg/tonne'] != 'Null' else None,
                    "Liquid basiskg/ litre": rawData['Liquid basiskg/ litre'] if rawData['Liquid basiskg/ litre'] != 'Null' else None,
                    "Lower heating Value TJ/Gg": rawData['Lower heating Value TJ/Gg'] if rawData['Lower heating Value TJ/Gg'] != 'Null' else None,
                    "Fuel density information1Of gases (kg/m3 of fuel)": rawData['Fuel density information1Of gases (kg/m3 of fuel)'] if rawData['Fuel density information1Of gases (kg/m3 of fuel)'] != 'Null' else None,
                    "Fuel density information1Of liquids (kg/litre fuel)": rawData['Fuel density information1Of liquids (kg/litre fuel)'] if rawData['Fuel density information1Of liquids (kg/litre fuel)'] != 'Null' else None,
                    "everId": everId
                }
            elif "N2O" in Type:
                EF_content[fuel]["N2O"] = {
                    "FuelType": rawData['FuelType'] if rawData['FuelType'] != 'Null' else None,
                    "Gas basiskg/m3": rawData['Gas basiskg/m3'] if rawData['Gas basiskg/m3'] != 'Null' else None,
                    "Energy basiskg/TJ": rawData['Energy basiskg/TJ'] if rawData['Energy basiskg/TJ'] != 'Null' else None,
                    "Mass basiskg/tonne": rawData['Mass basiskg/tonne'] if rawData['Mass basiskg/tonne'] != 'Null' else None,
                    "Liquid basiskg/ litre": rawData['Liquid basiskg/ litre'] if rawData['Liquid basiskg/ litre'] != 'Null' else None,
                    "Lower heating Value TJ/Gg": rawData['Lower heating Value TJ/Gg'] if rawData['Lower heating Value TJ/Gg'] != 'Null' else None,
                    "Fuel density information1Of gases (kg/m3 of fuel)": rawData['Fuel density information1Of gases (kg/m3 of fuel)'] if rawData['Fuel density information1Of gases (kg/m3 of fuel)'] != 'Null' else None,
                    "Fuel density information1Of liquids (kg/litre fuel)": rawData['Fuel density information1Of liquids (kg/litre fuel)'] if rawData['Fuel density information1Of liquids (kg/litre fuel)'] != 'Null' else None,
                    "everId": everId
                }
            else:
                logger.error(f"Wrong Type: {Type}")

        for sourceOfEmission, ghgInfo in EF_content.items():
            co2Info = ghgInfo.get('CO2')
            ch4Info = ghgInfo.get('CH4')
            n2oInfo = ghgInfo.get('N2O')

            if co2Info:
                co2m3 = co2Info["Gas basiskg/m3"]
                co2TJ = co2Info["Energy basiskg/TJ"]
                co2tonne = co2Info["Mass basiskg/tonne"]
                co2liter = co2Info["Liquid basiskg/ litre"]
                co2m32 = co2Info["Fuel density information1Of gases (kg/m3 of fuel)"]
                co2liter2 = co2Info["Fuel density information1Of liquids (kg/litre fuel)"]
                co2everId = co2Info['everId']
            else:
                co2m3 = None
                co2TJ = None
                co2tonne = None
                co2liter = None
                co2m32 = None
                co2liter2 = None
                co2everId = None

            if ch4Info:
                ch4m3 = ch4Info["Gas basiskg/m3"]
                ch4TJ = ch4Info["Energy basiskg/TJ"]
                ch4tonne = ch4Info["Mass basiskg/tonne"]
                ch4liter = ch4Info["Liquid basiskg/ litre"]
                ch4m32 = ch4Info["Fuel density information1Of gases (kg/m3 of fuel)"]
                ch4liter2 = ch4Info["Fuel density information1Of liquids (kg/litre fuel)"]
                ch4everId = ch4Info['everId']
            else:
                ch4m3 = None
                ch4TJ = None
                ch4tonne = None
                ch4liter = None
                ch4m32 = None
                ch4liter2 = None
                ch4everId = None

            if n2oInfo:
                n2om3 = n2oInfo["Gas basiskg/m3"]
                n2oTJ = n2oInfo["Energy basiskg/TJ"]
                n2otonne = n2oInfo["Mass basiskg/tonne"]
                n2oliter = n2oInfo["Liquid basiskg/ litre"]
                n2om32 = n2oInfo["Fuel density information1Of gases (kg/m3 of fuel)"]
                n2oliter2 = n2oInfo["Fuel density information1Of liquids (kg/litre fuel)"]
                n2oeverId = n2oInfo['everId']
            else:
                n2om3 = None
                n2oTJ = None
                n2otonne = None
                n2oliter = None
                n2om32 = None
                n2oliter2 = None
                n2oeverId = None

            if co2m3 is not None or ch4m3 is not None or n2om3 is not None:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                everIds = []
                if co2m3 is not None:
                    everIds.append(co2everId)
                if ch4m3 is not None:
                    everIds.append(ch4everId)
                if n2om3 is not None:
                    everIds.append(n2oeverId)

                _type = "Gas basis"
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{_type}', '{co2m3 if co2m3 is not None else 'NULL'}', '{ch4m3 if ch4m3 is not None else 'NULL'}', '{n2om3 if n2om3 is not None else 'NULL'}', 'm3', 2024, 'GHG EF from Cross-Sector Tools', 'Sheet Name : Stationary combustion', '{'kgCO₂' if co2m3 is not None else 'NULL'}', '{'kgCH₄' if ch4m3 is not None else 'NULL'}', '{'kgN₂O' if n2om3 is not None else 'NULL'}', {sourceId}, '{ever_table}', '{', '.join(str(x) for x in everIds)}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, type, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del co2m3, ch4m3, n2om3, everIds, _type, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            if co2TJ is not None or ch4TJ is not None or n2oTJ is not None:
                logger.debug(f"{co2TJ}, {ch4TJ}, {n2oTJ}")
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                everIds = []
                if co2TJ is not None:
                    everIds.append(co2everId)
                if ch4TJ is not None:
                    everIds.append(ch4everId)
                if n2oTJ is not None:
                    everIds.append(n2oeverId)

                _type = "Energy Basis"
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{_type}', '{co2TJ if co2TJ is not None else 'NULL'}', '{ch4TJ if ch4TJ is not None else 'NULL'}', '{n2oTJ if n2oTJ is not None else 'NULL'}', 'TJ', 2024, 'GHG EF from Cross-Sector Tools', 'Sheet Name : Stationary combustion', '{'kgCO₂' if co2TJ is not None else 'NULL'}', '{'kgCH₄' if ch4TJ is not None else 'NULL'}', '{'kgN₂O' if n2oTJ is not None else 'NULL'}', {sourceId}, '{ever_table}', '{', '.join(str(x) for x in everIds)}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, type, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del co2TJ, ch4TJ, n2oTJ, everIds, _type, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            if co2tonne is not None or ch4tonne is not None or n2otonne is not None:
                logger.debug(f"{co2tonne}, {ch4tonne}, {n2otonne}")
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                everIds = []
                if co2tonne is not None:
                    everIds.append(co2everId)
                if ch4tonne is not None:
                    everIds.append(ch4everId)
                if n2otonne is not None:
                    everIds.append(n2oeverId)

                _type = "Mass basis"
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{_type}', '{co2tonne if co2tonne is not None else 'NULL'}', '{ch4tonne if ch4tonne is not None else 'NULL'}', '{n2otonne if n2otonne is not None else 'NULL'}', 'tonne', 2024, 'GHG EF from Cross-Sector Tools', 'Sheet Name : Stationary combustion', '{'kgCO₂' if co2tonne is not None else 'NULL'}', '{'kgCH₄' if ch4tonne is not None else 'NULL'}', '{'kgN₂O' if n2otonne is not None else 'NULL'}', {sourceId}, '{ever_table}', '{', '.join(str(x) for x in everIds)}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, type, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del co2tonne, ch4tonne, n2otonne, everIds, _type, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            if co2liter is not None or ch4liter is not None or n2oliter is not None:
                logger.debug(f"{co2liter}, {ch4liter}, {n2oliter}")
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                everIds = []
                if co2liter is not None:
                    everIds.append(co2everId)
                if ch4liter is not None:
                    everIds.append(ch4everId)
                if n2oliter is not None:
                    everIds.append(n2oeverId)

                _type = "Liquid Basis"
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{_type}', '{co2liter if co2liter is not None else 'NULL'}', '{ch4liter if ch4liter is not None else 'NULL'}', '{n2oliter if n2oliter is not None else 'NULL'}', 'Liter', 2024, 'GHG EF from Cross-Sector Tools', 'Sheet Name : Stationary combustion', '{'kgCO₂' if co2liter is not None else 'NULL'}', '{'kgCH₄' if ch4liter is not None else 'NULL'}', '{'kgN₂O' if n2oliter is not None else 'NULL'}', {sourceId}, '{ever_table}', '{', '.join(str(x) for x in everIds)}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, type, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del co2liter, ch4liter, n2oliter, everIds, _type, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            if co2m32 is not None or ch4m32 is not None or n2om32 is not None:
                logger.debug(f"{co2m32}, {ch4m32}, {n2om32}")
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                everIds = []
                if co2m32 is not None:
                    everIds.append(co2everId)
                if ch4m32 is not None:
                    everIds.append(ch4everId)
                if n2om32 is not None:
                    everIds.append(n2oeverId)

                _type = "Fuel density information Of gases"
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{_type}', '{co2m32 if co2m32 is not None else 'NULL'}', '{ch4m32 if ch4m32 is not None else 'NULL'}', '{n2om32 if n2om32 is not None else 'NULL'}', 'm3', 2024, 'GHG EF from Cross-Sector Tools', 'Sheet Name : Stationary combustion', '{'kgCO₂' if co2m32 is not None else 'NULL'}', '{'kgCH₄' if ch4m32 is not None else 'NULL'}', '{'kgN₂O' if n2om32 is not None else 'NULL'}', {sourceId}, '{ever_table}', '{', '.join(str(x) for x in everIds)}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, type, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del co2m32, ch4m32, n2om32, everIds, _type, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            if co2liter2 is not None or ch4liter2 is not None or n2oliter2 is not None:
                logger.debug(f"{co2liter2}, {ch4liter2}, {n2oliter2}")
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                everIds = []
                if co2liter2 is not None:
                    everIds.append(co2everId)
                if ch4liter2 is not None:
                    everIds.append(ch4everId)
                if n2oliter2 is not None:
                    everIds.append(n2oeverId)

                _type = "Liquid Basis"
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{_type}', '{co2liter2 if co2liter2 is not None else 'NULL'}', '{ch4liter2 if ch4liter2 is not None else 'NULL'}', '{n2oliter2 if n2oliter2 is not None else 'NULL'}', 'liter2', 2024, 'GHG EF from Cross-Sector Tools', 'Sheet Name : Stationary combustion', '{'kgCO₂' if co2liter2 is not None else 'NULL'}', '{'kgCH₄' if ch4liter2 is not None else 'NULL'}', '{'kgN₂O' if n2oliter2 is not None else 'NULL'}', {sourceId}, '{ever_table}', '{', '.join(str(x) for x in everIds)}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, type, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del co2liter2, ch4liter2, n2oliter2, everIds, _type, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)