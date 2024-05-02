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
from pygoogletranslation import Translator

def has_non_alphanumeric(input_string):
    # 使用正则表达式查找非英文和非数字的字符
    pattern = re.compile(r'[^a-zA-Z0-9]')
    if pattern.search(input_string):
        return "yes"
    else:
        return "no"

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

def process_rawData(rawData, dictionary):

    rawData = eval(rawData)

    new_rawData = {}

    for key, value in rawData.items():
        if key in dictionary:
            key = dictionary[key]
        
        new_rawData[key] = value
    return new_rawData


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

    translator = Translator()

    with conn.cursor() as cursor:
        source = "ThaiWah"
        
        ever_schema = 'ever'
        ever_table = 'EF_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = getSourceId(source, write_schema, sourceTable, cursor)
        resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"SELECT * FROM {ever_schema}.{ever_table}"

        cursor.execute(sqlCommand)

        dictionary = {
            "ชอื่": "name",
            "แหลง่อา้งองิขอ้มูล": "source",
            "หนว่ ย": "unit", 
            "ล าดบั ท ี่": "numberOfOrder",
            "รายละเอยี ด": "details",
            "วนั ทอี่ พั เดท": "date",
            "แหลง่ ขอ้ มลู อา้ งองิ": "dataSourceFromMaluAhNgi",
            "คา่ แฟคเตอร ์(kgCO2e/หนว่ ย)": "EF(kgCO2e/unit)",
        }
        """
        [['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq/unit)', 'source'], 
        ['name', 'unit', 'numberOfOrder', 'details', 'date', 'dataSourceFromMaluAhNgi', 'EF(kgCO2e/unit)'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO O2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2e eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'To otal(kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq/un nit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq q/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Tota al(kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total( (kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(k kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit t)', 'Total(kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2 2O/unit)', 'Total(kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'CH4(k kgCH4/unit)', 'Total(kgCO2eq/unit)', 'source'], 
        ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'T Total(kgCO2eq/unit)', 'source']]
        """       
        for id, _type, update, rawData in  cursor.fetchall():

            everId = f"{id} - {_type}"
            
            rawData = process_rawData(rawData, dictionary)

            key = [k for k in rawData]
            
            if key == ['Units', 'name', 'CH4(kgCH4/unit)', 'CO2(kgCO2/unit)', 'N2O(kgN2O/unit)', 'Total(kgCO2eq/unit)', 'source']:
                # {'Units': 'scf', 'name': 'Natural gas', 'CH4(kgCH4/unit)': '1.02E-06', 'CO2(kgCO2/unit)': '5.72E-02', 'N2O(kgN2O/unit)': '1.02E-07', 'Total(kgCO2eq/unit)': '0.0573', 'source': 'IPCC Vol.2 table 2.2, DEDE'}
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                sourceOfEmission = rawData['name']
                ch4Factor, ch4Unit = rawData['CH4(kgCH4/unit)'], "kgCH₄"
                co2Factor, co2Unit = rawData['CO2(kgCO2/unit)'], "kgCO₂"
                n2oFactor, n2oUnit = rawData['N2O(kgN2O/unit)'], "kgN₂O"
                if n2oFactor == 'NULL L':
                    n2oFactor == "NULL"
                baseUnit = rawData['Units']

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factor if co2Factor != '-' else 'NULL'}', '{ch4Factor if ch4Factor != '-' else 'NULL'}', '{n2oFactor if n2oFactor != '-' else 'NULL'}', '{baseUnit}', 2022, '{co2Unit}', '{ch4Unit}', '{n2oUnit}', {sourceId}, '{ever_table}', '{everId}')".replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year,CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"
                
                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.debug(rawData)
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del baseUnit, sourceOfEmission, co2Factor, ch4Factor, n2oFactor, co2Unit, ch4Unit, n2oUnit, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            elif key == ['name', 'unit', 'numberOfOrder', 'details', 'date', 'dataSourceFromMaluAhNgi', 'EF(kgCO2e/unit)']: 
                # {'name': 'Acrylonitrile Butadiene Styrene(ABS)', 'unit': 'kg', 'numberOfOrder': '1.', 'details': 'ผลติ จากกระบวนการอัลคลิ เลชนั ของเบนซนี และเอทลี นี ; LCIA method IPCC 2013 GWP 100aV1.03', 'date': 'Update_Dec2019', 'dataSourceFromMaluAhNgi': 'Thai National LCI Database,TIIS-MTEC-NSTDA(with TGO electricity 2016-2018)', 'EF(kgCO2e/unit)': '4.1597'}

                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                sourceOfEmission = rawData["name"]
                baseUnit = rawData["unit"]
                date = rawData['date'][-4:]
                # if date in ('Update_24Sep12'):
                #     date = '2024'
                # elif date in ("Update_09Apr15"):
                #     date = '2015'
                # elif date in ("Update_24Aug11"):
                #     date = '2011'
                # else:

                try:
                    date = int(date[-4:])
                except:
                    date = 2000 + int(date[-2:])
                co2Factor, co2Unit = rawData['EF(kgCO2e/unit)'], "kgCO₂"

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factor if co2Factor != '-' else 'NULL'}', '{baseUnit}', {date}, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES {value_string}".replace("'NULL'", "NULL")

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del baseUnit, sourceOfEmission, co2Factor, co2Unit, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)