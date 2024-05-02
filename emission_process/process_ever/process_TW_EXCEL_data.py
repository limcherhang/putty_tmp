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
        ever_table = 'TW_EXCEL_data'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)
        # ['備註', '數值', '熱值', '原始係數', '數值-單位', 'CH4排放係數', '熱值資料來源', '原始係數-單位', '熱值-熱值單位', 'CH4排放係數-單位', '數值-95%信賴區間上限', '數值-95%信賴區間下限', 'CH4排放係數-95%信賴區間上限', 'CH4排放係數-95%信賴區間下限'], 
        # ['數值', '熱值', '原始係數', 'C排放係數', '數值-單位', 'CO2排放係數', '碳氧化因子', '熱值變更時間', '熱值資料來源', '原始係數-單位', '熱值-熱值單位', 'C排放係數-單位', 'CO2排放係數-單位', '數值-95%信賴區間上限', '數值-95%信賴區間下限', 'CO2排放係數-95%信賴區間上限', 'CO2排放係數-95%信賴區間下限'], 
        # ['備註', '數值', '熱值', '原始係數', '數值-單位', 'N2O排放係數', '熱值資料來源', '原始係數-單位', '熱值-熱值單位', 'N2O排放係數-單位', '數值-95%信賴區間上限', '數值-95%信賴區間下限', 'N2O排放係數-95%信賴區間上限', 'N2O排放係數-95%信賴區間下限']
        EF_data = {"固定源": {}, "移動源": {}}
        for GasType, Emission, EmissionCategory, FuelType, IPCCoriginalCoefficientNames, rawdata in cursor.fetchall():
            sourceOfEmission = IPCCoriginalCoefficientNames
            if FuelType == '自產煤':
                sourceOfEmission += ' 1'
            elif FuelType == '原料煤':
                sourceOfEmission += ' 2'
            elif FuelType == '煙煤':
                sourceOfEmission += ' 3'
            elif sourceOfEmission == 'Liquefied Petroleum Gases':
                sourceOfEmission = 'Liquefied Petroleum Gases (LPG)'
            elif sourceOfEmission == 'Liquefied Natural Gas':
                sourceOfEmission = 'Liquefied Natural Gas (LNG)'
            elif sourceOfEmission == 'Aviation Gasoline (Jet Gasoline)':
                sourceOfEmission = 'Aviation Gasoline(Jet Gasoline)'
            elif FuelType == '亞煙煤(發電)':
                sourceOfEmission += "(electricity)"
            elif FuelType == '亞煙煤(其他)':
                sourceOfEmission += "(other)"
            elif sourceOfEmission == 'Natural Gas Liquids':
                sourceOfEmission = 'Natural Gas Liquids (NGLs)'


            rawdata = eval(rawdata)
            if rawdata['數值'] == '-':
                continue

            if sourceOfEmission not in EF_data[Emission]:
                EF_data[Emission][sourceOfEmission] = {"CO2Unit": "kgCO₂", "CH4Unit": "kgCH₄", "N2OUnit": "kgN₂O"}

            EF_data[Emission][sourceOfEmission][GasType] = rawdata['數值']

            unit = rawdata['數值-單位'].split('/')[-1]

            EF_data[Emission][sourceOfEmission]["baseUnit"] = unit

        # {'CO2Unit': 'kgCO₂', 'CH4Unit': 'kgCH₄', 'N2OUnit': 'kgN₂O', 'CH4': 0.000137159568, 'baseUnit': 'L', 'CO2': 2.6060317920000005, 'N2O': 0.000137159568}
        for emission in EF_data.values():
            for sourceOfEmission, efInfo in emission.items():
                everId = sourceOfEmission
                latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)
                
                CO2Unit = efInfo["CO2Unit"]
                CH4Unit = efInfo["CH4Unit"]
                N2OUnit = efInfo["N2OUnit"]

                CO2 = efInfo['CO2']
                CH4 = efInfo['CH4']
                N2O = efInfo["N2O"]

                baseUnit = efInfo['baseUnit']

                EFId = code+f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {CO2}, {CH4}, {N2O}, '{baseUnit}', 2023, '{CO2Unit}', '{CH4Unit}', '{N2OUnit}', {sourceId}, '{ever_table}', '{everId}')"


                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del CO2Unit, CH4Unit, N2OUnit, CO2, CH4, N2O, baseUnit, replace_sql
                util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)