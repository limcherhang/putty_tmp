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
        source = "EPA(US)"
        
        ever_schema = 'ever'
        ever_table = 'EPA_rawData'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = getSourceId(source, write_schema, sourceTable, cursor)
        resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)

        # ['Gas', '100-YearGWP'], √
        # ['FuelType', 'gCH4permmBtu', 'gN2OpermmBtu', 'kgCO2permmBtu', 'gCH4pershortton', 'gN2Opershortton', 'kgCO2pershortton', 'mmBtupershortton'],  √
        # ['Unit', 'FuelType', 'kgCO2perunit'],  √
        # ['ModelYear', 'VehicleType', 'CH4Factor(gCH4/vehicle-mile)', 'N2OFactor(gN2O/vehicle-mile)'],  √
        # ['FuelType', 'ModelYear', 'VehicleType', 'CH4Factor(gCH4/vehicle-mile)', 'N2OFactor(gN2O/vehicle-mile)'],  √
        # ['FuelType', 'VehicleType', 'CH4Factor(gCH4/gallon)', 'N2OFactor(gN2O/gallon)'],  √
        # ['CH4Factor', 'CO2Factor', 'N2OFactor', 'eGRIDSubregionName', 'eGRIDSubregionAcronym'], √
        # ['FuelType', 'CH4Factor(gCH4/mmBtu)', 'N2OFactor(gN2O/mmBtu)', 'CO2Factor(kgCO2/mmBtu)'], √
        # ['Units', 'VehicleType', 'CH4Factor(gCH4/unit)', 'N2OFactor(gN2O/unit)', 'CO2Factor(kgCO2/unit)'], √
        # ['Material', 'RecycledA', 'CombustedC', 'CompostedD', 'LandfilledB', 'AnaerobicallyDigested(DryDigestatewithCuring)', 'AnaerobicallyDigested(WetDigestatewithCuring)'], √
        # ['100-YearGWP', 'ChemicalFormula', 'IndustrialDesignationorCommonName'], 
        # ['ASHRAE#', '100-YearGWP', 'BlendComposition']
        """
        {'Gas': sourceOfEmission, '100-YearGWP': co2Factor} skip!
        {'FuelType': sourceOfEmission, 'gCH4permmBtu': ch4Factormmbtu, 'gN2OpermmBtu': n2oFactormmbtu, 'kgCO2permmBtu': co2Factormmbtu, 'gCH4pershortton': ch4Factorshortton, 'gN2Opershortton': n2oFactorshortton, 'kgCO2pershortton': co2Factorshortton, 'mmBtupershortton']
        {'Unit': baseUnit, 'FuelType': sourceOfEmission, 'kgCO2perunit': co2Factor}
        {'ModelYear': , 'VehicleType': vehicleType, 'CH4Factor(gCH4/vehicle-mile)': ch4Factor, 'N2OFactor(gN2O/vehicle-mile)': n2oFactor}
        {'FuelType': sourceOfEmission, 'ModelYear', 'VehicleType', 'CH4Factor(gCH4/vehicle-mile)', 'N2OFactor(gN2O/vehicle-mile)'}
        {'FuelType': sourceOfEmission, 'VehicleType': vehicleType, 'CH4Factor(gCH4/gallon)': ch4Factor, 'N2OFactor(gN2O/gallon)': n2oFactor}
        {'CH4Factor': ch4Factor, 'CO2Factor': co2Factor, 'N2OFactor': n2oFactor, 'eGRIDSubregionName': sourceOfEmission(?), 'eGRIDSubregionAcronym'}
        {'FuelType': sourceOfEmission, 'CH4Factor(gCH4/mmBtu)': ch4Factor, 'N2OFactor(gN2O/mmBtu)': n2oFactor, 'CO2Factor(kgCO2/mmBtu)': co2Factor}
        {'Units': baseUnit, 'VehicleType': vehicleType, 'CH4Factor(gCH4/unit)': ch4Factor, 'N2OFactor(gN2O/unit)': n2oFactor, 'CO2Factor(kgCO2/unit)': co2Factor}
        {'Material': sourceOfEmission, 'RecycledA': co2FactorRecycled, 'CombustedC': co2FactorCombusted, 'CompostedD': co2FactorComposted, 'LandfilledB': co2FactorLandfilled, 'AnaerobicallyDigested(DryDigestatewithCuring)': typeOfEmission(out of methods), 'AnaerobicallyDigested(WetDigestatewithCuring)'}
        {'100-YearGWP': co2Factor, 'ChemicalFormula', 'IndustrialDesignationorCommonName': sourceOfEmission}
        {'ASHRAE#': sourceOfEmission, '100-YearGWP': co2Factor, 'BlendComposition'}
        """
        for ID, TableName, DBts, rawData in cursor.fetchall():        
            everId = ID

            rawData = eval(rawData)

            key = [k for k in rawData]

            if key == ['Gas', '100-YearGWP'] or key == ['100-YearGWP', 'ChemicalFormula', 'IndustrialDesignationorCommonName'] or key == ['ASHRAE#', '100-YearGWP', 'BlendComposition']:
                continue
            elif key == ['FuelType', 'gCH4permmBtu', 'gN2OpermmBtu', 'kgCO2permmBtu', 'gCH4pershortton', 'gN2Opershortton', 'kgCO2pershortton', 'mmBtupershortton']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)
                
                
                sourceOfEmission = rawData['FuelType']
                
                ch4Factormmbtu = rawData['gCH4permmBtu']/1000 if type(rawData['gCH4permmBtu']) != str else None
                n2oFactormmbtu = rawData['gN2OpermmBtu']/1000 if type(rawData['gN2OpermmBtu']) != str else None
                co2Factormmbtu = rawData['kgCO2permmBtu'] if type(rawData['kgCO2permmBtu']) != str else None
                
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factormmbtu}', '{ch4Factormmbtu}', '{n2oFactormmbtu}', 'mmBtu', '2024', 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                latestId += 1

                ch4Factorshortton = rawData['gCH4pershortton']/1000 if type(rawData['gCH4pershortton']) != str else None
                n2oFactorshortton = rawData['gN2Opershortton']/1000 if type(rawData['gN2Opershortton']) != str else None
                co2Factorshortton = rawData['kgCO2pershortton'] if type(rawData['kgCO2pershortton']) != str else None

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factorshortton}', '{ch4Factorshortton}', '{n2oFactorshortton}', 'short tons', '2024', 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                # logger.debug(replace_sql)
                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del sourceOfEmission, ch4Factormmbtu, n2oFactormmbtu, co2Factormmbtu, ch4Factorshortton, n2oFactorshortton, co2Factorshortton, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            elif key == ['Unit', 'FuelType', 'kgCO2perunit']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                baseUnit = rawData['Unit']
                sourceOfEmission = rawData['FuelType']
                co2Factor = rawData['kgCO2perunit']

                EFId = code+f"{latestId:05}"

                if baseUnit == 'gallon':
                    baseUnit = 'gal (US)'

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factor}', '{baseUnit}', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del baseUnit, sourceOfEmission, co2Factor, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            elif key == ['ModelYear', 'VehicleType', 'CH4Factor(gCH4/vehicle-mile)', 'N2OFactor(gN2O/vehicle-mile)']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)
                
                vehicleType = rawData['VehicleType']
                sourceOfEmission = vehicleType + f"{rawData['ModelYear']}"
                
                ch4Factor = rawData['CH4Factor(gCH4/vehicle-mile)']
                n2oFactor = rawData['N2OFactor(gN2O/vehicle-mile)']

                if type(ch4Factor) != str:
                    ch4Factor = ch4Factor / 1000
                elif ch4Factor == "0 ":
                    ch4Factor = 0
                else:
                    ch4Factor = None
                
                if type(n2oFactor) != str:
                    n2oFactor = n2oFactor / 1000
                elif n2oFactor == "0 ":
                    n2oFactor = 0
                else:
                    logger.error(n2oFactor)
                    n2oFactor = None
                
                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{vehicleType}', '{ch4Factor}', '{n2oFactor}', 'vehicle-mile', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, vehicleType, CH4, N2O, unit, year, urlName, sheetName, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del sourceOfEmission, vehicleType, ch4Factor, n2oFactor, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            elif key == ['FuelType', 'ModelYear', 'VehicleType', 'CH4Factor(gCH4/vehicle-mile)', 'N2OFactor(gN2O/vehicle-mile)']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)
                
                ModelYear = rawData['ModelYear']
                if ModelYear.lower() != 'null':
                    sourceOfEmission = rawData['FuelType'] + ' - ' + ModelYear
                else:
                    sourceOfEmission = rawData['FuelType'] + ' - ' + "Unknown Vehicle Year"
                vehicleType = rawData['VehicleType']

                ch4Factor = rawData['CH4Factor(gCH4/vehicle-mile)']/1000
                n2oFactor = rawData['N2OFactor(gN2O/vehicle-mile)']/1000

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{vehicleType}', '{ch4Factor}', '{n2oFactor}', 'vehicle-mile', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, vehicleType, CH4, N2O, unit, year, urlName, sheetName, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del ModelYear, sourceOfEmission, vehicleType, ch4Factor, n2oFactor, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            elif key == ['FuelType', 'VehicleType', 'CH4Factor(gCH4/gallon)', 'N2OFactor(gN2O/gallon)']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                vehicleType = rawData['VehicleType']
                sourceOfEmission = rawData['FuelType'] + ' - ' + vehicleType


                ch4Factor = rawData['CH4Factor(gCH4/gallon)']
                n2oFactor = rawData['N2OFactor(gN2O/gallon)']

                if type(ch4Factor) != str:
                    ch4Factor = ch4Factor / 1000
                elif ch4Factor == "0 ":
                    ch4Factor = 0
                else:
                    ch4Factor = None
                
                if type(n2oFactor) != str:
                    n2oFactor = n2oFactor / 1000
                elif n2oFactor == "0 ":
                    n2oFactor = 0
                else:
                    logger.error(n2oFactor)
                    n2oFactor = None

                EFId = code+f"{latestId:05}"
                
                value_string = f"('{EFId}', '{sourceOfEmission}', '{vehicleType}', '{ch4Factor}', '{n2oFactor}', 'gal(US)', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, vehicleType, CH4, N2O, unit, year, urlName, sheetName, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del vehicleType, sourceOfEmission, ch4Factor, n2oFactor, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            elif key == ['CH4Factor', 'CO2Factor', 'N2OFactor', 'eGRIDSubregionName', 'eGRIDSubregionAcronym']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                sourceOfEmission = rawData['eGRIDSubregionName'] + '(' + rawData['eGRIDSubregionAcronym'] + ')'

                co2Factor = rawData['CO2Factor']
                ch4Factor = rawData['CH4Factor']
                n2oFactor = rawData['N2OFactor']

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factor}', '{ch4Factor}', '{n2oFactor}', 'kwh', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del sourceOfEmission, co2Factor, ch4Factor, n2oFactor, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            elif key == ['FuelType', 'CH4Factor(gCH4/mmBtu)', 'N2OFactor(gN2O/mmBtu)', 'CO2Factor(kgCO2/mmBtu)']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                sourceOfEmission = rawData['FuelType']

                co2Factor = rawData['CO2Factor(kgCO2/mmBtu)']
                ch4Factor = rawData['CH4Factor(gCH4/mmBtu)']
                n2oFactor = rawData['N2OFactor(gN2O/mmBtu)']

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factor}', '{ch4Factor}', '{n2oFactor}', 'mmBtu', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del sourceOfEmission, co2Factor, ch4Factor, n2oFactor, value_string, replace_sql
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            elif key == ['Units', 'VehicleType', 'CH4Factor(gCH4/unit)', 'N2OFactor(gN2O/unit)', 'CO2Factor(kgCO2/unit)']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                baseUnit = rawData['Units']
                sourceOfEmission = rawData['VehicleType']
                
                ch4Factor, co2Factor, n2oFactor = rawData['CH4Factor(gCH4/unit)'], rawData['CO2Factor(kgCO2/unit)'], rawData['N2OFactor(gN2O/unit)']

                EFId = code+f"{latestId:05}"

                value_string = f"('{EFId}', '{sourceOfEmission}', '{co2Factor}', '{ch4Factor}', '{n2oFactor}', '{baseUnit}', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 'kgCH₄', 'kgN₂O', 1, '{ever_table}', '{everId}')".replace("'None'", "NULL").replace("'NULL'", "NULL")

                replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, CH4, N2O, unit, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, everTable, everId) VALUES {value_string}"

                try:
                    cursor.execute(replace_sql)
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                latestId+=1

                del sourceOfEmission, co2Factor, ch4Factor, n2oFactor, value_string, replace_sql, baseUnit
                updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            elif key == ['Material', 'RecycledA', 'CombustedC', 'CompostedD', 'LandfilledB', 'AnaerobicallyDigested(DryDigestatewithCuring)', 'AnaerobicallyDigested(WetDigestatewithCuring)']:
                latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                material = rawData['Material']
                co2recycled = rawData['RecycledA']
                co2combusted = rawData['CombustedC']
                co2composted = rawData['CompostedD']
                co2landfilled = rawData['LandfilledB']
                co2dry = rawData['AnaerobicallyDigested(DryDigestatewithCuring)']
                co2wet = rawData['AnaerobicallyDigested(WetDigestatewithCuring)']

                if co2recycled != 'Null':
                    latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                    sourceOfEmission = material + ' - Recycled'

                    EFId = code+f"{latestId:05}"

                    value_string = f"('{EFId}', '{sourceOfEmission}', '{co2recycled}', 'short tons', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                    replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                    try:
                        cursor.execute(replace_sql)
                    except Exception as ex:
                        logger.error(f"Failed Replace {ex}")
                        logger.error(replace_sql)
                    latestId+=1

                    del sourceOfEmission, value_string, replace_sql
                    updateLatestId(code, latestId, write_schema, sourceTable, cursor)

                if co2combusted != 'Null':
                    latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                    sourceOfEmission = material + ' - Combusted'

                    EFId = code+f"{latestId:05}"

                    value_string = f"('{EFId}', '{sourceOfEmission}', '{co2combusted}', 'short tons', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                    replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                    try:
                        cursor.execute(replace_sql)
                    except Exception as ex:
                        logger.error(f"Failed Replace {ex}")
                        logger.error(replace_sql)
                    latestId+=1

                    del sourceOfEmission, value_string, replace_sql
                    updateLatestId(code, latestId, write_schema, sourceTable, cursor)

                if co2composted != 'Null':
                    latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                    sourceOfEmission = material + ' - Composted'
                    
                    EFId = code+f"{latestId:05}"

                    value_string = f"('{EFId}', '{sourceOfEmission}', '{co2composted}', 'short tons', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                    replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                    try:
                        cursor.execute(replace_sql)
                    except Exception as ex:
                        logger.error(f"Failed Replace {ex}")
                        logger.error(replace_sql)
                    latestId+=1

                    del sourceOfEmission, value_string, replace_sql
                    updateLatestId(code, latestId, write_schema, sourceTable, cursor)

                if co2landfilled != 'Null':
                    latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                    sourceOfEmission = material + ' - Landfilled'
                    
                    EFId = code+f"{latestId:05}"

                    value_string = f"('{EFId}', '{sourceOfEmission}', '{co2landfilled}', 'short tons', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                    replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                    try:
                        cursor.execute(replace_sql)
                    except Exception as ex:
                        logger.error(f"Failed Replace {ex}")
                        logger.error(replace_sql)
                    latestId+=1

                    del sourceOfEmission, value_string, replace_sql
                    updateLatestId(code, latestId, write_schema, sourceTable, cursor)

                if co2dry != 'Null': 
                    latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                    sourceOfEmission = material + ' - Anaerobically Digested (Dry Digestate with Curing)'
                    
                    EFId = code+f"{latestId:05}"

                    value_string = f"('{EFId}', '{sourceOfEmission}', '{co2dry}', 'short tons', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                    replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                    try:
                        cursor.execute(replace_sql)
                    except Exception as ex:
                        logger.error(f"Failed Replace {ex}")
                        logger.error(replace_sql)
                    latestId+=1

                    del sourceOfEmission, value_string, replace_sql
                    updateLatestId(code, latestId, write_schema, sourceTable, cursor)
                
                if co2wet != 'Null':
                    latestId = getLatestId(sourceId, write_schema, sourceTable, cursor)

                    sourceOfEmission = material + ' - Anaerobically Digested (Wet Digestate with Curing)'
                    
                    EFId = code+f"{latestId:05}"

                    value_string = f"('{EFId}', '{sourceOfEmission}', '{co2wet}', 'short tons', 2024, 'EPA GHG Emission Factors Hub', 'Sheet Name : Emission Factors Hub', 'kgCO₂', 1, '{ever_table}', '{everId}')"

                    replace_sql = f"REPLACE INTO {write_schema}.{write_table}(EFId, name, CO2, unit, year, urlName, sheetName, CO2Unit, sourceId, everTable, everId) VALUES {value_string}"

                    try:
                        cursor.execute(replace_sql)
                    except Exception as ex:
                        logger.error(f"Failed Replace {ex}")
                        logger.error(replace_sql)
                    latestId+=1

                    del sourceOfEmission, value_string, replace_sql
                    updateLatestId(code, latestId, write_schema, sourceTable, cursor)

        