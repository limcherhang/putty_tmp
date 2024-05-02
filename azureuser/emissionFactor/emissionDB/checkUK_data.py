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
    
    loss_id = []

    with conn.cursor() as cursor:
        sqlCommand = f"""
            SELECT 
                LEFT(ID, LENGTH(ID) - 2) AS newID,
                Scope, Level1, Level2, 
                Level3, Level4, ColumnText, UOM AS baseUnit,
                GROUP_CONCAT(`GHG/Unit` SEPARATOR '|') as emissionUnits, -- √
                GROUP_CONCAT(ROUND(GHGConversionFactor,5) SEPARATOR '|') AS GHGConversionFactors -- √
            FROM
                ever.UK_data WHERE Scope='Scope 1' GROUP BY newID;
        """
        # Units: ['kg CO2e', 'kg CO2e of CO2 per unit', 'kg CO2e of CH4 per unit', 'kg CO2e of N2O per unit', 'kWh (Net CV)', 'kWh (net)']

        cursor.execute(sqlCommand)
       
        """for (ID, Scopes, Level1s, Level2s, Level3s, Level4s, ColumnTexts, baseUnits, GHGUnits, GHGConversionFactors) in cursor.fetchall():
            Scope = set(Scopes.split(','))
            Level1 = set(Level1s.split(','))
            Level2 = set(Level2s.split(','))
            if Level3s:
                Level3 = set(Level3s.split(','))
            else:
                Level3 = None
            if Level4s:
                Level4 = set(Level4s.split(','))
            else:
                Level4 = None
            if ColumnTexts:
                ColumnText = set(ColumnTexts.split(','))
            else:
                ColumnText = None
            baseUnit = set(baseUnits.split(','))
            GHGUnit = set(GHGUnits.split(','))
            if GHGConversionFactors:
                GHGConversionFactor = set(GHGConversionFactors.split(','))
            else:
                GHGConversionFactor = None

            logger.debug(f"{ID}")
            if len(Level1) > 1:
                logger.debug(f"Level1: {Level1}")
            if len(Level2) > 1:
                logger.debug(f"Level2: {Level2}")
            if Level3s:
                if len(Level3) > 1:
                    logger.debug(f"Level3: {Level3}")
            if Level4s:
                if len(Level4) > 1:
                    logger.debug(f"Level4: {Level4}")
            if ColumnTexts:
                if len(ColumnText) > 1:
                    logger.debug(f"ColumnText: {ColumnText}")
            if len(baseUnit) > 1:
                logger.debug(f"baseUnit: {baseUnit}")
            if len(GHGUnit) > 1:
                logger.debug(f"GHGUnit: {GHGUnit}")
            if GHGConversionFactors:
                if len(GHGConversionFactor) > 1:
                    logger.debug(f"GHGConversionFactor: {GHGConversionFactor}")
            logger.debug("")
        """

        for (ID, Scope, Level1, Level2, Level3, Level4, ColumnText, Unit, GHGUnits, GHGConversionFactors) in cursor.fetchall():
            if Scope == 'Scope 1':
                # logger.debug(ColumnText)
                sourceOfEmission = ColumnText if ColumnText else Level3
                vehicleType = Level2 + ' - ' + Level3 if ColumnText else None
                
                GHGUnits = GHGUnits.split('|')
                if GHGConversionFactors:
                    GHGConversionFactors = GHGConversionFactors.split('|')
                else:
                    GHGConversionFactors = [None for i in range(len(GHGUnits))]
                
                co2Factor = None
                n2oFactor = None
                ch4Factor = None
                if len(GHGUnits) == 1:
                    co2Factor = GHGConversionFactors[0]
                else:
                    for i in range(len(GHGUnits)):
                        if "CH4" in GHGUnits[i]:
                            ch4Factor = GHGConversionFactors[i]
                        elif "N2O" in GHGUnits[i]:
                            n2oFactor = GHGConversionFactors[i]
                        elif GHGUnits[i] in ("kg CO2e of CO2 per unit", "kWh (Net CV)", "kWh (net)"):
                            co2Factor = GHGConversionFactors[i]
                Scope = "scope1"

                if Unit == 'tonnes':
                    baseUnit = 'tonne'
                elif Unit == 'litres':
                    baseUnit = 'Liter'
                elif "kwh" in Unit.lower():
                    continue
                else:
                    baseUnit = Unit

                sqlCommand = f"""
                    SELECT EF.*
                    FROM mgmtCarbon.EF 
                    JOIN mgmtCarbon.EFsource ON EF.sourceId = EFsource.sourceId
                    WHERE EFsource.name='DEFRA' AND EF.name='{sourceOfEmission}' AND EF.unit='{baseUnit}' 
                """
                if vehicleType:
                    sqlCommand+= f"AND vehicleType='{vehicleType}'"
                # logger.debug(sqlCommand)

                cursor.execute(sqlCommand)
                result = cursor.fetchone()
                if result is None:
                    logger.debug(sqlCommand)
                    logger.debug(co2Factor)
                    loss_id.append(ID)
                
                else:
                    EFId, companyId, name, vehicleType, fuelEfficiency, CO2, CH4, N2O, unit, type, year, urlName, sheetName, CO2Unit, CH4Unit, N2OUnit, sourceId, typeId, everId = result

                    replace_sql = f"""
                        REPLACE INTO mgmtCarbon.EF VALUES ('{EFId}', '{companyId}', '{name}', '{vehicleType}', '{fuelEfficiency}', '{CO2}', '{CH4}', '{N2O}', '{unit}', '{type}', '{year}', '{urlName}', '{sheetName}', '{CO2Unit}', '{CH4Unit}', '{N2OUnit}', '{sourceId}', '{typeId}', '{ID}')
                    """.replace("'None'", "NULL")

                    # logger.debug(replace_sql)

                    cursor.execute(replace_sql)

        logger.debug(f"Loss Id in MongoDB: {loss_id}")
    conn.close()