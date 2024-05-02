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
        source = "IGES"
        
        ever_schema = 'ever'
        ever_table = 'IGES_rawData'

        write_schema = 'mgmtCarbon'
        write_table = 'EF2'
        sourceTable = 'EFsource2'

        sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
        util.resetLatestId(code, write_schema, sourceTable, cursor)

        sqlCommand = f"""
            SELECT * FROM {ever_schema}.{ever_table};
        """

        cursor.execute(sqlCommand)
        # ['2019.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power', '2010.0', '2011.0', '2012.0', '2013.0', '2014.0', '2015.0', '2016.0', '2017.0', '2018.0', '2020.0', '2021.0', '2009.0', '2006.0', '2007.0', '2008.0', '2022.0', '2005.0']
        
        keys = []

        for ID, rawData, year, updateTime in cursor.fetchall():
            if "null" in rawData:
                rawData = rawData.replace('null', '"NULL"')

            rawData = eval(rawData)

            baseUnit = 'kWh'

            Country = rawData["Country"]
            ForRegionGrid = rawData["For Region/Grid"]
            LatestGEFpublishedAdoptedBy = rawData["Latest GEF published/adopted by"]
            DataVintage = rawData["Data vintage"]
            Method = rawData["Method"]

            sourceOfEmission = Country + ' - ' + ForRegionGrid + ' - ' + LatestGEFpublishedAdoptedBy 
            if DataVintage and DataVintage.lower() != 'null':
                sourceOfEmission += ' - ' + DataVintage 
            if Method and Method.lower() != 'null':
                sourceOfEmission += ' - ' + Method
            
            everId = ID

            co2Unit = 'kgCO₂'

            co2_2005 = rawData.get('2005.0')
            co2_2006 = rawData.get('2006.0')
            co2_2007 = rawData.get('2007.0')
            co2_2008 = rawData.get('2008.0')
            co2_2009 = rawData.get('2009.0')
            co2_2010 = rawData.get('2010.0')
            co2_2011 = rawData.get('2011.0')
            co2_2012 = rawData.get('2012.0')
            co2_2013 = rawData.get('2013.0')
            co2_2014 = rawData.get('2014.0')
            co2_2015 = rawData.get('2015.0')
            co2_2016 = rawData.get('2016.0')
            co2_2017 = rawData.get('2017.0')
            co2_2018 = rawData.get('2018.0')
            co2_2019 = rawData.get('2019.0')
            co2_2020 = rawData.get('2020.0')
            co2_2021 = rawData.get('2021.0')
            co2_2022 = rawData.get('2022.0')

            latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)
            logger.debug(everId)
            if co2_2005 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2005}, '{baseUnit}', 2005, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql

            if co2_2006 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2006}, '{baseUnit}', 2006, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql

            if co2_2007 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2007}, '{baseUnit}', 2007, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql

            if co2_2008 != "NULL":
                co2_2008 = str(co2_2008).replace(' ', '')
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2008}, '{baseUnit}', 2008, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2009 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2009}, '{baseUnit}', 2009, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2010 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2011 != "NULL" and co2_2011 != 'N/A ':
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2011}, '{baseUnit}', 2011, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2012 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2013 != "NULL" and co2_2013 != '-':
                if type(co2_2013) == str:
                    if ' ' in co2_2013:
                        co2_2013 = co2_2013.replace(' ', '')
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2013}, '{baseUnit}', 2013, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2014 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2014}, '{baseUnit}', 2014, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2015 != "NULL" and co2_2015 != 'N/A ':
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2015}, '{baseUnit}', 2015, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2016 != "NULL" and co2_2016 != 'N/A ':
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2016}, '{baseUnit}', 2016, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2017 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2017}, '{baseUnit}', 2017, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2018 != "NULL":
                if co2_2018 == '0.87950,9465':
                    co2_2018 = '0.8795'
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2018}, '{baseUnit}', 2018, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2019 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2019}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2020 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2020}, '{baseUnit}', 2020, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2021 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2021}, '{baseUnit}', 2021, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2022 != "NULL":
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2022}, '{baseUnit}', 2022, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)
                

                del replace_sql

            util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)
    conn.close()