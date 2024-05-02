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
        ever_table = 'IGES_data'

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
        
        forCount = 0
        
        for Country, ForRegionGrid, LatestGEFpublishedAdoptedBy, DataVintage, Method, rawData in cursor.fetchall():
            rawData = eval(rawData)

            baseUnit = 'kWh'

            sourceOfEmission = Country + ' - ' + ForRegionGrid + ' - ' + LatestGEFpublishedAdoptedBy 
            if DataVintage and DataVintage.lower() != 'null':
                sourceOfEmission += ' - ' + DataVintage 
            if Method and Method.lower() != 'null':
                sourceOfEmission += ' - ' + Method
            
            everId = sourceOfEmission

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

           
            if co2_2005:
                forCount += 1
            if co2_2006:
                forCount += 1
            if co2_2007:
                forCount += 1
            if co2_2008:
                forCount += 1
            if co2_2009:
                forCount += 1
            if co2_2010:
                forCount += 1
            if co2_2011:
                forCount += 1
            if co2_2012:
                forCount += 1
            if co2_2013:
                forCount += 1
            if co2_2014:
                forCount += 1
            if co2_2015:
                forCount += 1
            if co2_2016:
                forCount += 1
            if co2_2017:
                forCount += 1
            if co2_2018:
                forCount += 1
            if co2_2019:
                forCount += 1
            if co2_2020:
                forCount += 1
            if co2_2021:
                forCount += 1
            if co2_2022:
                forCount += 1


            latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)
            
            if co2_2005:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2005}, '{baseUnit}', 2005, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql

            if co2_2006:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2006}, '{baseUnit}', 2006, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql

            if co2_2007:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2007}, '{baseUnit}', 2007, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql

            if co2_2008:
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
            
            if co2_2009:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2009}, '{baseUnit}', 2009, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2010:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2011 and co2_2011 != 'N/A ':
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2011}, '{baseUnit}', 2011, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2012:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2013 and co2_2013 != '-':
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
            
            if co2_2014:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2014}, '{baseUnit}', 2014, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2015 and co2_2015 != 'N/A ':
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2015}, '{baseUnit}', 2015, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2016 and co2_2016 != 'N/A ':
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2016}, '{baseUnit}', 2016, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2017:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2017}, '{baseUnit}', 2017, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2018:
                if co2_2018 == '"0.8795\n0,9465"':
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
            
            if co2_2019:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2019}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2020:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2020}, '{baseUnit}', 2020, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2021:
                EFId = code + f"{latestId:05}"

                replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2021}, '{baseUnit}', 2021, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

                try:
                    cursor.execute(replace_sql)
                    latestId+=1
                except Exception as ex:
                    logger.error(f"Failed Replace {ex}")
                    logger.error(replace_sql)

                del replace_sql
            
            if co2_2022:
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
        logger.debug(forCount)
    conn.close()











            # if key == ['2019.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 9
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)              

            #     co2Factor = rawData['2019.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2Factor}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql)
            #     latestId+=1

            #     del co2Factor, co2Unit, replace_sql
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            # elif key == ['Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power'] or key == []:
            #     continue

            # elif key == ['2010.0', '2011.0', '2012.0', '2013.0', '2014.0', '2015.0', '2016.0', '2017.0', '2018.0', '2019.0', '2020.0', '2021.0']: # 24 / 12
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2010 = rawData['2010.0']
            #     co2_2011 = rawData['2011.0']
            #     co2_2012 = rawData['2012.0']
            #     co2_2013 = rawData['2013.0']
            #     co2_2014 = rawData['2014.0']
            #     co2_2015 = rawData['2015.0']
            #     co2_2016 = rawData['2016.0']
            #     co2_2017 = rawData['2017.0']
            #     co2_2018 = rawData['2018.0']
            #     co2_2019 = rawData['2019.0']
            #     co2_2020 = rawData['2020.0']
            #     co2_2021 = rawData['2021.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2010 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2011 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2011}, '{baseUnit}', 2011, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2012 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2013 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2013}, '{baseUnit}', 2013, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2014 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2014}, '{baseUnit}', 2014, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2015 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2015}, '{baseUnit}', 2015, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2016 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2016}, '{baseUnit}', 2016, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2017 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2017}, '{baseUnit}', 2017, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2018 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2018}, '{baseUnit}', 2018, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2019 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2019}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2020 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2020}, '{baseUnit}', 2020, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2021 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2021}, '{baseUnit}', 2021, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2010)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2010)

            #     try:
            #         cursor.execute(replace_sql2011)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2011)

            #     try:
            #         cursor.execute(replace_sql2012)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2012)

            #     try:
            #         cursor.execute(replace_sql2013)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2013)

            #     try:
            #         cursor.execute(replace_sql2014)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2014)

            #     try:
            #         cursor.execute(replace_sql2015)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2015)

            #     try:
            #         cursor.execute(replace_sql2016)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2016)

            #     try:
            #         cursor.execute(replace_sql2017)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2017)

            #     try:
            #         cursor.execute(replace_sql2018)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2018)

            #     try:
            #         cursor.execute(replace_sql2019)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2019)
                
            #     try:
            #         cursor.execute(replace_sql2020)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2020)

            #     try:
            #         cursor.execute(replace_sql2021)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2021)


            #     latestId+=1

            #     del co2_2010, co2_2011, co2_2012, co2_2013, co2_2014, co2_2015, co2_2016, co2_2017, co2_2018, co2_2019, co2_2020, co2_2021, co2Unit, replace_sql2010, replace_sql2011, replace_sql2012, replace_sql2013, replace_sql2014, replace_sql2015, replace_sql2016, replace_sql2017, replace_sql2018, replace_sql2019, replace_sql2020, replace_sql2021
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            # elif key == ['2010.0', '2011.0', '2012.0', '2013.0', '2014.0', '2015.0', '2018.0', '2019.0', '2020.0', '2021.0'] or key == ['2010.0', '2011.0', '2012.0', '2013.0', '2014.0', '2015.0', '2018.0', '2019.0', '2020.0', '2021.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 10 / 10 + 10 / 10
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2010 = rawData['2010.0']
            #     co2_2011 = rawData['2011.0']
            #     co2_2012 = rawData['2012.0']
            #     co2_2013 = rawData['2013.0']
            #     co2_2014 = rawData['2014.0']
            #     co2_2015 = rawData['2015.0']
            #     co2_2018 = rawData['2018.0']
            #     co2_2019 = rawData['2019.0']
            #     co2_2020 = rawData['2020.0']
            #     co2_2021 = rawData['2021.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2010 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2011 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2011}, '{baseUnit}', 2011, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2012 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2013 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2013}, '{baseUnit}', 2013, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2014 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2014}, '{baseUnit}', 2014, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2015 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2015}, '{baseUnit}', 2015, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2018 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2018}, '{baseUnit}', 2018, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2019 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2019}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2020 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2020}, '{baseUnit}', 2020, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2021 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2021}, '{baseUnit}', 2021, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2010)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2010)

            #     try:
            #         cursor.execute(replace_sql2011)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2011)

            #     try:
            #         cursor.execute(replace_sql2012)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2012)

            #     try:
            #         cursor.execute(replace_sql2013)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2013)

            #     try:
            #         cursor.execute(replace_sql2014)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2014)

            #     try:
            #         cursor.execute(replace_sql2015)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2015)

            #     try:
            #         cursor.execute(replace_sql2018)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2018)

            #     try:
            #         cursor.execute(replace_sql2019)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2019)
                
            #     try:
            #         cursor.execute(replace_sql2020)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2020)

            #     try:
            #         cursor.execute(replace_sql2021)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2021)


            #     latestId+=1

            #     del co2_2010, co2_2011, co2_2012, co2_2013, co2_2014, co2_2015, co2_2018, co2_2019, co2_2020, co2_2021, co2Unit, replace_sql2010, replace_sql2011, replace_sql2012, replace_sql2013, replace_sql2014, replace_sql2015, replace_sql2018, replace_sql2019, replace_sql2020, replace_sql2021
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            # elif key == ['2012.0', '2016.0', '2019.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 3 / 3
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2012 = rawData['2012.0']
            #     co2_2016 = rawData['2016.0']
            #     co2_2019 = rawData['2019.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2012 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2016 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2016}, '{baseUnit}', 2016, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2019 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2019}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2012)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2012)

            #     try:
            #         cursor.execute(replace_sql2016)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2016)

            #     try:
            #         cursor.execute(replace_sql2019)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2019)
                

            #     latestId+=1

            #     del co2_2012, co2_2016, co2_2019, co2Unit, replace_sql2012, replace_sql2016, replace_sql2019
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            
            # elif key == ['2009.0', '2010.0', '2011.0', '2012.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 8 / 4
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2009 = rawData['2009.0']
            #     co2_2010 = rawData['2010.0']
            #     co2_2011 = rawData['2011.0']
            #     co2_2012 = rawData['2012.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2009 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2009}, '{baseUnit}', 2009, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2010 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2011 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2011}, '{baseUnit}', 2011, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2012 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2009)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2009)
                
            #     try:
            #         cursor.execute(replace_sql2010)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2010)

            #     try:
            #         cursor.execute(replace_sql2011)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2011)

            #     try:
            #         cursor.execute(replace_sql2012)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2012)

            #     latestId+=1

            #     del co2_2009, co2_2010, co2_2011, co2_2012, co2Unit, replace_sql2009, replace_sql2010, replace_sql2011, replace_sql2012
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            # elif key == ['2010.0', '2011.0', '2012.0']: # 3 / 3
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2010 = rawData['2010.0']
            #     co2_2011 = rawData['2011.0']
            #     co2_2012 = rawData['2012.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2010 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2010}, '{baseUnit}', 2010, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2011 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2011}, '{baseUnit}', 2011, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2012 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2012}, '{baseUnit}', 2012, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2010)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2010)

            #     try:
            #         cursor.execute(replace_sql2011)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2011)

            #     try:
            #         cursor.execute(replace_sql2012)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2012)

            #     latestId+=1

            #     del co2_2010, co2_2011, co2_2012, co2Unit, replace_sql2010, replace_sql2011, replace_sql2012
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)

            # elif key == ['2014.0', '2018.0'] or key == ['2014.0', '2018.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 4 / 2 + 2 / 2
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2014 = rawData['2014.0']
            #     co2_2018 = rawData['2018.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2014 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2014}, '{baseUnit}', 2014, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2018 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2018}, '{baseUnit}', 2018, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2014)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2014)

            #     try:
            #         cursor.execute(replace_sql2018)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2018)

            #     latestId+=1

            #     del co2_2014, co2_2018, co2Unit, replace_sql2014, replace_sql2018
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            # elif key == ['2013.0', '2019.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 54 / 2
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2013 = rawData['2013.0']
            #     co2_2019 = rawData['2019.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2013 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2013}, '{baseUnit}', 2013, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2019 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2019}, '{baseUnit}', 2019, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2013)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2013)

            #     try:
            #         cursor.execute(replace_sql2019)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2019)

            #     latestId+=1

            #     del co2_2013, co2_2019, co2Unit, replace_sql2013, replace_sql2019
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)
            # elif key == ['2013.0', '2015.0', 'Wind andsolar power', 'First crediting period', 'Third crediting period', 'Second crediting period', 'Other than wind and solar power']: # 42 / 2
            #     latestId = util.getLatestId(sourceId, write_schema, sourceTable, cursor)

            #     co2_2013 = rawData['2013.0']
            #     co2_2015 = rawData['2015.0']

            #     co2Unit = 'kgCO₂'

            #     EFId = code + f"{latestId:05}"

            #     replace_sql2013 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2013}, '{baseUnit}', 2013, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     latestId += 1
            #     EFId = code + f"{latestId:05}"
                
            #     replace_sql2015 = f"REPLACE INTO {write_schema}.{write_table} (EFId, name, CO2, unit, year, CO2Unit, sourceId, everTable, everId) VALUES ('{EFId}', '{sourceOfEmission}', {co2_2015}, '{baseUnit}', 2015, '{co2Unit}', {sourceId}, '{ever_table}', '{everId}')"

            #     try:
            #         cursor.execute(replace_sql2013)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2013)

            #     try:
            #         cursor.execute(replace_sql2015)
            #     except Exception as ex:
            #         logger.error(f"Failed Replace {ex}")
            #         logger.error(replace_sql2015)

            #     latestId+=1

            #     del co2_2013, co2_2015, co2Unit, replace_sql2013, replace_sql2015
            #     util.updateLatestId(code, latestId, write_schema, sourceTable, cursor)