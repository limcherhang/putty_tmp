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

    # Create mongo connection
    client = MongoConn(config['mongo_production_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    cursorMongo = db.cal_approaches.find({"methods.source": "IPCC"})

    SOEs = {}

    for cur in cursorMongo:
        # methods = cur['methods']

        # for method in methods:
        #     if methods['source'] != 'IPCC':
        #         continue
        #     sourceOfEmission = method['sourceOfEmission']
        #     if sourceOfEmission not in SOEs:
        #         SOEs[sourceOfEmission] = {}
        logger.debug(cur['category'])

    # with conn.cursor() as cursor:
    #     source = "IPCC"
        
    #     ever_schema = 'ever'
    #     ever_table = 'IPCC_data'

    #     write_schema = 'mgmtCarbon'
    #     write_table = 'EF2'
    #     sourceTable = 'EFsource2'

    #     sourceId, code = util.getSourceId(source, write_schema, sourceTable, cursor)
    #     util.resetLatestId(code, write_schema, sourceTable, cursor)

    #     sqlCommand = f"""
    #         SELECT * FROM {ever_schema}.{ever_table} WHERE Fuel2006 is not NULL;
    #     """

    #     process_ghg = ['CARBON DIOXIDE', 'METHANE', 'NITROUS OXIDE']

    #     cursor.execute(sqlCommand)
    #     units = []
        
        # for EFID, IPCC1996SourceSinkCategory, IPCC2006SourceSinkCategory, Gas, Fuel1996, Fuel2006, Cpool, Typeofparameter, Description, TechnologiesPractices, ParametersConditions, RegionRegionalConditions, AbatementControlTechnologies, Otherproperties, Value, Unit, Equation, IPCCWorksheet, TechnicalReference, Sourceofdata, Dataprovider in cursor.fetchall():
            # if Gas not in process_ghg or (Fuel2006 is None and Fuel2006 is None ):
            #     continue


            # everID = EFID
            # if Fuel2006:
            #     sourceOfEmission = Fuel2006
            #     if Otherproperties:
            #         sourceOfEmission += f' - {Otherproperties}'

            # else:
            #     logger.error(f"{EFID}")
            
            # tmp_typeOfEmission = IPCC2006SourceSinkCategory.split('-') if IPCC2006SourceSinkCategory is not None else IPCC1996SourceSinkCategory.split('-')
            # typeOfEmission_of_type = tmp_typeOfEmission[1].replace(' ', '')
            # co2Factor = Value
            
            # if Unit not in units:
            #     # logger.debug(f"{EFID}, {IPCC1996SourceSinkCategory}, {IPCC2006SourceSinkCategory}, {Gas}, {Fuel1996}, {Fuel2006}, {Cpool}, {Typeofparameter}, {Description}, {TechnologiesPractices}, {ParametersConditions}, {RegionRegionalConditions}, {AbatementControlTechnologies}, {Otherproperties}, {Value}, {Unit}, {Equation}, {IPCCWorksheet}, {TechnicalReference}, {Sourceofdata}, {Dataprovider}")
            #     logger.debug(f"{sourceOfEmission}, {Unit}")
            #     units.append(Unit)