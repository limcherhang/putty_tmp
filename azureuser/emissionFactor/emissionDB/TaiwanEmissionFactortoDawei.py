import os
import sys
rootPath = os.getcwd() + '/../../'
sys.path.append(rootPath)
import configparser
from connection.mongo_connection import MongoConn
from connection.mysql_connection import MySQLConn
import pymysql
from utils import myLog
from bson import ObjectId
import time
import pandas as pd

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]+'.log'
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}", config["mysql_azureV2"])


    # Create mongo connection
    client = MongoConn(config['mongo_production_nxmap'])
    client.connect()

    # Get database
    db = client.get_database()

    conn = MySQLConn(config['mysql_azureV2'])

    cal_approaches = db.cal_approaches.find({"usedRegion" : "Taiwan"})

    # columns = [_id, type, scope, method, sourceOfEmission, co2Factor, ch4Factor, n2oFactor, baseUnit, source, urlName, sheetName, file, link, year, emissionUnit, ch4Unit, n2oUnit, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion]
    result = []
    for cal_approach in cal_approaches:
        _id = cal_approach['_id']
        _type = cal_approach['type']
        scope = cal_approach['scope']
        methods = cal_approach['methods']
        typeOfEmission = cal_approach['typeOfEmission']
        category = cal_approach['category']
        categoryKey = cal_approach['categoryKey']
        nameKey = cal_approach['nameKey']
        sourceType = cal_approach['sourceType']
        usedRegion = cal_approach['usedRegion']

        for method in methods:
            sourceOfEmission = method['sourceOfEmission']
            co2Factor = method['co2Factor'] 
            ch4Factor = method['ch4Factor'] 
            n2oFactor = method['n2oFactor'] 
            baseUnit = method['baseUnit'] 
            source = method['source'] 
            urlName = method['urlName'] 
            sheetName = method['sheetName'] 
            _file = method['file'] 
            link = method['link'] 
            year = method['year']
            emissionUnit = method['emissionUnit'] 
            ch4Unit = method['ch4Unit'] 
            n2oUnit = method['n2oUnit']

            result.append([_type, scope, sourceOfEmission, co2Factor, ch4Factor, n2oFactor, baseUnit, source, urlName, sheetName, _file, link, year, emissionUnit, ch4Unit, n2oUnit, typeOfEmission, category, categoryKey, nameKey, sourceType, usedRegion])

    
    df = pd.DataFrame(result, columns=["type", "scope", "sourceOfEmission", "co2Factor", "ch4Factor", "n2oFactor", "baseUnit", "source", "urlName", "sheetName", "file", "link", "year", "emissionUnit", "ch4Unit", "n2oUnit", "typeOfEmission", "category", "categoryKey", "nameKey", "sourceType", "usedRegion"])
    
    with pd.ExcelWriter('Emission Factor EPA Taiwan.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name="EPA Taiwan")

        worksheet = writer.sheets['EPA Taiwan']

        worksheet.column_dimensions['A'].width = 15
        worksheet.column_dimensions['B'].width = 10
        worksheet.column_dimensions['C'].width = 15
        worksheet.column_dimensions['D'].width = 10
        worksheet.column_dimensions['E'].width = 10
        worksheet.column_dimensions['F'].width = 10
        worksheet.column_dimensions['G'].width = 15
        worksheet.column_dimensions['H'].width = 15
        worksheet.column_dimensions['I'].width = 15
        worksheet.column_dimensions['J'].width = 15
        worksheet.column_dimensions['K'].width = 15
        worksheet.column_dimensions['L'].width = 15
        worksheet.column_dimensions['M'].width = 15
        worksheet.column_dimensions['N'].width = 15
        worksheet.column_dimensions['O'].width = 15
        worksheet.column_dimensions['P'].width = 15

    conn.close()
    client.close()