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
import pandas as pd

if __name__ == '__main__':
    startTime = time.time()
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    file = __file__
    basename = os.path.basename(file)
    logFile = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    logger = myLog.get_logger(logPath, f"{logFile}.log", config["mysql_azureV2"])

    client = MongoConn(config["mongo_dev_v1_nxmap"])
    client.connect()
    db = client.get_database()

    conn = MySQLConn(config["mysql_azureV2"])

    with conn.cursor() as cursor:
        sqlCommand = "SELECT Entity, max(`Year`) FROM ever.GEF_data GROUP BY Entity;"
        cursor.execute(sqlCommand)

        insert_content = {
            "type": "Location Based",
            "methods": [],
            "scope": "scope2",
            "category": "Purchased Electricity"
        }

        for (entity, year) in cursor.fetchall():
            sqlCommand = f"""SELECT * FROM ever.GEF_data WHERE Entity = "{entity}" AND Year = {year};"""
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            ID, Entity, Code, Year, Carbon = cursor.fetchone()
            sourceOfEmission = Entity
            co2Factor = Carbon/1000
            ch4Factor = None
            n2oFactor = None
            baseUnit = "kWh"
            source = "GEF"
            sheetName = "carbon-intensity-electricity"
            urlName = "GEF"
            file = "./upload/ef/carbon-intensity-electricity.csv"
            link = "https://ourworldindata.org/grapher/carbon-intensity-electricity"
            emissionUnit = "kgCO₂"
            ch4Unit = "kgCH₄"
            n2oUnit = "kgN₂O"
            insert_content["methods"].append({
                "sourceOfEmission": sourceOfEmission,
                "co2Factor": co2Factor,
                "ch4Factor": ch4Factor,
                "n2oFactor": n2oFactor,
                "baseUnit": baseUnit,
                "source": source,
                "urlName": urlName,
                "sheetName": sheetName,
                "file": file,
                "link": link,
                "year": year,
                "emissionUnit": emissionUnit,
                "ch4Unit": ch4Unit,
                "n2oUnit": n2oUnit
            })
        insert_result = db.cal_approaches.insert_one(insert_content)
    print(insert_result.inserted_id)
    conn.close()
    client.close()
    endTime = time.time()                
    logger.info(f"Total time: {endTime - startTime:.2f} seconds")