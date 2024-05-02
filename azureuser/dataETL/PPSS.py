import sys
import os
rootPath = os.getcwd()+'/../'
sys.path.append(rootPath)
import time
import datetime
import json
import logging

import configparser
from utils import myLog, util
from connection.mysql_connection import MySQLConn
import pymysql

def execute_sql(value_string: str, table: str, cursor: pymysql.cursors.Cursor):
    value_string = value_string[:-2]
    
    if table == 'power':
        replaceCommand = f"REPLACE INTO dataETL.power (ts, gatewayId, name, ch1Watt) VALUES {value_string}"
    elif table == 'flow':
        replaceCommand = f"REPLACE INTO dataETL.flow (ts, gatewayId, name, flowRate) VALUES {value_string}"
    elif table == 'temp':
        replaceCommand = f"REPLACE INTO dataETL.temp (ts, gatewayId, name, temp1, temp2, temp3, temp4) VALUES {value_string}"

    logger.debug(replaceCommand)

    try:
        cursor.execute(replaceCommand)
        logger.info(f"REPLACE {table} Succeed!")
    except Exception as ex:
        logger.error(f"SQL {table}: {replaceCommand}")
        logger.error(f"[REPLACE ERROR]: {str(ex)}({table})")


if __name__ == "__main__":
    startRunTime = time.time()

    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"], level=logging.ERROR)
    conn = MySQLConn(config['mysql_azureV2'])

    nowTime = datetime.datetime.now().replace(second=0, microsecond=0)
    logger.info(f"---------- now: {nowTime} ----------")

    st = (nowTime - datetime.timedelta(hours=1)).replace(microsecond=0)
    et = nowTime

    logger.info(f"----- Searching from {st} to {et} -----")

    value_string_power = ''
    value_string_flow = ''
    value_string_temp = ''
    with conn.cursor() as cursor:
        sqlCommand = """
            SELECT 
                ETL.name, ETL.gatewayId, ETL.deviceId, Logic.dataTableETL   # Power#X, 123, ieee, writeTable
            FROM mgmtETL.DataETL ETL
            JOIN mgmtETL.DeviceLogic Logic
            ON ETL.deviceType=Logic.deviceType AND ETL.deviceLogic=Logic.logicId AND siteId IN (7, 8)
        """
        cursor.execute(sqlCommand)

        for (name, gId, dId, table) in cursor.fetchall():
            logger.debug(f"Processing GatewyId:{gId} {dId} {name}")

            sqlCommand = f"""
                SELECT
                    GWts as ts, rawData
                FROM rawData.PPSS
                WHERE gatewayId={gId} AND name='{dId}' AND GWts>='{st}' AND GWts<'{et}'
            """
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                result = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                result = ()

            if result: # 無資料的話就不執行
                for (ts, data) in result:
                    data = json.loads(data)
                    if table == 'power':
                        value_string_power += f" ('{ts}', {gId}, '{name}', {data.get('ch1Watt')}), "
                    elif table == 'flow':
                        value_string_flow += f" ('{ts}', {gId}, '{name}', {data.get('flowRate')}), "
                    elif table == 'temp':
                        value_string_temp += f" ('{ts}', {gId}, '{name}', {data.get('temp1')}, {data.get('temp2')}, {data.get('temp3')}, {data.get('temp4')}), "
                    else:
                        logger.error(f"table {table} is not valid!")
                        continue

    with conn.cursor() as cursor:
        if value_string_power != '':
            execute_sql(value_string_power.replace("None", "NULL"), 'power', cursor)

        if value_string_flow != '':
            execute_sql(value_string_flow.replace("None", "NULL"), 'flow', cursor)
        
        if value_string_temp != '':
            execute_sql(value_string_temp.replace("None", "NULL"), 'temp', cursor)

    conn.close()
    endRunTime = time.time()

    logger.info(f"Execute time: {util.convert_sec(endRunTime-startRunTime)}")
