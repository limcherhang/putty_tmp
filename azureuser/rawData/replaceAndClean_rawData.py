import os
import sys
rootPath = os.getcwd()+'/../'
sys.path.append(rootPath)

import configparser
import datetime
import pymysql
import time
import pandas as pd
import logging

from connection.mysql_connection import MySQLConn
from utils import myLog, util   

if __name__ == "__main__":
    startRunTime = time.time()
    
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    mail = config['mail_server_aaron']
    mail_username = mail['mail_username']
    mail_password = mail['mail_password']
    from_email = 'Alerts Replace rawData'
    to_email = 'alin@evercomm.com.sg'

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"], level=logging.ERROR)

    conn = MySQLConn(config['mysql_azureV2'])

    nowTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"---------- now: {nowTime} ----------")

    process_date = nowTime - datetime.timedelta(days=1)

    process_schema = 'rawData'

    with conn.cursor() as cursor:
        if process_date.month == 1 and process_date.day == 1:
            sqlCommand = f"""
                SELECT SUBSTRING_INDEX(table_name, '_', 1) as tbl_name FROM information_schema.tables WHERE table_schema = '{process_schema}{process_date.year-1}' group by tbl_name
            """
        else:
            sqlCommand = f"""
                SELECT SUBSTRING_INDEX(table_name, '_', 1) as tbl_name FROM information_schema.tables WHERE table_schema = '{process_schema}{process_date.year}' group by tbl_name
            """
        try:
            cursor.execute(sqlCommand)
            modules = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")

        if modules == ():
            if process_date.month == 1 and process_date.day == 1:
                logger.error(f"No module in {process_schema}{process_date.year-1}")
            else:
                logger.error(f"No module in {process_schema}{process_date.year}")

        for (module, ) in modules:
            message_alert_no_replace = f"""
                <html>
                    <body>
                        <p>{module} </p>
            """
            alert = 0
            logger.info(f"-------------------- Processing {module} --------------------")

            if "API" in module:
                sqlCommand = f"""
                    SELECT DISTINCT(DATE(APIts)) FROM {process_schema}.{module} WHERE APIts < '{process_date}'
                """
            elif module == 'milesight':
                sqlCommand = f"""
                    SELECT DISTINCT(DATE(DBts)) FROM {process_schema}.{module} WHERE DBts < '{process_date}'
                """
            else:
                sqlCommand = f"""
                    SELECT DISTINCT(DATE(GWts)) FROM {process_schema}.{module} WHERE GWts < '{process_date}'
                """
            
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")

            for (date, ) in cursor.fetchall():
                write_month = f"{date.month:02}"
                write_year = date.year
                date = datetime.datetime(date.year, date.month, date.day, 0, 0, 0)
                next_date = date + datetime.timedelta(days=1)

                while date.timestamp() < next_date.timestamp():
                    next_time = date + datetime.timedelta(hours=1)
                    # if process_date.timestamp() - date.timestamp() < 60 * 60 * 24 * 2:
                    if "API" in module:
                        replace_sql = f"""
                            REPLACE INTO {process_schema}{write_year}.{module}_{write_month}
                            (
                                SELECT * FROM {process_schema}.{module} WHERE APIts >= '{date}' AND APIts < '{next_time}'
                                EXCEPT
                                SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE APIts >= '{date}' AND APIts < '{next_time}'
                            )
                        """
                    elif module == "milesight":
                        replace_sql = f"""
                            REPLACE INTO {process_schema}{write_year}.{module}_{write_month}
                            (
                                SELECT * FROM {process_schema}.{module} WHERE DBts >= '{date}' AND DBts < '{next_time}'
                                EXCEPT
                                SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE DBts >= '{date}' AND DBts < '{next_time}'
                            )
                        """
                    else:
                        replace_sql = f"""
                            REPLACE INTO {process_schema}{write_year}.{module}_{write_month}
                            (
                                SELECT * FROM {process_schema}.{module} WHERE GWts >= '{date}' AND GWts < '{next_time}'
                                EXCEPT
                                SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE GWts >= '{date}' AND GWts < '{next_time}'
                            )
                        """
                    try:
                        logger.debug(replace_sql)
                        cursor.execute(replace_sql)
                        logger.info("REPLACE Succeed!")
                        errorCode = 0
                    except Exception as ex:
                        logger.error(f"Error executing {replace_sql}")
                        logger.error(f"Error message: {ex}")
                        errorCode = 1
                    if errorCode == 1:
                        logger.error(f"Since {replace_sql} is unsuccessful, delete is not executed")
                        # subject = f"ALERTS SYSTEM"
                        message_alert_no_replace += f"""
                            <p>{replace_sql} </p>
                            <p>The above sql wasn't executed successfully, please check it.</p>

                            <p>-----------------------------------------------------------------------------------------------</p>
                        """
                        alert+=1
                        # util.send_mail(mail_username, mail_password, from_email, to_email, subject, message)
                    else:
                        if "API" in module:
                            sqlCommand = f"""
                                SELECT * FROM {process_schema}.{module} WHERE APIts >= '{date}' AND APIts < '{next_time}'
                                EXCEPT
                                SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE APIts >= '{date}' AND APIts < '{next_time}'
                            """
                        elif module == 'zigbee':
                            sqlCommand = f"""
                                SELECT DBts, GWts, ZBts, ieee, clusterID FROM {process_schema}.{module} WHERE GWts >= '{date}' AND GWts < '{next_time}' GROUP BY DBts, GWts, ZBts, ieee, clusterID
                                EXCEPT
                                SELECT DBts, GWts, ZBts, ieee, clusterID FROM {process_schema}{write_year}.{module}_{write_month} WHERE GWts >= '{date}' AND GWts < '{next_time}'
                            """
                        elif module == 'milesight':
                            sqlCommand = f"""
                                SELECT * FROM {process_schema}.{module} WHERE DBts >= '{date}' AND DBts < '{next_time}'
                                EXCEPT
                                SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE DBts >= '{date}' AND DBts < '{next_time}'
                            """
                        else:
                            sqlCommand = f"""
                                SELECT * FROM {process_schema}.{module} WHERE GWts >= '{date}' AND GWts < '{next_time}'
                                EXCEPT
                                SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE GWts >= '{date}' AND GWts < '{next_time}'
                            """
                        try:
                            logger.debug(sqlCommand)
                            cursor.execute(sqlCommand)
                            exist_data = cursor.fetchall()
                        except Exception as ex:
                            logger.error(f"Error executing {sqlCommand}")
                            logger.error(f"Error message: {ex}")
                            continue

                        if exist_data == ():
                            util.process_delete(process_schema, module, date, next_time, cursor)
                        else:
                            datas = []
                            _count = 0
                            for data in exist_data:
                                if module in ('bacnet', 'contecM2M', 'PPSS'):
                                    GWts = data[1]
                                    gId = data[2]
                                    name = data[3]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE Gwts='{GWts}' AND gatewayId={gId} AND name='{name}';"
                                elif module == 'kiwiAPI':
                                    APIts = data[1]
                                    gId = data[2]
                                    uId = data[3]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE APIts='{APIts}' AND gatewayId={gId} AND uId='{uId}';"
                                elif module == 'milesight':
                                    DBts = data[0]
                                    gId = data[1]
                                    devEui = data[2]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE DBts='{DBts}' AND gatewayId={gId} AND devEui='{devEui}';"
                                elif module == 'modbus':
                                    GWts = data[1]
                                    gId = data[2]
                                    cmd = data[3]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE GWts='{GWts}' AND gatewayId={gId} AND cmd='{cmd}';"
                                elif module == 'sigfoxAPI':
                                    APIts = data[1]
                                    gId = data[2]
                                    dId = data[3]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE APIts='{APIts}' AND gatewayId={gId} AND deviceId='{dId}';"
                                elif module == 'sindconAPI':
                                    APIts = data[1]
                                    gId = data[2]
                                    SN = data[3]
                                    devEui = data[4]
                                    title = data[5]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE APIts='{APIts}' AND gatewayId={gId} AND SN='{SN}' AND devEui='{devEui}' AND title='{title}';"
                                elif module == 'tealeAPI':
                                    APIts = data[1]
                                    gId = data[2]
                                    mId = data[3]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE APIts='{APIts}' AND gatewayId={gId} AND meterId='{mId};"
                                elif module == 'v3API':
                                    APIts = data[1]
                                    gId = data[2]
                                    label = data[3]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE APIts='{APIts}' AND gatewayId={gId} AND label='{label}'"
                                elif module == 'zigbee':
                                    GWts = data[1]
                                    ZBts = data[2]
                                    gId = data[3]
                                    ieee = data[5]
                                    cId = data[6]
                                    sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE Gwts='{GWts}' AND ZBts='{ZBts}' AND gatewayId={gId} AND ieee='{ieee}' AND clusterId='{cId}';"
                                else:
                                    subject = f"ALERTS SYSTEM"
                                    message = f"""
                                        Lost module {module}
                                    """
                                    util.send_mail(mail_username, mail_password, from_email, to_email, subject, message)
                                    sqlCommand = ''
                                
                                try:
                                    logger.info(f"Checking {module} in {date} to {next_time}")
                                    logger.debug(sqlCommand)                             
                                    cursor.execute(sqlCommand)
                                    data = cursor.fetchall()
                                    logger.debug(data)
                                except Exception as ex:
                                    logger.error(f"Error executing {sqlCommand}")
                                    logger.error(f"Error message: {ex}")
                                    data = ()
                                
                                if len(data) == 1:
                                    datas.append(data)
                                    _count += 1
                            if _count == 0:
                                util.process_delete(process_schema, module, date, next_time, cursor)
                            else:
                                logger.error(f"It still have some data doesn't move to {process_schema}{write_year}")
                                logger.error(exist_data)
                                message = ""
                                for i, d in enumerate(datas):
                                    message += f"""
                                        {i+1}. {d}
                                    """
                                subject = f"ALERTS SYSTEM"
                                message += f"""
                                    ------------------------------------------------------------------------
                                    {exist_data} 
                                    The above data haven't move to {process_schema}
                                """
                                util.send_mail(mail_username, mail_password, from_email, to_email, subject, message)
                    del errorCode
                    date = next_time
            message_alert_no_replace += """
                    </body>
                </html>
            """
            subject = f"ALERTS SYSTEM"
            if alert > 0:
                util.send_mail(mail_username, mail_password, from_email, to_email, subject, message_alert_no_replace, html=True)

    conn.close()
    endRunTime = time.time()
    logger.info(f"EXECUTE TIME: {util.convert_sec(endRunTime-startRunTime)}")