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
    from_email = 'Alerts Replace dataETL'
    to_email = 'alin@evercomm.com.sg'

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"], level=logging.ERROR)

    conn = MySQLConn(config['mysql_azureV2'])

    nowTime = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"---------- now: {nowTime} ----------")

    process_date = nowTime - datetime.timedelta(days=1)

    process_schema = 'dataETL'

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

            sqlCommand = f"""
                SELECT DISTINCT DATE(ts) FROM {process_schema}.{module} WHERE ts < '{process_date}'
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
                    replace_sql = f"""
                        REPLACE INTO {process_schema}{write_year}.{module}_{write_month}
                        (
                            SELECT * FROM {process_schema}.{module} WHERE ts >= '{date}' AND ts < '{next_time}'
                            EXCEPT
                            SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE ts >= '{date}' AND ts < '{next_time}'
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
                        sqlCommand = f"""
                            SELECT * FROM {process_schema}.{module} WHERE ts >= '{date}' AND ts < '{next_time}'
                            EXCEPT
                            SELECT * FROM {process_schema}{write_year}.{module}_{write_month} WHERE ts >= '{date}' AND ts < '{next_time}'
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
                                ts = data[0]
                                gId = data[1]
                                name = data[2]
                                sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE ts='{ts}' AND gatewayId={gId} AND name='{name}';"

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