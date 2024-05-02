import sys
import os
rootPath = os.getcwd()
sys.path.append(rootPath)
import time
import datetime
import json
import logging

import configparser
from utils import myLog, util
from connection.mysql_connection import MySQLConn
import pymysql

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
    from_email = 'Alerts SYSTEM'
    to_email = ['alin@evercomm.com.sg', 'ehuang@evercomm.com.sg']
    Subject = "ALERTS LOG ERROR"

    conn = MySQLConn(config['mysql_azureV2'])

    nowTime = datetime.datetime.now().replace(second=0, microsecond=0)

    st = (nowTime - datetime.timedelta(hours=1)).replace(microsecond=0)
    et = nowTime


    send = 0
    with conn.cursor() as cursor:
        html_content = """
            <html>
                <body>
        """

        sqlCommand = f"SELECT * FROM logETL.logEntries WHERE ts >= '{st}' AND ts < '{et}';"
        cursor.execute(sqlCommand)
        for (ts, fileName, path, level, mes) in cursor.fetchall():
            html_content += f"""
                        <p>The <strong>{level}</strong> of <strong>{path}/{fileName}</strong> log occur in <strong>{ts}</strong>: </p>
                        <p></p>
                        <p>{mes}</p>
                <p>---------------------------------------------------------------------------------------------------------</p>
            """
            send += 1
        
        html_content += """
                </body>
            </html>
        """

        if send > 0:
            for email in to_email:
                util.send_mail(mail_username, mail_password, from_email, email, Subject, html_content, html=True)