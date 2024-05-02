from connection.mysql_connection import MySQLConn
from utils import myLog
import os

import configparser

if __name__ == '__main__':

    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    config = configparser.ConfigParser()
    config.read('config.ini')

    logger = myLog.get_logger(logPath, filename, config['mysql_azureV2'])

    conn = MySQLConn(config['mysql_azureV2'])

    with conn.cursor() as cursor:
        cursor.execute("SHOW PROCESSLIST;")
        for res in cursor.fetchall():
            if 'Sleep' in res and res[5] > 1000:
                logger.debug(res)
                try:
                    cursor.execute(f"KILL {res[0]};")
                except Exception as ex:
                    logger.error(f"Error executing KILL {res[0]};")
                    logger.error(f"Error message: {ex}")
    
    conn.close()