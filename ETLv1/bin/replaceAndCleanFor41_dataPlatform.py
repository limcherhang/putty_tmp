import pymysql
import logging
import os
import datetime
import time

from logging.handlers import TimedRotatingFileHandler

class MySQLConn:
    def __init__(self, db_env: dict, autocommit: bool=True) -> None:
        self.host=db_env['host']
        self.port=int(db_env['port'])
        # self.user=db_env['user']
        # self.password=db_env['password']
        self.autocommit = autocommit
        self.connect()
        
    def connect(self,):
        self.connection = pymysql.connect(
            host=self.host,
            port=int(self.port),
            # user=self.user,
            # password=self.password,
            autocommit=self.autocommit,
            charset='utf8mb4',
            read_default_file = '~/.my.cnf'
        )
        logger.info("=================================================")
        logger.info("our db_config:")
        logger.info(f"host: {self.host}")
        logger.info(f"port: {self.port}")
        # logger.info(f"user: {self.user}")
        # logger.info(f"password: {self.password}")
        logger.info(f"autocommit: {self.autocommit}")
        # logger.info(f"dictionary mode: {self.dict_mode}")
        logger.info("=================================================")

    def cursor(self,):       
        try:
            return self.connection.cursor()
        except pymysql.err.OperationalError as e:
            logger.error(f"Lost connection to MySQL server: {e}")
            self.connect()
            return self.connection.cursor()
        
    def close(self,):
        return self.connection.close()

def get_logger(logPath: str, filename: str, level: str=logging.DEBUG):
    
    check_if_folder_exists(logPath)
    logfile = f"{logPath}/{filename}"

    logging.basicConfig(handlers=[TimedRotatingFileHandler(logfile , when='midnight')], level=level, format='%(asctime)s.%(msecs)03d %(name)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    logger = logging.getLogger(__name__)
    return logger

def check_if_folder_exists(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass

def process_delete(schema: str, table: str, start: datetime.datetime, end: datetime.datetime, cursor: pymysql.cursors.Cursor):
    while True:
        delete_sql = f"""
            DELETE FROM {schema}.{module} WHERE ts >= '{start}' AND ts < '{end}' LIMIT 10000;
        """
        try:
            logger.debug(delete_sql)
            cursor.execute(delete_sql)
            logger.info(f"DELETE {table} from {start} to {end} Succeed!")
            
        except Exception as ex:
            logger.error(f"Error executing {delete_sql}")
            logger.error(f"Error message: {ex}")
        if cursor.rowcount == 0:
            break
        
        time.sleep(1)

def convert_sec(times):
    if times < 60:
        result = f"{round(times)} sec"
    elif times < 3600:      # times < 3600 sec = 60 sec * 60 min = 1 hour
        m = times // 60
        s = round(times - m*60)
        result = f"{m} minutes {s} sec"
    elif times < 86400:     # times < 86400 sec = 3600 sec * 24 hour = 1 day
        h = times // 3600
        s = times - h*3600
        m = s // 60
        s = round(s - m*60)
        result = f"{h} hour {m} minutes {s} sec"
    else:                   # times >= 1 day
        d = times // 86400
        s = times - d*86400
        h = s // 3600
        s = s - h*3600
        m = s // 60
        s = round(s - m*60)
        result = f"{d} day {h} hour {m} minutes {s} sec"
    return result

db_config = {
    'host': "127.0.0.1",
    'port': 3306,
}

if __name__ == '__main__':
    startRunTime = time.time()
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()+ '/log'

    logger = get_logger(logPath, filename+'.log')

    conn = MySQLConn(db_config)

    nowTime = datetime.datetime.now().date()

    process_date = nowTime - datetime.timedelta(days=1) # 昨天的日期，昨天以前的都要replace和清掉
    process_schema = 'dataPlatform'

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
                        logger.error(f"Since {replace_sql} is unsuccessful, detele is not executed")
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
                            process_delete(process_schema, module, date, next_time, cursor)
                        else:
                            datas = []
                            _count = 0
                            for data in exist_data:
                                ts = data[0]
                                siteId = data[1]
                                name = data[2]
                                sqlCommand = f"SELECT * FROM {process_schema}.{module} WHERE ts='{ts}' AND siteId={siteId} AND name='{name}';"
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
                                process_delete(process_schema, module, date, next_time, cursor)
                            else:
                                logger.error(f"It still have some data doesn't move to rawData{write_year}")
                                logger.error(exist_data)
                    del errorCode    
                    date = next_time
    conn.close()
    endRuntime = time.time()
    logger.info(f"RUN TIME: {convert_sec(endRuntime-startRunTime)}")