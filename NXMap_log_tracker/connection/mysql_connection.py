import pymysql
import logging

logger = logging.getLogger(__name__)

class MySQLConn:
    def __init__(self, db_env: dict, autocommit: bool=True, dict_mode: bool=False) -> None:
        self.host=db_env['host']
        self.port=int(db_env['port'])
        self.user=db_env['user']
        self.password=db_env['password']
        self.dict_mode = dict_mode
        self.autocommit = autocommit
        self.connection = None
        self.connect()
        
    def connect(self,):
        if self.connection is not None:
            self.connection.close()

        if self.dict_mode:
            self.connection =  pymysql.connect(
                host=self.host,
                port=int(self.port),
                user=self.user,
                password=self.password,
                autocommit=self.autocommit,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        else:
            self.connection = pymysql.connect(
                host=self.host,
                port=int(self.port),
                user=self.user,
                password=self.password,
                autocommit=self.autocommit,
                charset='utf8mb4',
            )
        logger.info("=================================================")
        logger.info("our db_config:")
        logger.info(f"host: {self.host}")
        logger.info(f"port: {self.port}")
        logger.info(f"user: {self.user}")
        logger.info(f"password: {self.password}")
        logger.info(f"autocommit: {self.autocommit}")
        logger.info(f"dictionary mode: {self.dict_mode}")
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