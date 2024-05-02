import pymongo
import logging
import configparser

logger = logging.getLogger(__name__)

class MongoConn:
    def __init__(self, db_cfg: dict):
        self.host = db_cfg['host']
        self.port = int(db_cfg['port'])
        self.username = db_cfg['username']
        self.password = db_cfg['password']
        self.db_name = db_cfg['db']
        self.connection = None

    def connect(self):
        connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        try:
            self.connection = pymongo.MongoClient(connection_string)
            logger.info("Connected to MongoDB")
        except pymongo.errors.ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")

        return self.connection

    def get_database(self):
        if self.connection is None:
            logger.error("Not connected to MongoDB")
            return None
        return self.connection[self.db_name]

    def close(self):
        if self.connection is not None:
            self.connection.close()
            logger.info("Closed MongoDB connection")