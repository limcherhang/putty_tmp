from datetime import datetime, timedelta
import time
import os
from pathlib import Path
import sys , os
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser

def main(conn,start,end):
    with conn.cursor() as cursor:
        sqlCommand="select siteId, name from mgmtETL.DataPlatform where name like 'ammonia#%' and gatewayId>0"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            NameList = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            NameList = ()
        logger.info(f"--------------- ammonia --------------- Took: {round(time.time()-s, 3)}s")
        for rows in NameList:
            sId=rows[0]
            name=rows[1]
            logger.info(f"--------------- select sId:{sId} name:{name}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand=f"SELECT max(NH3),min(NH3),count(NH3) FROM dataPlatform.ammonia where siteId={sId} and name='{name}' and ts>='{start}' and ts<'{end}'"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                result = cursor.fetchone()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                result = (0, 0, 0)

            if result == (0, 0, 0) or result[2] == 0:
                logger.warning(f"has no data in '{start}' to '{end}'")
                continue
            else:
                NH3Max=result[0]
                NH3Min=result[1]
                NH3count=result[2]
                num = round(NH3count/2)
    
                sqlCommand=f"""select * from ( select * from dataPlatform.ammonia where siteId={sId} and name='{name}' and ts>='{start}' and ts<'{end}' 
                            order by NH3 desc limit {num}) as newtable 
                            order by NH3 
                            limit 1 
                            """
                try:
                    cursor.execute(sqlCommand)
                    data=cursor.fetchone()
                    date=data[0].strftime('%Y-%m-%d')
                    NH3Median=data[3]
                except Exception as ex:
                    logger.error(f"Error executing {sqlCommand}")
                    logger.error(f"Error message: {ex}")
                    date= start
                    NH3Median = 0

                replace_sql="""
                replace into `reportPlatform{}`.`Dammonia`(
                `date`,`siteId`,`name`,`NH3Min`,`NH3Median`,`NH3Max`
                ) Values(\'{}\',{},\'{}\',{},{},{})
                """.format(start.year,date,sId,name,NH3Min,NH3Median,NH3Max)
                try:
                    logger.debug(replace_sql)
                    cursor.execute(replace_sql)
                    logger.info(f"--------------- replace to {conn.host} success --------------- Took: {round(time.time()-s, 3)}s")
                except Exception as ex:
                    logger.error(f"Error executing {replace_sql}")
                    logger.error(f"Error message: {ex}")

if __name__ == '__main__':
    s = time.time()
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')
    conn = MySQLConn(config['mysql_azureV2'])
    logger = myLog.get_logger(os.getcwd(), f"{filename}.log",config['mysql_azureV2'])
    nowTime = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
    
    logger.info(f"---------- Program Starts! : {nowTime} ---------- ")
    start = nowTime-timedelta(days=1)
    end = nowTime
    main(conn,start,end)
    conn.close()
    logger.info(f"{conn.host} close")   
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")