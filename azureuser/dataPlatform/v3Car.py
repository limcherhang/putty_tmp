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
import logging

def getlast(conn,name,st):
    stime = datetime.date(st)
    etime = stime+timedelta(days=1)
    with conn.cursor() as cursor:
        sql2 = f"SELECT ts,mileage,fuelLevel,distance,fuelConsumption FROM dataPlatform.car where name = '{name}' and siteId = 87 and ts >='{stime}' and ts < '{etime}' order by ts desc limit 1 ;"
        cursor.execute(sql2)
        logger.info(sql2)
        if cursor.rowcount==0:
            return None,None,None,None,None
        else:
            for data in cursor:
                ts = data[0]
                mileage = data[1]
                fuel = data[2]
                distance = data[3]
                total = data[4]

            return ts,mileage,fuel,distance,total
        
def getTank(tank_list):
    with conn.cursor() as cursor:
        sql = f"SELECT a.name,b.tankCapacity FROM mgmtETL.DataETL a,mgmtETL.V3API b where  a.siteId = 87 and a.deviceId = b.label"
        cursor.execute(sql)
        logger.info(sql)
        for car in cursor:
            name = car[0]
            tank = car[1]
            tank_list[name] = tank

    return tank_list

def main(conn,st,et):
    value_string = ''
    tank_list = {}
    tank_list = getTank(tank_list)
    with conn.cursor() as cursor:
        sqlCommand = f"select siteId,gatewayId,name,dataETLName from mgmtETL.DataPlatform where name like 'car#%' and logicModel = 'normal' and siteId = 87 and logicModel is NOT NULL"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        logger.info(f"--------------- select car Mapping --------------- Took: {round(time.time()-s, 3)}s")
        for rows in infos:
            sId = rows[0]
            gId = rows[1]
            name = rows[2]
            etlName = rows[3]
            tank = tank_list[etlName]
            logger.info(f"--------------- select sId:{sId} gId:{gId} ETLname:{etlName}--------------- Took: {round(time.time()-s, 3)}s")
            sqlCommand =  f"""SELECT ts,mileage,fuelLevel,speed FROM dataETL.car 
                        where name = '{etlName}'
                        and ts >= '{st}' and  ts < '{et}' and gatewayId = {gId}
                        order by ts asc 
                    """
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.info (f"'{st}' to '{et}' has no data"  )
                continue
            else:
                last_ts,last_mileage,last_fuel,last_distance,last_total = getlast(conn,name,st)
                for row in datas:
                    ts = row[0]
                    mileage = row[1]
                    fuel_level = row[2]
                    speed = row[3]
                    distance = 0
                    total = 0
                    total_fuel = 0

                    if last_ts is None:
                        if fuel_level == 0:
                            value_string += f"('{ts}','{name}',{sId},{mileage},null,{distance},{total},{total_fuel},{speed}), "
                        else:     
                            value_string += f"('{ts}','{name}',{sId},{mileage},{fuel_level},{distance},{total},{total_fuel},{speed}), "

                    elif ts < last_ts:
                        continue

                    else :
                        km = mileage - last_mileage
                        last_mileage = mileage
                        distance =  last_distance + km
                        last_distance = distance
                        
                        if fuel_level > 0:
                            if last_fuel is None :
                                fuel = 0
                            else:
                                fuel = fuel_level - last_fuel

                            if fuel < 0:
                                last_fuel = fuel_level
                                total = last_total + abs(fuel)
                                total_fuel = (total/100)*tank
                                last_total = total
                            else:
                                last_fuel = fuel_level
                                total = last_total 
                                total_fuel = (total/100)*tank
                            value_string += f"('{ts}','{name}',{sId},{mileage},{fuel_level},{distance},{total},{total_fuel},{speed}), "
                        else :
                            value_string += f"('{ts}','{name}',{sId},{mileage},null,{distance},null,null,{speed}), "
                    

    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into dataPlatform.car(ts,name,siteId,mileage,fuelLevel,distance,fuelConsumption,totalFuelConsumption,speed) VALUES {value_string}"       
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
    logger = myLog.get_logger(os.getcwd(), f"{filename}.log",config['mysql_azureV2'], level=logging.ERROR)

    nowTime = datetime.now().replace(microsecond=0)
    logger.info(f"---------- Program Starts! : {nowTime} ---------- ")
    st = nowTime - timedelta(minutes=60)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")   
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
