from datetime import datetime, timedelta
import time
import os,sys,json
from pathlib import Path
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser
import logging

def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 11 and deviceLogic = 1 "
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        for row in infos:
            gId = row[0]
            label = row[1]
            name = row[2]
        
            sqlCommand = f"select APIts,rawdata from rawData.v3API where APIts>='{st}' and APIts<'{et}' and gatewayId = '{gId}' and label = '{label}'  order by APIts asc"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.info(f'{row} has no data')
            for data in datas:
                ts = data[0]
                rawdata = json.loads(data[1])
            
                mileage =rawdata['mileage']
                latitude = rawdata['latitude']
                longitude = rawdata['longitude']
                heading = rawdata['heading']
                headingValue = rawdata['headingValue']
                validity = rawdata['validity']
                event = rawdata['event']
                speed = rawdata['speed']
                battery_voltage = rawdata['battery_voltage']
                service_brake_circuit_1_air_pressure = rawdata['service_brake_circuit_1_air_pressure']
                fuel_level = rawdata['fuel_level']
                engine_coolant_temperature = rawdata['engine_coolant_temperature']
                engine_hours = rawdata['engine_hours']
                rpm = rawdata['rpm']
                temperature = rawdata['temperature']
                sec_temperature = rawdata['sec_temperature']
                main_power_voltage = rawdata['main_power_voltage']
                value_string += f"('{ts}','{gId}','{name}',{mileage},{latitude},{longitude},'{heading}',{headingValue},'{validity}','{event}',{speed},{battery_voltage},{service_brake_circuit_1_air_pressure},{fuel_level},{engine_coolant_temperature},{engine_hours},{rpm},{temperature},{sec_temperature},{main_power_voltage}), "
                
    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into dataETL.car(ts,gatewayId,name,mileage,latitude,longitude,heading,headingValue,validity,event,speed,batteryVoltage,serviceBrakeCircuit1AirPressure,fuelLevel,engineCoolantTemperature,engineHours,rpm,temperature,secTemperature,mainPowerVoltage) Values {value_string}"
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
    st = (nowTime - timedelta(minutes=30)).replace(second=0)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
