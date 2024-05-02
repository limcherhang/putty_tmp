from datetime import datetime, timedelta
import time
import os,sys,json
from pathlib import Path
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog, util
from connection.mysql_connection import MySQLConn
import configparser
import logging

def getNtotal(conn,time,gId,name):
    with conn.cursor() as cursor:
        sql = f"SELECT totalNegative FROM dataETL.power where ts < '{time}' and gatewayId = {gId} and name = '{name}' order by ts desc limit 1"
        try:
            logger.debug(sql)
            cursor.execute(sql)
            data = cursor.fetchone()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            data = (None, )
    if data is None :
        return 'null'
    elif data == ():
        return 'null'
    else:
        return data[0]
    
def main(conn,st,et):
    value_string = ''
    with conn.cursor() as cursor:
        sql = f"SELECT gatewayId,deviceId,name FROM mgmtETL.DataETL  where deviceType = 1 and deviceLogic = 2"
        try:
            logger.debug(sql)
            cursor.execute(sql)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sql}")
            logger.error(f"Error message: {ex}")
            infos = []
        for rows in infos:
            gId = rows[0]
            dId = rows[1]
            name = rows[2]
            
            sqlCommand = f"select APIts, rawdata from rawData.sigfoxAPI where APIts>='{st}' and APIts<'{et}' and  gatewayId='{gId}' and deviceId='{dId}' order by APIts asc"
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                datas = []
            if len(datas) == 0:
                logger.warning(f"has no data in '{st}' to '{et}'")
                continue
            for row in datas:
                ts = row[0]
                rawdata = json.loads(row[1])
                data = rawdata['data']
                header = util.signed_hex2dec(data[0:2])
                Ntotal = getNtotal(conn,ts,gId,name)
                
                if header == 1:
                    sId = util.signed_hex2dec(data[2:4])
                    kWh = util.signed_hex2dec(data[4:12]) 
                    pf = util.signed_hex2dec(data[12:16]) 
                    w = util.signed_hex2dec(data[16:24])
                    
                elif header == 4:
                    NkWh = util.signed_hex2dec(data[4:12]) # activeEnergyKwh
                    Ntotal = round(float(NkWh),3)
                    
                if gId == 248 and sId == 2 and dId == '40B268':
                    name = 'Power#2'
                elif gId == 248 and sId == 1 and dId == '40B268':
                    name = 'Power#1'

                totalPositiveWattHour = ('null' if kWh is None else round(float(kWh),3)) # kWH > WH
                ch1Watt = ('null' if w is None else round(float(w)/1000,3))
                powerFactor = ('null' if pf is None else round(float(pf)/1000,3))
                
                value_string += f"('{ts}',{gId},'{name}',{ch1Watt},{powerFactor},{totalPositiveWattHour},{Ntotal}), ".replace("None", "NULL")
                        
    if value_string != '':
        value_string = value_string[:-2]
        with conn.cursor() as cursor:
            replace_sql = f"replace into `dataETL`.`power` (`ts`, `gatewayId`, `name`, `ch1Watt`,`ch1PowerFactor`, `total`, `totalNegative`) Values {value_string}"
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
    st = (nowTime - timedelta(minutes=60)).replace(second=0)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")
