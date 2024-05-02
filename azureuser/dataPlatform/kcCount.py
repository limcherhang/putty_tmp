from datetime import datetime, timedelta
import time
from pathlib import Path
import sys , os ,re
rootPath = str(Path.cwd())+'/../'
sys.path.append(rootPath)
from utils import myLog
from connection.mysql_connection import MySQLConn
import configparser
import logging

def powerConsumed(formula,power_list):

    for power in power_list:
        pattern = fr"(?<!\w){power.lower()}+(?!\d)"
        formula = re.sub(pattern, str(power_list[power]), formula)
    try:
        if 'power' in formula:
            return None
        else:
            power = round(eval(formula), 2)
    except ZeroDivisionError:
        if power == 0:
            power = 0
        
    return power
                                
def energyConsumed(formula,energy_list):

    for energy in energy_list:
        pattern = fr"(?<!\w){energy.lower()}+(?!\d)"
        formula = re.sub(pattern, str(energy_list[energy]), formula)

    try:
        if 'power' in formula:
            return None
        else:
            energy = round(eval(formula), 2)
    except ZeroDivisionError:
        if energy == 0:
            energy = 0
    
    return energy                     
def main(conn,st,et):
    current_st = st.replace(hour=0,minute=0,second=0)
    with conn.cursor() as cursor:
        sqlCommand = f"SELECT siteId, name, formula, tsSource,level FROM mgmtETL.DataPlatform where logicModel = 'formula' order by level asc"
        try:
            logger.debug(sqlCommand)
            cursor.execute(sqlCommand)
            infos = cursor.fetchall()
        except Exception as ex:
            logger.error(f"Error executing {sqlCommand}")
            logger.error(f"Error message: {ex}")
            infos = []
        for rows in infos:
            sId = rows[0]
            names = rows[1]
            formula = rows[2]
            tsSource = rows[3]
            result = []
            result_list = ''
            value_string = ''
            if 'to' in formula.lower():  #power#1TOpower#20 1到20的加總
                a = ([int(s) for s in re.findall(r'-?\d+\.?\d*', formula)]) #
                b = formula[0:formula.find(str(a[0]))]
                formula = ''.join(f"{b}{i}+" for i in range(a[0], a[1] + 1))[:-1]

            result = (([str(s) for s in re.findall(r'power#\d+', formula)]))
            
            for row in result:
                result_list += f"'{row}', "

            result_list = result_list[:-2]
            
            sqlCommand = f"select ts from dataPlatform.power where siteId = {sId} and name = '{tsSource}' and ts >=  '{st}' and ts < '{et}' order by ts desc limit 1 "
            try:
                logger.debug(sqlCommand)
                cursor.execute(sqlCommand)
                time_datas = cursor.fetchall()
            except Exception as ex:
                logger.error(f"Error executing {sqlCommand}")
                logger.error(f"Error message: {ex}")
                time_datas = []
            for times in time_datas:
                last_time = times[0]
                start = last_time - timedelta(minutes=20)
                # 避免凌晨 抓到跨日的
                if start < current_st:
                    start = current_st

                power_list = {}
                energy_list = {}
                sqlCommand = f"""SELECT ts,name, powerConsumed, energyConsumed 
                                FROM dataPlatform.power 
                                WHERE siteId = {sId} AND name in ({result_list}) AND ts > '{start}' and ts <= '{last_time}' order by ts desc """
                try:
                    logger.debug(sqlCommand)
                    cursor.execute(sqlCommand)
                    datas = cursor.fetchall()
                except Exception as ex:
                    logger.error(f"Error executing {sqlCommand}")
                    logger.error(f"Error message: {ex}")
                    datas = []
                
                if len(datas) == 0:
                    logger.error(f"{row} has no data")
                    continue
                else:
                    for data in datas:
                        ts = data[0]
                        name = data[1]
                        power = data[2]
                        energy = data[3]
                        power_list[name]=power
                        energy_list[name]=energy

                    powerC = powerConsumed (formula,power_list)
                    energyC = energyConsumed (formula,energy_list)
                        
                        
                if powerC is None and energyC is None:
                    continue
                else:
                    value_string += f"('{last_time}', '{sId}', '{names}', {powerC}, {energyC}), "

    if value_string != '':
        value_string = value_string[:-2]                   
        with conn.cursor() as cursor:
            replace_sql = f"REPLACE INTO dataPlatform.power(ts, siteId, name, powerConsumed, energyConsumed) VALUES {value_string}"
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
    st = nowTime - timedelta(minutes=20)
    et = nowTime 
    main(conn,st,et)
    conn.close()
    logger.info(f"{conn.host} close")   
    logger.info(f"--------------- Program Done --------------- Took: {round(time.time()-s, 3)}s")