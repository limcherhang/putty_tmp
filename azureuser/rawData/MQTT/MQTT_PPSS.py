import os
import sys
rootPath = os.getcwd()+'/../../'
sys.path.append(rootPath)

import configparser
import datetime
import queue
import time
import json
import logging

from connection.mysql_connection import MySQLConn
from connection.mqtt_connection import MQTT
from utils import myLog

if __name__ == "__main__":
    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0]
    logPath = os.getcwd()

    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"], level=logging.ERROR)
    
    conn = MySQLConn(config['mysql_azureV2'])

    MQTTclientId = config['MQTT_TW']['clientId']
    MQTTuser = config['MQTT_TW']['user']
    MQTTpwd = config['MQTT_TW']['password']
    MQTTport = int(config['MQTT_TW']['port'])

    MQTTips = []
    for k, v in config.items('MQTT_TW'):
        if k.find('ip') == 0:
            MQTTips.append(v)

    subClient_rawdata = 'SubppssRaw' + MQTTclientId # 'ProductionClient@ + yyyy-MM-dd_HH:mm:ss'
    subTopic_rawdata_pm = 'iotdata/+/powerMeter/#'
    subTopic_rawdata_flow = 'iotdata/+/flowMeter/#'
    subTopic_rawdata_temp = 'iotdata/+/dTemperature/#'

    subTopic_list = [(subTopic_rawdata_pm, 2), (subTopic_rawdata_flow, 2), (subTopic_rawdata_temp, 2)]

    logger.info(f"Now: {datetime.datetime.now().replace(microsecond=0)} Program Start!")
    logger.info("================================================ Config Setting ===============================================")
    logger.info(f"Protocol: PPSS")
    logger.info(f"MQTT Brokers: {MQTTips}")
    logger.info(f"MQTT Port: {MQTTport}")
    logger.info(f"MQTT User: {MQTTuser}")
    logger.info(f"MQTT Password: {MQTTpwd}")
    logger.info(f"Subscriber ClientId: {subClient_rawdata}")
    logger.info(f"Subscriber Topic: '{subTopic_list}'")
    logger.info("===============================================================================================================")

    dataQueue = queue.Queue(maxsize=0)

    for ip in MQTTips:
        subMQTT = MQTT(ip, MQTTport, MQTTuser, MQTTpwd, subClient_rawdata, subTopic_list, dataQueue)
        subMQTT.conn()

    data_list = []
    op_list = []
    while True:
        current_time = datetime.datetime.now()
        if current_time.hour == 0 and current_time.minute == 0:
            conn.close()
            conn = MySQLConn(config['mysql_azureV2'])
        value_string = ''

        while dataQueue.qsize() != 0:
            message = dataQueue.get()
            msg = message.payload.decode('utf-8')
            topic_list = message.topic.split('/')
            msg_dict = json.loads(msg)
  
            if 'powerMeter' in topic_list:
                
                gId = topic_list[1]
                name = topic_list[3]
                GWts = msg_dict.get('ts')
                ch1Watt = int(float(msg_dict.get('value'))*1000)
                data = {'ch1Watt': ch1Watt}
                data = json.dumps(data)

                value_string += f"('{datetime.datetime.now()}', {gId}, '{name}', '{GWts}', '{data}'), "
                if len(value_string) > 10000:
                    value_string = value_string[:-2]
                    sqlCommand = f"INSERT INTO rawData.PPSS(DBts, gatewayId, name, GWts, rawData)  VALUES {value_string}"
                    
                    logger.debug(f"executing the following query:")
                    logger.debug(sqlCommand)

                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(sqlCommand)
                            logger.debug("INSERT succeed!")
                        value_string = ''
                    except Exception as ex:
                        logger.error(f"ERROR in  {sqlCommand}")
                        logger.error(f"Error executing SQL query: {ex}")

            elif 'flowMeter' in topic_list:

                gId = topic_list[1] 
                name = topic_list[3]
                GWts = msg_dict.get('ts')
                flowRate = float(msg_dict.get('value'))
                data = {'flowRate': flowRate}
                data = json.dumps(data)

                value_string += f"('{datetime.datetime.now()}', {gId}, '{name}', '{GWts}', '{data}'), "
                if len(value_string) > 10000:
                    value_string = value_string[:-2]
                    sqlCommand = f"INSERT INTO rawData.PPSS(DBts, gatewayId, name, GWts, rawData)  VALUES {value_string}"
                    
                    logger.debug(f"executing the following query:")
                    logger.debug(sqlCommand)

                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(sqlCommand)
                            logger.debug("INSERT succeed!")
                        value_string = ''
                    except Exception as ex:
                        logger.error(f"ERROR in  {sqlCommand}")
                        logger.error(f"Error executing SQL query: {ex}")

            elif 'dTemperature' in topic_list:

                gId = topic_list[1]
                name = topic_list[3]
                GWts = msg_dict.get('ts')
                temp = float(msg_dict.get('value'))

                data = f"{gId}, '{name}', '{GWts}', {temp}"
                type = topic_list[4]

                process_list = data.replace("'","").split(', ')
                process_list.append(type)

                ts = datetime.datetime.strptime(process_list[2], '%Y-%m-%d %H:%M:%S').replace(second=0)

                if ts not in op_list:
                    op_list.append(ts)
                
                data_list.append(process_list)

            else:
                logger.error("Unknown mqtt type, please check your giving type")
                time.sleep(5)
            
            time.sleep(0.1)

        temp_list = []
        logger.debug(f"-------- data length: {len(data_list)} --------")
        for index, data in enumerate(data_list):
            ts = datetime.datetime.strptime(data[2], '%Y-%m-%d %H:%M:%S')
            if (datetime.datetime.now() - ts).seconds /60 >= 1:
                temp_list.append(index)
            
            logger.debug(f"{[index]} {data}")
        
        for index in temp_list[::-1]:
            logger.debug(f'deleting index: {index}')
            del data_list[index]
        
        if op_list:
            for ts in op_list:
                for _gId, l in {'112':['ppssbms000f', 'ppssbms0010', 'ppssbms0011', 'ppssbms0012'], '113':['ppssbms001f', 'ppssbms0020', 'ppssbms0021', 'ppssbms0022', 'ppssbms0024', 'ppssbms0025']}.items():
                    for _name in l:
                        logger.debug(f"Processing {_gId} {_name}")
                        temp1 = -999
                        temp2 = -999
                        temp3 = -999
                        temp4 = -999

                        index_list = []
                        for index, data in enumerate(data_list):
                            if datetime.datetime.strptime(data[2], '%Y-%m-%d %H:%M:%S').replace(second=0) == ts and data[0] == _gId and data[1] == _name:
                                if data[4] == "tempChws":
                                    temp1 = data[3]
                                elif data[4] == 'tempChwr':
                                    temp2 = data[3]
                                elif data[4] == 'tempCws':
                                    temp3 = data[3]
                                elif data[4] == 'tempCwr':
                                    temp4 = data[3]
                                index_list.append(index)

                        if temp1 != -999 and temp2 != -999 and temp3 != -999 and temp4 != -999:
                            temp_data = {'temp1': temp1, 'temp2': temp2, 'temp3': temp3, 'temp4': temp4}
                            temp_data = json.dumps(temp_data)

                            value_string += f"('{datetime.datetime.now()}', {_gId}, '{_name}', '{data[2]}', '{temp_data}'), "
                            logger.debug(f"{_gId}, '{_name}', '{data[2]}', {temp_data}")
                            logger.debug(f"index list for delete: {index_list}")
                            for index in index_list[::-1]:
                                del data_list[index]
                            
                            if len(value_string) > 10000:
                                value_string = value_string[:-2]
                                sqlCommand = f"INSERT INTO rawData.PPSS(DBts, gatewayId, name, GWts, rawData)  VALUES {value_string}"
                                
                                logger.debug(f"executing the following query:")
                                logger.debug(sqlCommand)

                                try:
                                    with conn.cursor() as cursor:
                                        cursor.execute(sqlCommand)
                                        logger.debug("INSERT succeed!")
                                    value_string = ''
                                except Exception as ex:
                                    logger.error(f"ERROR in  {sqlCommand}")
                                    logger.error(f"Error executing SQL query: {ex}")
                        elif _name == 'ppssbms0024' and temp1 != -999:
                            temp_data = {'temp1': temp1}
                            temp_data = json.dumps(temp_data)

                            value_string += f"('{datetime.datetime.now()}', {_gId}, '{_name}', '{data[2]}', '{temp_data}'), "
                            logger.debug(f"{_gId}, '{_name}', '{data[2]}', {temp_data}")
                            for index in index_list[::-1]:
                                del data_list[index]
                            if len(value_string) > 10000:
                                value_string = value_string[:-2]
                                sqlCommand = f"INSERT INTO rawData.PPSS(DBts, gatewayId, name, GWts, rawData)  VALUES {value_string}"
                                
                                logger.debug(f"executing the following query:")
                                logger.debug(sqlCommand)

                                try:
                                    with conn.cursor() as cursor:
                                        cursor.execute(sqlCommand)
                                        logger.debug("INSERT succeed!")
                                    value_string = ''
                                except Exception as ex:
                                    logger.error(f"ERROR in  {sqlCommand}")
                                    logger.error(f"Error executing SQL query: {ex}")
                        elif _name == 'ppssbms0025' and temp1 != -999:
                            temp_data = {'temp1': temp1}
                            temp_data = json.dumps(temp_data)

                            value_string += f"('{datetime.datetime.now()}', {_gId}, '{_name}', '{data[2]}', '{temp_data}'), "
                            logger.debug(f"{_gId}, '{_name}', '{data[2]}', {temp_data}")
                            for index in index_list[::-1]:
                                del data_list[index]
                            if len(value_string) > 10000:
                                value_string = value_string[:-2]
                                sqlCommand = f"INSERT INTO rawData.PPSS(DBts, gatewayId, name, GWts, rawData)  VALUES {value_string}"
                                
                                logger.debug(f"executing the following query:")
                                logger.debug(sqlCommand)

                                try:
                                    with conn.cursor() as cursor:
                                        cursor.execute(sqlCommand)
                                        logger.debug("INSERT succeed!")
                                    value_string = ''
                                except Exception as ex:
                                    logger.error(f"ERROR in  {sqlCommand}")
                                    logger.error(f"Error executing SQL query: {ex}")

        if value_string != '':
            value_string = value_string[:-2]
            sqlCommand = f"INSERT INTO rawData.PPSS(DBts, gatewayId, name, GWts, rawData)  VALUES {value_string}"
            
            logger.debug(f"executing the following query:")
            logger.debug(sqlCommand)

            try:
                with conn.cursor() as cursor:
                    cursor.execute(sqlCommand)
                    logger.debug("INSERT succeed!")
            except Exception as ex:
                logger.error(f"ERROR in  {sqlCommand}")
                logger.error(f"Error executing SQL query: {ex}")
      
        time.sleep(30)