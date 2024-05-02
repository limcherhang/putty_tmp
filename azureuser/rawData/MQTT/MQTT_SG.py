######################################
# python3 MQTT_TW --subType='bacnet' #
######################################
# or #
######################################
# python3 MQTT_TW --subType='modbus' #
######################################
# or # 
######################################
# python3 MQTT_TW --subType='zigbee' #
######################################

import os
import sys
rootPath = os.getcwd()+'/../../'
sys.path.append(rootPath)

import configparser
import argparse
import datetime
import queue
import time
import json
import logging
import pytz
from dateutil import parser as p

from connection.mysql_connection import MySQLConn
from connection.mqtt_connection import MQTT
from utils import myLog

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subType", default=None, type=str, help="Subcribe topic"
    ) # ["bacnet", "modbus", "zigbee","m2m"]

    args = parser.parse_args()

    subType = args.subType.lower()  # 無論返回的大小寫都一律轉換成小寫

    file = __file__
    basename = os.path.basename(file)
    filename = os.path.splitext(basename)[0] + '_' + subType
    logPath = os.getcwd()
    
    config = configparser.ConfigParser()
    config.read(rootPath+'/config.ini')
    
    conn = MySQLConn(config['mysql_azureV2'])

    logger = myLog.get_logger(logPath, f"{filename}.log", config["mysql_azureV2"], level=logging.ERROR)

    MQTTclientId = config['MQTT_SG']['clientId']
    MQTTuser = config['MQTT_SG']['user']
    MQTTpwd = config['MQTT_SG']['password']
    MQTTport = int(config['MQTT_SG']['port'])

    MQTTips = []
    for k, v in config.items('MQTT_SG'):
        if k.find('ip') == 0:
            MQTTips.append(v)

    if subType == 'bacnet':
        subClient_rawdata = 'SubbacnetRaw' + MQTTclientId
        subTopic_rawdata = "bacnet/+/raw"
        subTopic_rawdata_Hank = 'rawdata/bacnet/raw' # Hank's MQTT Topic: rawdata/protocol/raw for BACnet
        subTopic_list = [(subTopic_rawdata, 2), (subTopic_rawdata_Hank, 2)]
    elif subType == "modbus":
        subClient_rawdata = 'SubmodbusRaw' + MQTTclientId
        subTopic_rawdata = 'modbus/+/raw' # 'modbus/+/raw'
        subTopic_rawdata_Hank = 'rawdata/modbus/raw' # Hank's MQTT Topic: rawdata/modbus/raw for Modbus
        subTopic_list = [(subTopic_rawdata, 2), (subTopic_rawdata_Hank, 2)]
    elif subType == "zigbee":
        subClient_rawdata = 'SubzigbeeRaw' + MQTTclientId
        subTopic_rawdata = 'zigbee/+/raw' # 'zigbee/+/raw'  #優點:一對多、訊息分類，訂閱者只取自己相關的訊息
        subTopic_rawdata_Hank = 'rawdata/zb/raw' # Hank's MQTT Topic: 'rawdata/zb/raw' for Zigbee #缺點 一對一
        subTopic_rawdata_HBF = 'ISSHBF/iaqsensor'
        subTopic_rawdata_8CL = 'ISS8CL/iaqsensor'
        subTopic_list = [(subTopic_rawdata, 2), (subTopic_rawdata_Hank, 2), (subTopic_rawdata_HBF, 2), (subTopic_rawdata_8CL, 2)]
    elif subType == "m2m":
        subClient_rawdata = 'Subm2mRaw' + MQTTclientId
        subTopic_rawdata = 'm2m/+/raw' # m2m/+/raw
        subTopic_rawdata_Hank = 'JHP/065166' # Hank's MQTT Topic: JHP/065166 for M2M GW in JHP
        subTopic_rawdata_Hank2 = 'Singrow/065165' # Hank's MQTT Topic: Singrow/065165 for M2M GW in Singrow
        subTopic_rawdata_HDB = 'M2M/gatewayID/raw' # HDB Smart Pump Testing MQTT Topic: 'M2M/gatewayID/raw'
        subTopic_rawdata_JHP = '+/065166'
        
        subTopic_list = [(subTopic_rawdata,2), (subTopic_rawdata_Hank, 2), (subTopic_rawdata_Hank2, 2), (subTopic_rawdata_HDB, 2), (subTopic_rawdata_JHP, 2)]
    else:
        logger.error("Unknown mqtt type, please check your giving type")
        time.sleep(5)

    logger.info(f"Now: {datetime.datetime.now().replace(microsecond=0)} Program Start!")
    logger.info("================================================ Config Setting ===============================================")
    logger.info(f"Protocol: {subType}")
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

    
    while True:
        current_time = datetime.datetime.now()
        if current_time.hour == 0 and current_time.minute == 0 and current_time.second ==0:
            conn.close()
            conn = MySQLConn(config['mysql_azureV2'])
        data_list = []
        value_string = ''
        value_string_ISS = ''
        while dataQueue.qsize() != 0:
            message = dataQueue.get()
            data_list.append(message)

        for message in data_list:
            data = str(message.payload.decode('utf-8'))
            topic = message.topic.split('/')
            if subType == 'zigbee':
                if 'zigbee' in topic or 'zb' in topic:
                    # if 'zb' in topic:
                    #     value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    #     # sqlCommand = f"INSERT INTO rawData.zigbee(`DBts`, `GWts`, `ZBts`, `gatewayId`, `ieee`, `clusterID`, `rawData`) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"
                    # else:
                    #     value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    #     # sqlCommand = f"INSERT INTO rawData.zigbee(`DBts`, `GWts`, `ZBts`, `gatewayId`, `linkQuality`, `ieee`, `clusterID`, `rawData`) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"
                    msg_list = data.split(',')
                    if len(msg_list) == 6:
                        new_data = ''
                        for index, d in enumerate(msg_list):
                            if index == 2:
                                new_data += d + ', NULL, '
                            else:
                                new_data += d + ', '
                        value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {new_data[:-2]}), "
                    else:
                        value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "

                elif 'ISSHBF' in topic or 'ISS8CL' in topic:
                    json_data = json.loads(data)
                    devEui = json_data['devEUI']
                    # gId: 301 --> HBF
                    # gId: 302 --> 8CL
                    if 'ISSHBF' in topic:
                        value_string_ISS += f"('{datetime.datetime.now().replace(microsecond=0)}', 301, '{devEui}', {json.dumps(data)}), "
                        # sqlCommand = f"""INSERT INTO rawData.milesight(DBts, gatewayId, devEui, rawData) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', 301, '{devEui}', {json.dumps(data)})"""
                    else:
                        value_string_ISS += f"('{datetime.datetime.now().replace(microsecond=0)}', 302, '{devEui}', {json.dumps(data)}), "
                        # sqlCommand = f"""INSERT INTO rawData.milesight(DBts, gatewayId, devEui, rawData) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', 302, '{devEui}', {json.dumps(data)})"""
    
                else:
                    logger.error(f"ERROR Topic: {topic}")
                    continue
            else:
                if subType not in topic and subType.upper() not in topic:
                    logger.error(f"ERROR Topic: {topic}")
                    continue
                if subType == 'bacnet':
                    value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    # sqlCommand = f"INSERT INTO `rawData`.`bacnet` (DBts, GWts, gatewayId, `name`, rawData) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"
                elif subType == 'modbus':
                    value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    # sqlCommand = f"INSERT INTO `rawData`.`modbus` (`DBts`, `GWts`, `gatewayId`, `cmd`, `rawData`) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"
                elif subType == 'm2m':
                    dict = json.loads(data)
                    ts = p.parse(dict['T']).astimezone(pytz.timezone('Asia/Singapore')).strftime('%Y-%m-%d %H:%M:%S')
                    for d in dict['DATA']:
                        name, status = d.values()
                        value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', '{ts}', '300', '{name}', '{status}'), "
                    # if value_string != '':
                    #     value_string = value_string[:-2]
                    #     sqlCommand = f"INSERT INTO `rawData`.`contecM2M` (`DBts`, `GWts`, `gatewayId`, `name`, `rawData`) VALUES {value_string}"
                else:
                    logger.error("Unknown mqtt type, please check your giving type")
                    time.sleep(5)

        if value_string != '':
            value_string = value_string[:-2]
            if subType == 'zigbee':
                sqlCommand = f"INSERT INTO rawData.zigbee(`DBts`, `GWts`, `ZBts`, `gatewayId`, `linkQuality`, `ieee`, `clusterID`, `rawData`) VALUES {value_string}"
            elif subType == 'bacnet':
                sqlCommand = f"INSERT INTO `rawData`.`bacnet` (DBts, GWts, gatewayId, `name`, rawData) VALUES {value_string}"
            elif subType == 'modbus':
                sqlCommand = f"INSERT INTO `rawData`.`modbus` (`DBts`, `GWts`, `gatewayId`, `cmd`, `rawData`) VALUES {value_string}"
            elif subType == 'm2m':
                sqlCommand = f"INSERT INTO `rawData`.`contecM2M` (`DBts`, `GWts`, `gatewayId`, `name`, `rawData`) VALUES {value_string}"

            try:
                logger.debug(sqlCommand)
                with conn.cursor() as cursor:
                    cursor.execute(sqlCommand)
                    logger.debug("INSERT Succeed!")
            except Exception as ex:
                logger.error(f"ERROR in  {sqlCommand}")
                logger.error(f"Error executing SQL query: {ex}")


        if value_string_ISS != '':
            value_string_ISS = value_string_ISS[:-2]
            sqlCommand = f"INSERT INTO rawData.milesight(DBts, gatewayId, devEui, rawData) VALUES {value_string_ISS}"

            try:
                logger.debug(sqlCommand)
                with conn.cursor() as cursor:
                    cursor.execute(sqlCommand)
                    logger.debug("INSERT Succeed!")
            except Exception as ex:
                logger.error(f"ERROR in  {sqlCommand}")
                logger.error(f"Error executing SQL query: {ex}")
        time.sleep(0.01)
