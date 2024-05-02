######################################
# python3 MQTT_TW.py --subType='bacnet' #
######################################
# or #
######################################
# python3 MQTT_TW.py --subType='modbus' #
######################################
# or # 
######################################
# python3 MQTT_TW.py --subType='zigbee' #
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
import logging

from connection.mysql_connection import MySQLConn
from connection.mqtt_connection import MQTT
from utils import myLog

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subType", default=None, type=str, help="Subcribe topic"
    ) # ["bacnet", "modbus", "zigbee"]

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

    MQTTclientId = config['MQTT_TW']['clientId']
    MQTTuser = config['MQTT_TW']['user']
    MQTTpwd = config['MQTT_TW']['password']
    MQTTport = int(config['MQTT_TW']['port'])

    MQTTips = []
    for k, v in config.items('MQTT_TW'):
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
        subTopic_list = [(subTopic_rawdata, 2), (subTopic_rawdata_Hank, 2)]
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
        while dataQueue.qsize() != 0:
            message = dataQueue.get()
            data_list.append(message)

        for message in data_list:
            data = str(message.payload.decode('utf-8'))
            topic = message.topic.split('/')
            
            if subType == 'zigbee':
                if 'zigbee' not in topic and 'zb' not in topic:
                    logger.error(f"ERROR Topic: {topic}")
                    continue
                msg_list = data.split(',')
                if len(msg_list) ==  6:
                    new_data = ''
                    for index, d in enumerate(msg_list):
                        if index == 2:
                            new_data += d + ', NULL, '
                        else:
                            new_data += d + ', '
                    value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {new_data[:-2]}), "
                    # sqlCommand = f"INSERT INTO rawData.zigbee(`DBts`, `GWts`, `ZBts`, `gatewayId`, `linkQuality`, `ieee`, `clusterID`, `rawData`) VALUES "
                else:
                    value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    # sqlCommand = f"INSERT INTO rawData.zigbee(`DBts`, `GWts`, `ZBts`, `gatewayId`, `linkQuality`, `ieee`, `clusterID`, `rawData`) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"

            else:
                if subType not in topic:
                    logger.error(f"ERROR Topic: {topic}")
                    continue
                if subType == 'bacnet':
                    value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    # sqlCommand = f"INSERT INTO `rawData`.`bacnet` (DBts, GWts, gatewayId, `name`, rawData) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"
                elif subType == 'modbus':
                    value_string += f"('{datetime.datetime.now().replace(microsecond=0)}', {data}), "
                    # sqlCommand = f"INSERT INTO `rawData`.`modbus` (`DBts`, `GWts`, `gatewayId`, `cmd`, `rawData`) VALUES ('{datetime.datetime.now().replace(microsecond=0)}', {data})"
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

            try:
                logger.debug(sqlCommand)
                with conn.cursor() as cursor:
                    cursor.execute(sqlCommand)
                    logger.debug("INSERT Succeed!")
            except Exception as ex:
                logger.error(f"ERROR in  {sqlCommand}")
                logger.error(f"Error executing SQL query: {ex}")

          
        time.sleep(0.01)
