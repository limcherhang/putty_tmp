import logging
import paho
import paho.mqtt.client as mqtt
import datetime
import queue

logger = logging.getLogger(__name__)
class MQTT():

    def __init__(self, ip: str, port: int, user: str, pwd: str, clientId: str, topic: str, dataQ: queue.Queue):
        self.broker = ip
        self.port = port
        self.user = user
        self.pwd = pwd
        self.clientId = clientId
        self.topic = topic
        self.q = dataQ

        self.reconnect_Flag = False
        self.client = mqtt.Client(client_id=self.clientId, clean_session=False)

    def conn(self):
        self.client.username_pw_set(self.user, self.pwd)
        self.client.reconnect_delay_set(min_delay=1, max_delay=300)
        self.client.on_connect = self.__on_connect
        self.client.on_disconnect = self.__on_disconnect
        self.client.on_message = self.__on_message_raw
        
        try:
            logger.debug(f"Trying to connect to broker: {self.broker} / on topic: {self.topic}")
            self.client.connect(self.broker, port=self.port, keepalive=30)
        except Exception as ex:
            print(ex)
            logger.error(f"[MQTT Broker({self.broker}) Connetion Failed]: {str(ex)}")
            self.reconnect_Flag = True
        
        self.client.loop_start()

    def __on_connect(self, client: paho.mqtt.client.Client, userdata, flags: dict, rc: int):

        if rc == 0:
            logger.debug("0: Connection successful")
            if not self.reconnect_Flag:
                logger.info(f"Connection to MQTT Broker: {self.broker} on {self.topic} Succeed!")
            else:
                logger.critical(f"RE-connection to MQTT Broker: {self.broker} on {self.topic} Succeed! at {datetime.datetime.now().replace(microsecond=0)}")
                self.reconnect_Flag = False
            
            self.client.subscribe(self.topic, qos=2) 
        elif rc == 1:
            logger.error("1: Connection refused - incorrect protocol version")
        elif rc == 2:
            logger.error("2: Connection refused - invalid client identifier")
        elif rc == 3:
            logger.error("3: Connection refused - server unavailable")
        elif rc == 4:
            logger.error("4: Connection refused - bad username or password")
        elif rc == 5:
            logger.error("5: Connection refused - not authorised")
        else:
            logger.error(f"{rc}: other issues")

    def __on_disconnect(self, client: paho.mqtt.client.Client, userdata, rc: int):
        if rc != 0:
            logger.error(f"{rc}: Broker: {self.broker} Unexpected disconnection at {datetime.datetime.now().replace(microsecond=0)}.")
            self.reconnect_Flag = True

    def __on_message_raw(self, client: paho.mqtt.client.Client, userdata, message :paho.mqtt.client.MQTTMessage):
        msg = message.payload.decode('utf-8')
        logger.debug(f"{msg} on {message.topic} in __on_message_raw func.")
        # msg = str(message.payload.decode('utf-8'))
        logger.info(f"Receiving Message: '{str(msg)}' from {self.broker} on topic {message.topic}")
        # self.q.put(msg)

        self.q.put(message)
