# -*- coding: utf-8 -*-
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import MQTTErrorCode
import sys
import time


class PublishUtil:
    broker_hostname = 'localhost'
    broker_port = 1883
    clientobj = None

    def on_connect( self, client, userdata, flags, reason_code, properties ):
        if reason_code.is_failure == False:
            print( "Succeeded to connect to MQTT Broker." )
        else:
            print( "Failed to connect to MQTT Broker: reason=%s", reason_code.getName() )

    def on_disconnect( self, client, userdata, flags, reason_code, properties ):
        if reason_code.is_failure == False:
            print( "Succeeded to disconnect from MQTT Broker." )
        else:
            print( "Failed to disconnect from MQTT Broker: reason=%s", reason_code.getName() )

    def __init__( self, hostname="localhost", port=1883 ):
        self.broker_hostname = hostname
        self.broker_port = port
        self.clientobj = mqtt_client.Client( mqtt_client.CallbackAPIVersion.VERSION2 )
        self.clientobj.on_connect = self.on_connect
        self.clientobj.on_disconnect = self.on_disconnect

    def connect( self ) -> MQTTErrorCode :
        result = self.clientobj.connect( self.broker_hostname, self.broker_port )
        return result

    def disconnect( self ) -> MQTTErrorCode :
        result = self.clientobj.disconnect()
        return result

    def publish( self, topic, msg ) -> MQTTErrorCode :
        self.clientobj.loop_start()
        result = self.clientobj.publish( topic, msg )
        status = result[0]
        self.clientobj.loop_stop()
        return status


def publish(topic: str, msg: str):
    hostname = 'localhost'
    port = 1883

    pubobj = PublishUtil( hostname, port )
    pubobj.connect()
    status = pubobj.publish( topic, msg )
    if status == 0:
        print( f"Sent topic: {topic}  message:{msg}" )
    else:
        print( f"Failed to send message({msg}) to topic {topic}" )

    pubobj.disconnect()
