import time
import random
import json
import paho.mqtt.client as mqtt
from paho.mqtt.subscribeoptions import SubscribeOptions
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

client_id = f'sender-{random.randint(0, 100)}'

# ****************** Change the endpoint below to your MQTT (no TLS, port 1883)
# ****************** broker (or get it here: https://www.hivemq.com/download/ )
# ****************** supporting topic aliasses 
broker_address = '192.168.0.209'

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id,  protocol=mqtt.MQTTv5)
client.connect(broker_address)

# Publish
Topic3 = 'sender-topic/cmd/ridiculously_long_topic_name/3'
Topic7 = 'sender-topic/cmd/another_long_topic_name/7'
publish_properties = Properties(PacketTypes.PUBLISH)

publish_properties.TopicAlias = 3  
# now topic alias for Topic3 is set to 3 at the brokerside
client.publish(Topic3, 'Initial payload with alias 3', 1, properties=publish_properties) 

publish_properties.TopicAlias = 7 
# now topic alias for Topic is set to 7 at the brokerside
client.publish(Topic7, "Initial payload with alias 7", 1, properties=publish_properties)

publish_properties.TopicAlias = 3 
 # now topic alias for Topic is set to 3 and no topic is explicitly set
client.publish('', "subsequent Payload on alias 3", 1, properties=publish_properties) 

publish_properties.TopicAlias = 7 
# now topic alias for Topic is set to 7 and no topic is explicitly set
client.publish('', "subsequent Payload on alias 7", 1, properties=publish_properties) 