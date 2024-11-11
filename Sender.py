import time
import random
import json
import paho.mqtt.client as mqtt
from paho.mqtt.subscribeoptions import SubscribeOptions
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

def on_message(client, userdata, message):
    print ('\nResponse received at Sender-side on topic: \'{}\', with payload:: \'{}\' and correlation data : \'{}\' '.format(message.topic, message.payload,message.properties.CorrelationData))



client_id = f'sender-{random.randint(0, 100)}'
# ****************** Change the endpoint below to your MQTT (no TLS, port 1883)
# ****************** broker (or get it here: https://www.hivemq.com/download/ )
# ****************** supporting topic aliasses 
broker_address = '192.168.0.209'


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id,  protocol=mqtt.MQTTv5)
client.connect(broker_address)
client.on_message = on_message # set callback

ResponseChannel = 'response/' + client_id

# subscribe myself to response topic
client.subscribe(ResponseChannel)

# Publish

Topic = 'sender-topic/cmd/' + client_id
Payload = """{
    "command": "start",
    "data" : "Your engine"
    }"""

publish_properties = Properties(PacketTypes.PUBLISH)
publish_properties.ResponseTopic = ResponseChannel


publish_properties.TopicAlias = 3  
publish_properties.CorrelationData = b"334"
client.publish(Topic, Payload, 1, properties=publish_properties) # now topic alias for Topic is set to 3 at the brokerside
print ('Command message: \'{}\' sent from (this) Sender App to broker on topic: \'{}\''.format(Payload, Topic,))
print ('with response topic set to: \'{}\' and TopicAlias set to:\'{}\''.format(publish_properties.ResponseTopic, publish_properties.TopicAlias))



print ('\n Now awaiting (max 20s) response from backend on (requested reponse-)topic \'{}\''.format(publish_properties.ResponseTopic))
client.loop_start()
time.sleep(10)  # wait
client.loop_stop()  # stop the loop