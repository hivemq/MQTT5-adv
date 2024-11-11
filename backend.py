import paho.mqtt.client as mqtt
from paho.mqtt.subscribeoptions import SubscribeOptions
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import time
import random
import json



publish_properties = Properties(PacketTypes.PUBLISH)

client_id = f'backend-listener-{random.randint(0, 100)}'
# ****************** Change the endpoint below to your MQTT (no TLS, port 1883)
# ****************** broker (or get it here: https://www.hivemq.com/download/ )
# ****************** supporting topic aliasses 
broker_address = '192.168.0.209'

def send_ack(command, data, msg):
   publish_properties.CorrelationData = msg.properties.CorrelationData
   publish_properties.ResponseTopic = '*** DO NOT REPLY ***'
   client.publish(msg.properties.ResponseTopic, 'ACK '+ command, 1, properties=publish_properties)
   print ('\n\'ACK {}\' send to responsetopic \'{}\'....'.format( command, msg.properties.ResponseTopic))
   print ('... corr data for ACK : {}'.format(msg.properties.CorrelationData) )
          

def send_Nack(command, data, msg):
   publish_properties.CorrelationData = msg.properties.CorrelationData
   publish_properties.ResponseTopic = '*** DO NOT REPLY ***'
   client.publish(msg.properties.ResponseTopic, 'NACK '+ command, 1, properties=publish_properties)
   print ('\n\'NACK {}\' send to responsetopic \'{}\'....'.format( command, msg.properties.ResponseTopic))
   print ('... corr data for NACK : {}'.format(msg.properties.CorrelationData) )


# Define command handlers
def handle_start_command(data):
    print("Executing 'start' command with data:", data)
    # Implement your start logic here

def handle_stop_command(data):
    print("Executing 'stop' command with data:", data)
    # Implement your stop logic here

def handle_status_command(data):
    print("Executing 'status' command with data:", data)
    # Implement your status logic here    

# Command dispatcher to route different commands
def command_dispatcher(command, data):
    if command == "start":
        handle_start_command(data)
        return True
    elif command == "stop":
        handle_stop_command(data)
        return True
    elif command == "status":
        handle_status_command(data)
        return True
    else:
        print("Unknown command:", command)
        return False
       
        
# Callback for when a message is received on the subscribed topic
def on_message(client, userdata, msg):
    
    print ('Command received from sender on topic: \'{}\', with payload:: \'{}\' ..'.format(msg.topic,msg.payload))
    print ('..and reponse toipic set to:: \'{}\' ... '.format( msg.properties.ResponseTopic))
    print ('..and correlation data set to:: \'{}\' \n '.format( msg.properties.CorrelationData))

    try:
        # Decode and parse the JSON payload
        #print(f"recvd '{msg}'")
        payload = json.loads(msg.payload.decode())
        #print(f"PL: '{payload}'")
        command = payload.get("command")
        data = payload.get("data", {})
        
        if command:
            print(f"Received command: '{command}' with data: {data}")
            # Dispatch command to appropriate handler
            if command_dispatcher(command, data):
                # print(f"ACKing Received command: '{command}' with data: {data}")
                send_ack(command, data, msg)
            else:
                # print(f"N ACKing Received command: '{command}' with data: {data}")
                send_Nack(command, data, msg)
        else:
            print("Invalid message format: 'command' field is missing")
            print(f"NACKing Received command: '{command}' with data: {data}")
            send_Nack(command, data, msg)
    except json.JSONDecodeError:
        print("Failed to decode JSON payload:", msg.payload.decode())
        print(f"NACKing Received command: '{command}' with data: {data}")
        send_Nack(command, data, msg)



# This backend proces will listen for 20 seconds into 
# topic 'ListenTopic' (='sender-topic/cmd/#')  for start/stop/status commands (JSON formatted):
# {
#     "command": "start",
#     "data" : "Your engine"
# }
# Once a valid command is recieved a ACK message (or a NACK if invalid) 
# will be PUB-ed on the requested Responsetopic along with the set collelation data 




client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id,  protocol=mqtt.MQTTv5)
client.connect(broker_address)
client.on_message = on_message # set callback

ListenTopic = 'sender-topic/cmd/#'

client.subscribe(ListenTopic) # subscribe myself to response topic 'ListenTopic' 
print ('\n\nNow listening-in (max 20s) for commands from sender on topic \'{}\' \n'.format(ListenTopic))
client.loop_start()
time.sleep(200)  # wait
client.loop_stop()  # stop the loop