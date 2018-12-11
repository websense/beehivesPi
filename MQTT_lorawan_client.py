# TTN version read soil temp and moisture sensor from MQTT and write to csv file
# run  in /var/www/html directory so output file is web accessible
# nohup python3 /home/pi/beehives/mqtt-bee-client.py
import paho.mqtt.client as mqtt
import time
import datetime
import random
import json
import UWATTNSoilMConfiguration as UWASoil

def on_message(client, userdata, message):
    decoded_message = str(message.payload.decode("utf-8"))
    #print("message received " , decoded_message)
    parsed_json = json.loads(decoded_message)
    #print(json.dumps(parsed_json, indent=4))
    timestampStr = (parsed_json['metadata']['time']).split(".")[0] #ignore millisecs
    sensorID = (parsed_json['dev_id'])
    tempC = str(parsed_json['payload_fields']['temperature'])
    VMCpct =  str(parsed_json['payload_fields']['moisture'])
    outstring = timestampStr + "," + tempC + "," + VMCpct + "\n"
    print(outstring + sensorID)
    if sensorID=="uwa_node_3":
        outfile = UWASoil.OUTPUT_THREE
    if sensorID=="uwa_node_4":
        outfile = UWASoil.OUTPUT_FOUR
    #append received string to csv file
    try:
        with open(outfile,"a") as fo:
            fo.write(outstring)
            fo.flush()
            #print("File append OK to ",outfile)
    except:
        print("File append exception: ", outfile)


def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("Connection done code=",rc)
    else:
        print("Bad connection code=",rc)
    # Start subscribe with QoS level 0
    cc = client.subscribe(UWASoil.CLIENT_USER + UWASoil.BROKER_TOPIC, qos=0)

def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed code, qos = " + str(mid) + " " + str(granted_qos))



# Define client
c_id = str(random.randint(1,999999999))
client = mqtt.Client(client_id=c_id)

# Assign event callbacks
client.on_message=on_message
client.on_connect=on_connect
client.on_subscribe=on_subscribe

# Connect
client.username_pw_set(UWASoil.CLIENT_USER, UWASoil.CLIENT_PASSWORD)
client.connect(host=UWASoil.BROKER_ADDRESS, port=UWASoil.PORT_ID, keepalive=90, bind_address="")
# on connect starts the subscription
print("Wait connect")
time.sleep(4) # Wait for connection setup to complete
# Continue the network loop forever
client.loop_forever()

# system automatically checks if connection live - if not restart



