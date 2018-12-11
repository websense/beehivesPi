# run MQTT-python 
import paho.mqtt.client as mqtt
import time
import datetime
import random
import json
import UWAMatthiasBeehiveConfiguration as UWAbee


def on_message(client, userdata, msg):
    #print(msg.topic + " " + str(msg.qos) + " ")
    #print(str(msg.payload.decode("utf-8")))
    #timestamp, sensor, packet, temp voltage, soilmoisture, battery, checksum, rssi, noise, snr
    ss=str(msg.payload.decode("utf-8")).split(',')  #extract fields from csv input string
    if len(ss)!=10:
        print("Unexpected Msg: ",ss)
    else:
        timestampStr = str(ss[0])
        stationID = str(ss[1])
        scalevoltage = float(ss[3])
        # Example:  Voltage=-1.029 -> Weight= 46,1 kg
        weightKG = str(round((-52.4755*scalevoltage-7.84119),3))
        temperatureC = str(round(float(ss[4]),1))
        batteryV = str(round(float(ss[5]),1))
        #format must match UWAbee.DATA_HEADER
        outstringData = timestampStr + "," + stationID + "," + weightKG + "," + temperatureC + "," + batteryV + "\n"
        print(outstringData)
        pktID = ss[2]
        rssi = ss[7]
        noise = ss[8]
        snr = ss[9]
        #format to match UWAbee.CHANNEL_HEADER
        outstringChannel = timestampStr + "," + stationID + "," + pktID + ","  + rssi + "," + noise + "," + snr
        print(outstringChannel)
        #append received string to data  csv file
        try:
            with open(UWAbee.OUTPUT_DATA,"a") as fo:
                fo.write(outstringData)
                fo.flush()
        except:
            print("File append exception: ", UWAbee.OUTPUT_DATA)
        #append received string to channel csv file
        try:
            with open(UWAbee.OUTPUT_CHANNEL,"a") as f1:
                f1.write(outstringChannel)
                f1.flush()
        except:
            print("File append exception: ", UWAbee.OUTPUT_CHANNEL)


def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("Connection done code=",rc)
    else:
        print("Bad connection code=",rc)
    # Start subscribe with QoS level 0
    cc = client.subscribe(UWAbee.BROKER_TOPIC, qos=0)
    #cc = client.subscribe("Matthias-Heartbeat",qos=0)

def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed code, qos = " + str(mid) + " " + str(granted_qos))



# Define client
c_id = str(random.randint(1,999999999))
client = mqtt.Client(client_id=c_id)

# Assign event callbacks
client.on_message=on_message
client.on_connect=on_connect
client.on_subscribe=on_subscribe

# Make files with headers if needed
try:
    file = open(UWAbee.OUTPUT_DATA, 'r')
except IOError:
    file = open(UWAbee.OUTPUT_DATA, 'a')
    file.write(UWAbee.DATA_HEADER)
    file.flush()
try:
    file = open(UWAbee.OUTPUT_CHANNEL, 'r')
except IOError:
    file = open(UWAbee.OUTPUT_CHANNEL, 'a')
    file.write(UWAbee.CHANNEL_HEADER)
    file.flush()

# Connect
client.username_pw_set(UWAbee.CLIENT_USER, UWAbee.CLIENT_PASSWORD)
client.connect(host=UWAbee.BROKER_ADDRESS, port=UWAbee.PORT_ID, keepalive=90, bind_address="")
# on connect starts the subscription
print("Wait connect")
time.sleep(4) # Wait for connection setup to complete
# Continue the network loop forever
client.loop_forever()

# system automatically checks if connection live - if not restart




