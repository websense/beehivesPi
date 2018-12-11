# read soil temp and moisture sensor from MQTT and write to csv file
# run  in /var/www/html directory so output file is web accessible
# nohup python3 /home/pi/beehives/mqtt-bee-client.py
import paho.mqtt.client as mqtt
import time
import datetime
import random
#import json
import tailer
import UWALoraSoilMConfiguration as UWASoil

def on_message(client, userdata, msg):
     #print(msg.topic + " " + str(msg.qos) + " ")
    data = msg.payload.decode('UTF-8')
     #timestamp, sensor, packet, temp voltage, soilmoisture, battery, checksum, rssi, noise, snr
    ss=str(msg.payload.decode("utf-8")).split(',')  #extract fields from csv input string
    timestampStr = str(ss[0])
    sensorID = str(ss[1])
    tempvoltage = float(ss[3])
    tempC = str(round((tempvoltage - 0.5)*100,1))
    watervoltage = float(ss[4])
    VMCpct = str(round((watervoltage * 50)/3,1))
    outstring = timestampStr + "," + tempC + "," + VMCpct + "\n"
    if sensorID == "0001":
        outfile = UWASoil.OUTPUT_ONE
    if sensorID == "0002":
        outfile = UWASoil.OUTPUT_TWO
    print(outstring + " to " + outfile)
    #append received string to csv file
    try:
        with open(outfile,"a") as fo:
            fo.write(outstring)
            fo.flush()
    except:
        print("File append exception: ", outfile)



def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("Connection done code=",rc)
    else:
        print("Bad connection code=",rc)
    # Start subscribe with QoS level 0
    cc = client.subscribe(UWASoil.BROKER_TOPIC, qos=0)

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



