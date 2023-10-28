import json
import time
import paho.mqtt.client as mqtt
import datetime

HOST = "localhost"
PORT = 1883
WAIT_TIME = 0.25


class Edge_Server:
    COMMAND_NUMBER = 0

    def __init__(self, instance_name):
        self._instance_id = instance_name
        self.client = mqtt.Client(self._instance_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.connect(HOST, PORT, keepalive=60)
        self.client.loop_start()
        self._registered_list = []

    # Terminating the MQTT broker and stopping the execution
    def terminate(self):
        self.client.disconnect()
        self.client.loop_stop()

    # Connect method to subscribe to various topics.     
    def _on_connect(self, client, userdata, flags, result_code):
        client.subscribe("Edge Server")
        # print("Edge Server Connected to Topic 'Edge Server'")

    # method to process the recieved messages and publish them on relevant topics 
    # this method can also be used to take the action based on received commands
    def _on_message(self, client, userdata, msg):
        message = json.loads(msg.payload)
        if (message["action"] == "Registration"):
            device = {"deviceid": message["device_id"], "device_type": message["device_type"],
                      "room_type": message["room_type"], }
            self._registered_list.append(device)
        print(str(device) + " Registered on Edge.")

    # Returning the current registered list
    def get_registered_device_list(self):
        return self._registered_list

    # Getting the status for the connected devices
    def get_status(self):
        pass

    # Controlling and performing the operations on the devices
    # based on the request received
    def set(self):
        pass

    # Publish Message
    def _publish(self, device_id, action, switch="OFF", temperature=26, light_intensity=2):
        Edge_Server.COMMAND_NUMBER = Edge_Server.COMMAND_NUMBER + 1
        msg = {}
        msg["device_id"] = device_id
        msg["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg["action"] = action
        msg["switch_status"] = switch
        msg["temperature"] = temperature
        msg["light_intensity"] = light_intensity
        msg["command"] = Edge_Server.COMMAND_NUMBER
        print("\nCommand Number : " + str(Edge_Server.COMMAND_NUMBER) + " initiated from Edge Server.")
        self.client.publish(device_id, json.dumps(msg))