import json
import paho.mqtt.client as mqtt
import datetime

HOST = "localhost"
PORT = 1883


class AC_Device():
    _MIN_TEMP = 18
    _MAX_TEMP = 32

    def __init__(self, device_id, room):

        self._device_id = device_id
        self._room_type = room
        self._temperature = 22
        self._device_type = "AC"
        self._device_registration_flag = False
        self.client = mqtt.Client(self._device_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.connect(HOST, PORT, keepalive=60)
        self.client.loop_start()
        self._switch_status = "OFF"
        self._register_device(self._device_id, self._room_type, self._device_type)

    # calling registration method to register the device
    def _register_device(self, device_id, room_type, device_type):
        self._publish("Registration")
        self._device_registration_flag = True

    # Connect method to subscribe to various topics. 
    def _on_connect(self, client, userdata, flags, result_code):
        client.subscribe(self._device_id)
        # print(self._device_id +  " Connected to Topic : '" + self._device_id + "'")

    # method to process the recieved messages and publish them on relevant topics 
    # this method can also be used to take the action based on received commands
    def _on_message(self, client, userdata, msg):
        message = json.loads(msg.payload)
        print("\nCommand received on device and Executing Command : " + str(message["command"]))

        if (message["action"] == "status"):
            self._get_switch_status()
        elif (message["action"] == "switch"):
            self._set_switch_status(message["switch_status"])
            self._get_switch_status()
        elif (message["action"] == "temperature"):
            self._set_temperature(message["temperature"])
            self._get_temperature()
        else:
            print("Failed ! Invalid Action for : " + message["device_id"] + "\n")

            # print(message["device_id"] + " Switched :" + message["switch_status"])

    # Getting the current switch status of devices
    def _get_switch_status(self):
        print(self._device_id + " is switched : " + self._switch_status)
        return self._switch_status

    # Setting the the switch of devices
    def _set_switch_status(self, switch_state):
        self._switch_status = switch_state

    # Getting the temperature for the devices
    def _get_temperature(self):
        if (self._switch_status == "ON"):
            print(self._device_id + " Temperature is set to : " + str(self._temperature) + "\n")
            return self._temperature
        else:
            return 0

            # Setting up the temperature of the devices

    def _set_temperature(self, temperature):
        if ((temperature >= AC_Device._MIN_TEMP) and (temperature <= AC_Device._MAX_TEMP)):
            self._temperature = temperature
        elif (temperature < AC_Device._MIN_TEMP):
            self._temperature = AC_Device._MIN_TEMP
        elif (temperature > AC_Device._MAX_TEMP):
            self._temperature = AC_Device._MAX_TEMP

    def _on_disconnect(self, client, useddate, rc):
        self.client.disconnect()
        self.client.loop_stop()

    # Publish Message
    def _publish(self, action):
        msg = {}
        msg["device_id"] = self._device_id
        msg["device_type"] = self._device_type
        msg["room_type"] = self._room_type
        msg["switch_status"] = self._get_switch_status()
        msg["temperature"] = self._get_temperature()
        msg["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg["action"] = action
        self.client.publish("Edge Server", json.dumps(msg))