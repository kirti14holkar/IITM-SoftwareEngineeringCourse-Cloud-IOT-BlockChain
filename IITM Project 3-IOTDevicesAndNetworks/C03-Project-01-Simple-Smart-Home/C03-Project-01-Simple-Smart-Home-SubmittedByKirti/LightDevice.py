import json
import paho.mqtt.client as mqtt
import datetime

HOST = "localhost"
PORT = 1883


class Light_Device():
    # setting up the intensity choices for Smart Light Bulb
    _INTENSITY = ["LOW", "HIGH", "MEDIUM", "OFF"]

    def __init__(self, device_id, room):
        # Assigning device level information for each of the devices.
        self._device_id = device_id
        self._room_type = room
        self._light_intensity = self._INTENSITY[0]
        self._device_type = "LIGHT"
        self._device_registration_flag = False
        self.client = mqtt.Client(self._device_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.connect(HOST, PORT, keepalive=60)
        self.client.loop_start()
        self._switch_status = "OFF"
        self._register_device(self._device_id, self._room_type, self._device_type)

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
        elif (message["action"] == "light_intensity"):
            self._set_light_intensity(message["light_intensity"])
            self._get_light_intensity()
        else:
            print("Failed ! Invalid Action for : " + message["device_id"] + "\n")

    # Getting the current switch status of devices
    def _get_switch_status(self):
        print(self._device_id + " is switched : " + self._switch_status)
        return self._switch_status

    # Setting the the switch of devices
    def _set_switch_status(self, switch_state):
        self._switch_status = switch_state

    # Getting the light intensity for the devices
    def _get_light_intensity(self):
        if (self._switch_status == "ON"):
            print(self._device_id + " Light Intensity is set to : " + self._light_intensity + "\n")
            return self._light_intensity
        else:
            return 0

            # Setting the light intensity for devices

    def _set_light_intensity(self, light_intensity):
        if (light_intensity < len(Light_Device._INTENSITY)) and (light_intensity >= 0):
            self._light_intensity = Light_Device._INTENSITY[light_intensity]

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
        msg["light_intensity"] = self._get_light_intensity()
        msg["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg["action"] = action
        self.client.publish("Edge Server", json.dumps(msg))