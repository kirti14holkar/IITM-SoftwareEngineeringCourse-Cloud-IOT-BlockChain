import time
from EdgeServer import Edge_Server
from LightDevice import Light_Device
from ACDevice import AC_Device

WAIT_TIME = 1


# Function to query devices and get device id.
def _query_devices(device_id="", device_type="All", room_type="All"):
    # Get list of all registered devices.
    List_of_Devices = edge_server_1.get_registered_device_list()

    devices = []

    # query based on users inputs and find devices.
    if (device_id != ""):
        devices.append(device_id)

    elif (device_type != "All") and (room_type != "All") and (device_type != "") and (room_type != ""):
        for d in List_of_Devices:
            if (d["device_type"] == device_type) and (d["room_type"] == room_type):
                devices.append(d["deviceid"])

    elif (device_type == "All") and (room_type != "All") and (room_type != ""):
        for d in List_of_Devices:
            if (d["room_type"] == room_type):
                devices.append(d["deviceid"])

    elif (device_type != "All") and (room_type == "All") and (device_type != ""):
        for d in List_of_Devices:
            if (d["device_type"] == device_type):
                devices.append(d["deviceid"])

    elif (device_type == "All") and (room_type == "All"):
        for d in List_of_Devices:
            devices.append(d["deviceid"])

    # return devices list
    return devices


# Function to get Device Status by Device Id, Device Type and Room Type
def _get_status(device_id="", device_type="All", room_type="All"):
    devices = _query_devices(device_id=device_id, device_type=device_type, room_type=room_type)
    for d in devices:
        # publish message to device topic so that repective devices can share status
        edge_server_1._publish(d, action="status")

    # Function to Switch on off any type of Devices.


def _set_device_status(switch_status="OFF", device_id="", device_type="All", room_type="All"):
    devices = _query_devices(device_id=device_id, device_type=device_type, room_type=room_type)
    for d in devices:
        edge_server_1._publish(d, action="switch", switch=switch_status)

    # Function to Switch change light intensity.


def _set_light_intensity(light_intesity=1, device_id="", room_type="All"):
    devices = _query_devices(device_id=device_id, device_type="LIGHT", room_type=room_type)
    for d in devices:
        edge_server_1._publish(d, action="light_intensity", light_intensity=light_intesity)

    # Function to Switch change AC Temperature.


def _set_ac_temperature(temperature=25, device_id="", room_type="All"):
    devices = _query_devices(device_id=device_id, device_type="AC", room_type=room_type)
    for d in devices:
        edge_server_1._publish(d, action="temperature", temperature=temperature)


print("\nSmart Home Simulation started.")
# Creating the edge-server for the communication with the user

edge_server_1 = Edge_Server('edge_server_1')
time.sleep(WAIT_TIME)

# Creating the light_device
print("Intitate the device creation and registration process.")
print("\nCreating the Light devices for their respective rooms.")

light_device_1 = Light_Device("light_1", "Kitchen")
time.sleep(WAIT_TIME)

light_device_2 = Light_Device("light_2", "Kitchen")
time.sleep(WAIT_TIME)

light_device_3 = Light_Device("light_3", "BR1")
time.sleep(WAIT_TIME)

light_device_4 = Light_Device("light_4", "ChildrenRoom")
time.sleep(WAIT_TIME)

# Creating the ac_device
print("\nCreating the AC devices for their respective rooms. ")
ac_device_1 = AC_Device("ac_1", "BR1")
time.sleep(WAIT_TIME)

ac_device_2 = AC_Device("ac_2", "BR2")
time.sleep(WAIT_TIME)

ac_device_3 = AC_Device("ac_3", "BR3")
time.sleep(WAIT_TIME)

##Print All registered devices
List_of_Devices = edge_server_1.get_registered_device_list()

print("\n\nTotal Registered Devices : ")
for item in List_of_Devices:
    print(item)

print("\n\n")

# Get status of one device.
_get_status(device_id="light_2")

# Get status of all devices for one room type.
_get_status(room_type="BR1")

# Get status of all devices for one device type.
_get_status(device_type="LIGHT")

# Get status of all devices at home.
_get_status()

# Switch on one device.
_set_device_status(switch_status="ON", device_id="light_2")

# Switch on all devices for one room type.
_set_device_status(switch_status="ON", room_type="BR1")

# Switch on all devices for one device type.
_set_device_status(switch_status="ON", device_type="LIGHT")

# Switch on one device for one device type.
_set_device_status(switch_status="ON", device_type="LIGHT4")

# Switch on all devices at home.
_set_device_status(switch_status="ON")

time.sleep(WAIT_TIME * 2)

# set light intensity for one device.
_set_light_intensity(light_intesity=1, device_id="light_2")

# set light intensity for  all Light devices in one room type.
_set_light_intensity(light_intesity=2, room_type="Kitchen")

# Sset light intensity for all Light devices at home.
_set_light_intensity(light_intesity=3)

time.sleep(WAIT_TIME * 2)

# set AC Temp for one device.
_set_ac_temperature(temperature=25, device_id="ac_1")

# set AC Temp for  all AC devices in one room type.
_set_ac_temperature(temperature=26, room_type="BR1")

# Sset AC Temp for all AC devices at home.
_set_ac_temperature(temperature=20)

time.sleep(WAIT_TIME * 5)

print("\nSmart Home Simulation stopped.")

## Terminate the Edge Server Instances
edge_server_1.terminate()
