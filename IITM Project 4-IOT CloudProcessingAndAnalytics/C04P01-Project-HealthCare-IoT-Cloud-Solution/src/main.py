from RawDataModel import RawDataModel
from AggregateDataModel import AggregateDataModel
from AlertDataModel import AlertDataModel


# Two device ids
DEVICE_ID_1 = 'BSM_G101'
DEVICE_ID_2 = 'BSM_G102'

# Start and end timestamps based on the data generated
START_TIMESTAMP = '2021-10-19 13:00:00.000000'
END_TIMESTAMP = '2021-10-19 14:30:00.000000'

RULES_CONFIG = '../config/rules.json'


raw_data_model = RawDataModel()
aggregate_data_model = AggregateDataModel()
alert_data_model = AlertDataModel(RULES_CONFIG)

# Aggregating items from raw model and then inserting them in the aggregate model
items = {}
print(f'Aggregating data for device {DEVICE_ID_1}')
items = raw_data_model.aggregate_by_datatype_and_minute(DEVICE_ID_1, START_TIMESTAMP, END_TIMESTAMP)
aggregate_data_model.insert_aggregate_data(DEVICE_ID_1, items)

items = {}
print(f'Aggregating data for device {DEVICE_ID_2}')
items = raw_data_model.aggregate_by_datatype_and_minute(DEVICE_ID_2, START_TIMESTAMP, END_TIMESTAMP)
aggregate_data_model.insert_aggregate_data(DEVICE_ID_2, items)


# Processing the rules on both devices
print(f'Processing rules for device {DEVICE_ID_1}')
alert_data_model.process_rules(DEVICE_ID_1, START_TIMESTAMP, END_TIMESTAMP)
print(f'Processing rules for device {DEVICE_ID_2}')
alert_data_model.process_rules(DEVICE_ID_2, START_TIMESTAMP, END_TIMESTAMP)

