import random
from pymongo import MongoClient
from datetime import datetime

HOST = '127.0.0.1'
PORT = '27017'

RELATIVE_CONFIG_PATH = '../config/'

DB_NAME = 'reservoir_db'
DEVICE_COLLECTION = 'devices'
RESERVOIR_DATA_COLLECTION = 'reservoir_data'

db_handle = MongoClient(f'mongodb://{HOST}:{PORT}')

db_handle.drop_database(DB_NAME)

reservoir_dbh = db_handle[DB_NAME]

with open(RELATIVE_CONFIG_PATH + DEVICE_COLLECTION + '.csv', 'r') as device_fh:
    for row in device_fh:
        row = row.rstrip()

        if row:
            device_id, desc, type, manufacturer = row.split(',')
            device_data = {
                'device_id': device_id,
                'desc': desc,
                'type': type,
                'manufacturer': manufacturer
            }

            device_collection = reservoir_dbh[DEVICE_COLLECTION]
            device_collection.insert_one(device_data)

with open(RELATIVE_CONFIG_PATH + DEVICE_COLLECTION + '.csv', 'r') as device_fh:
    for row in device_fh:
        row = row.rstrip()
        if row:
            (device_id, _, type, _) = row.split(',')

        for day in range(1, 7):
            for hour in range(0, 24):
                timestamp = datetime(2021, 12, day, hour, 30, 0)

                value = None

                if (type.lower() == 'salinity'):
                    value = int(random.normalvariate(1000, 100))
                elif (type.lower() == 'calcium'):
                    value = int(random.normalvariate(75, 10))

                reservoir_data = {
                    'device_id': device_id,
                    'value': value,
                    'timestamp': timestamp
                }

                reservoir_data_collection = reservoir_dbh[RESERVOIR_DATA_COLLECTION]

                reservoir_data_collection.insert_one(reservoir_data)