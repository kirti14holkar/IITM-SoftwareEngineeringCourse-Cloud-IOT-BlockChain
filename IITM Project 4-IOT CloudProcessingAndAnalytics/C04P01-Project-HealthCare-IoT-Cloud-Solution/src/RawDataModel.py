from Database import Database
import datetime
import math

class RawDataModel:
    # Table name: bsm_data
    # Partition key: deviceid
    # Sort key: timestamp
    def __init__(self):
        self._db = Database('bsm_data', 'deviceid', 'timestamp')

    def query_exact(self, device_id, timestamp):
        return self._db.query_exact(device_id, timestamp)

    def query_range(self, device_id, start_timestamp, end_timestamp):
        return self._db.query_range(device_id, start_timestamp, end_timestamp)

    # This method loops over starting to end range in one minute intervals
    # It gets the corresponding data and maintains running stats by datatype and minute timestamp
    # Finally it calculates averages and returns the aggregated data structure
    def aggregate_by_datatype_and_minute(self, device_id, start_timestamp, end_timestamp):
        agg_data = {}

        start_time = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        end_time = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        current_time = start_time

        while (current_time < end_time):
            # timestamp to be stored in aggregate table would be at the minute level
            current_timestamp = current_time.strftime('%Y-%m-%d %H:%M')
            items = self.query_range(device_id, str(current_time), str(current_time + datetime.timedelta(minutes=1)))
            for item in items:
                if (item['datatype'] not in agg_data):
                    agg_data[item['datatype']] = {}
                if (current_timestamp not in agg_data[item['datatype']]):
                    agg_data[item['datatype']][current_timestamp] = {'sum': 0, 'count': 0, 'min': math.inf, 'max': -math.inf}

                agg_data[item['datatype']][current_timestamp]['sum'] += item['value']
                agg_data[item['datatype']][current_timestamp]['count'] += 1
                agg_data[item['datatype']][current_timestamp]['min'] = min(item['value'], agg_data[item['datatype']][current_timestamp]['min'])
                agg_data[item['datatype']][current_timestamp]['max'] = max(item['value'], agg_data[item['datatype']][current_timestamp]['max'])

            for datatype in agg_data:
                for timestamp in agg_data[datatype]:
                    agg_data[datatype][timestamp]['avg'] = agg_data[datatype][timestamp]['sum'] / agg_data[datatype][timestamp]['count']


            current_time += datetime.timedelta(minutes=1)

        return agg_data

