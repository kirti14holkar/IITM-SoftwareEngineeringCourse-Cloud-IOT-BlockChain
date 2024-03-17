import json
import datetime

from Database import Database
from AggregateDataModel import AggregateDataModel


class AlertDataModel:
    # Table name: bsm_alert_data
    # Partition key: A hyphenated concatenation of deviceid and ruleid
    # This is so that all unique facors for alert data are covered in partition and sort keys
    # Sort key: Minute level timestamp
    def __init__(self, config_file):
        self._db = Database('bsm_alert_data', 'deviceid', 'timestamp')

        # Composing one model in another is not a great idea
        # Ideally there should be a service layer for such orchestration
        # However, this is a simple use case and service layer wasn't really needed for anything else
        self._agg_model = AggregateDataModel()

        # Reading the config file and unserializing the rules
        self._config_file = config_file
        self._rules = self._ingest_rules(self._config_file)
        

    def _ingest_rules(self, config_file):
        with open(config_file) as config_fh:
            rules = json.loads(config_fh.read())
            return rules

    def query_exact(self, did_dtype, timestamp):
        return self._db.query_exact(did_dtype, timestamp)

    def query_range(self, did_dtype, start_timestamp, end_timestamp):
        return self._db.query_range(did_dtype, start_timestamp, end_timestamp)

    def put_item(self, item):
        return self._db.put_item(item)

    # Loop over each rule and fetch the corresponding aggregated data first
    # For that data, run thorugh it one by one, keeping necessary states like 
    # current running breach count, timestamp of the breach, type of the breach, etc.
    # Reset states if found an item without breach or if the rule is triggered
    def process_rules(self, device_id, start_timestamp, end_timestamp):
        start_time = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        end_time = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')

        for rule_id, rule in self._rules.items():
            did_dtype = device_id + '-' + rule['datatype']
            start_range = start_time.strftime('%Y-%m-%d %H:%M')
            end_range = end_time.strftime('%Y-%m-%d %H:%M')

            items = self._agg_model.query_range(did_dtype, start_range, end_range)
            breach_count = 0
            starting_timestamp = None
            last_breach_type = None
            
            for item in items:
                breach = False
                if ('avg_min' in rule and item['avg'] < rule['avg_min']):
                    breach = True
                    last_breach_type = 'min'
                elif ('avg_max' in rule and item['avg'] > rule['avg_max']):
                    breach = True
                    last_breach_type = 'max'

                if breach:
                    breach_count += 1
                    if breach_count == 1:
                        starting_timestamp = item['timestamp']
                elif breach_count:
                    # Reset the states if the continuation of breaches break
                    breach_count = 0
                    starting_timestamp = None
                    last_breach_type = None
                    
                # Log the alert and reset the states if trigger_count of breaches reached
                if breach_count >= rule['trigger_count']:
                    self._log_alert(device_id, rule_id, starting_timestamp, last_breach_type)
                    breach_count = 0
                    starting_timestamp = None
                    last_breach_type = None

    def _log_alert(self, device_id, rule_id, starting_timestamp, last_breach_type):
        print(f'Alert for device_id {device_id} on rule {rule_id} starting at {starting_timestamp} with breach type {last_breach_type}')
        item = {
                'deviceid-ruleid': device_id + '-' + rule_id, 
                'device_id': device_id, 
                'rule_id': rule_id,
                'timestamp': starting_timestamp, 
                'breach_type': last_breach_type
                }
        self.put_item(item)


