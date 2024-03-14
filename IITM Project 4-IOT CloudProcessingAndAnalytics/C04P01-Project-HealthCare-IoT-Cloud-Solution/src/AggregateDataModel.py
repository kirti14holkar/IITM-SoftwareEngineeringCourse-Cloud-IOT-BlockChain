from Database import Database

class AggregateDataModel:
    def __init__(self):
        # Table name: bsm_agg_data
        # Partition key: A hyphenated concatenation of deviceid and datatype
        # This is so that all unique factors for aggregate data are covered in partition and sort keys
        # Sort key: Minute level timestamp
        self._db = Database('bsm_agg_data', 'deviceid-datatype', 'timestamp')

    def query_exact(self, did_dtype, timestamp):
        return self._db.query_exact(did_dtype, timestamp)

    def query_range(self, did_dtype, start_timestamp, end_timestamp):
        return self._db.query_range(did_dtype, start_timestamp, end_timestamp)

    def put_item(self, item):
        return self._db.put_item(item)

    # Just takes the input aggregated data structure, loops by datatype, and timestamp within it
    # and inserts the aggregated items one by one
    # We can do bulk inserts here but normally it'll be triggered every minute or so it doesn't help as much
    def insert_aggregate_data(self, device_id, agg_data):
        for datatype in agg_data:
            for timestamp in agg_data[datatype]:
                item = {}
                item['deviceid-datatype'] = device_id + '-' + datatype
                item['device_id'] = device_id
                item['datatype'] = datatype
                item['timestamp'] = timestamp
                item['avg'] = round(agg_data[datatype][timestamp]['avg'], 2)
                item['min'] = round(agg_data[datatype][timestamp]['min'], 2)
                item['max'] = round(agg_data[datatype][timestamp]['max'], 2)
                self.put_item(item)


