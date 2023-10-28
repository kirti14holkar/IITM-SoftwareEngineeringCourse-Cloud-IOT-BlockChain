from model import DeviceModel, ReservoirDataModel, DailyReportModel
import math, datetime

class DeviceService:
    def __init__(self):
        self._latest_error = ''
        self._model = DeviceModel()

    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error

    def find_by_device_id(self, device_id):
        self.latest_error = ''

        return self._model.find_by_device_id(device_id)

    def insert(self, device_id, desc, type, manufacturer):
        self.latest_error = ''

        device_doc = self._model.insert(device_id, desc, type, manufacturer)

        if device_doc == -1:
            self.latest_error = self._model.latest_error

        return device_doc


class ReservoirDataService:
    
    def __init__(self):
        self._latest_error = ''
        self._model = ReservoirDataModel()
    
    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error
    
    def find_by_device_id_and_timestamp(self, device_id, timestamp):
        self.latest_error = ''
        
        return self._model.find_by_device_id_and_timestamp(device_id, timestamp)
    
    def aggregate(self, pipeline):       
        self.latest_error = ''
        
        reservoir_data_docs = self._model.aggregate(pipeline)
        return reservoir_data_docs


    def insert(self, device_id, value, timestamp):
        self.latest_error = ''

        reservoir_data_doc = self._model.insert(device_id, value, timestamp)
        
        if (reservoir_data_doc == -1):
            self.latest_error = self._model.latest_error
        return reservoir_data_doc


class DailyReportService:

    def __init__(self):
        self._latest_error = ''
        self._model = DailyReportModel()
    
    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error

    def find_by_device_id_and_date(self, device_id, date):
        self.latest_error = ''

        report_doc = self._model.find_by_device_id_and_date(device_id, date)

        report_data = {
            'device_id': report_doc['device_id'], 
            'avg_value': report_doc['avg_value'], 
            'min_value': report_doc['min_value'], 
            'max_value': report_doc['max_value'], 
            'date': report_doc['date'].date().isoformat()
        }

        return report_data
    
    def find_by_device_id_and_date_range(self, device_id, from_date, to_date):
        self.latest_error = ''

        report_docs = self._model.find_by_device_id_and_date_range(device_id, from_date, to_date)

        report_data = []

        for doc in report_docs:
            report_single = {
                'device_id': doc['device_id'], 
                'avg_value': doc['avg_value'], 
                'min_value': doc['min_value'], 
                'max_value': doc['max_value'], 
                'date': doc['date'].date().isoformat()
            }

            report_data.append(report_single)

        return report_data

    #####################################################################################
    ############################## NEWLY ADDED!##########################################
    #####################################################################################

    def find_first_anomaly_by_date_range(self, device_ids, threshold, from_date, to_date):
        self.latest_error = ''
        anomaly_doc = self._model.find_first_anomaly_by_date_range(device_ids, threshold, from_date, to_date)

        anomaly_report = {
            'device_id': anomaly_doc['device_id'],
            'anomaly_max': anomaly_doc['max_value'],
            'date': anomaly_doc['date'].date().isoformat()
        }

        return anomaly_report

    def insert(self, device_id, avg_value, min_value, max_value, date):
        self.latest_error = ''

        report_doc = self._model.insert(device_id, avg_value, min_value, max_value, date)

        if report_doc == -1:
            self.latest_error = self._model.latest_error

        return report_doc

    def insert_multiple(self, report_docs):
        self.latest_error = ''

        report_object_ids = self._model.insert_multiple(report_docs)
        return report_object_ids
        
    def create_reports(self):
        self.latest_error = ''

        report_data = self.__aggregate_data_mdb()
        # dr_data = self.__aggregate_data_py()

        self.insert_multiple(report_data)
        
        return True
    
    def __aggregate_data_mdb(self):
        pipeline = [
            {
                '$group': {
                    '_id': {'device_id': '$device_id', 'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$timestamp'}}},
                    'average': {'$avg': '$value'},
                    'min': {'$min': '$value'},
                    'max': {'$max': '$value'},

                }
            }
        ]
        reservoir_data_model = ReservoirDataModel()
        
        report_data = []
        agg_docs = reservoir_data_model.aggregate(pipeline)
        for agg_doc in agg_docs:
            report_doc = {
                'device_id': agg_doc['_id']['device_id'], 
                'avg_value': round(agg_doc['average'], 2), 
                'min_value': agg_doc['min'], 
                'max_value': agg_doc['max'], 
                'date': datetime.datetime.fromisoformat(agg_doc['_id']['date'])
            }

            report_data.append(report_doc)

        return report_data
    
    def __aggregate_data_py(self):
        reservoir_data_model = ReservoirDataModel()
        agg_data = {}
        for data in reservoir_data_model.find_all():
            device_id = data['device_id']
            date = data['timestamp'].date()
            value = data['value']
            
            if (device_id not in agg_data):
                agg_data[device_id] = {}
            if (date not in agg_data[device_id]):
                agg_data[device_id][date] = {'sum': 0, 'count': 0, 'min': math.inf, 'max': -math.inf}
            
            agg_data[device_id][date]['sum'] += value
            agg_data[device_id][date]['count'] += 1
                    
            if (value < agg_data[device_id][date]['min']):
                agg_data[device_id][date]['min'] = value
            
            if (value > agg_data[device_id][date]['max']):
                agg_data[device_id][date]['max'] = value
        
        report_data = []
        for device_id in agg_data:
            for date in agg_data[device_id]:
                report_doc = {
                    'device_id': device_id, 
                    'avg_value': round(agg_data[device_id][date]['sum'] / agg_data[device_id][date]['count'], 2), 
                    'min_value': agg_data[device_id][date]['min'], 
                    'max_value': agg_data[device_id][date]['max'], 
                    'date': datetime.datetime(date.year, date.month, date.day)
                }

                report_data.append(report_doc)
        
        return report_data
