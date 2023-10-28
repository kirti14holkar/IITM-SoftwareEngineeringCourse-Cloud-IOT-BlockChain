from service import DeviceService, ReservoirDataService, DailyReportService
import datetime

device_service = DeviceService()
reservoir_data_service = ReservoirDataService()
daily_report_service = DailyReportService()

#######################################################################################
# Access path for problem 1
#######################################################################################

# print(Accessing device DS004')

device_doc = device_service.find_by_device_id('DS004')
if (device_doc == -1):
    print(device_service.latest_error, end='\n\n')
else:
    print(device_doc, end='\n\n')


# print('Creating device DC201')

device_doc = device_service.insert('DC201', 'Calcium Sensor', 'Calcium', 'Acme')
if (device_doc == -1):
    print(device_service.latest_error, end='\n\n')
else:
    print(device_doc, end='\n\n')


# print('Read DS001 device data')

reservoir_data_doc = reservoir_data_service.find_by_device_id_and_timestamp('DS001', 
                                                                            datetime.datetime(2021, 12, 2, 13, 30, 0))
if (reservoir_data_doc == -1):
    print(reservoir_data_service.latest_error, end='\n\n')
else:
    print(reservoir_data_doc, end='\n\n')



###############################################################################################
# admin access path for problem 2
###############################################################################################

print('Generate daily reports')
daily_report_service.create_reports()

print('Get daily report for one day')
daily_report = daily_report_service.find_by_device_id_and_date('DS004',
                                                                datetime.datetime(2021, 12, 2))
print(daily_report, end='\n\n')

print('Get daily report for multiple days')
daily_reports = daily_report_service.find_by_device_id_and_date_range('DC004',
                                                                        datetime.datetime(2021, 12, 2),
                                                                        datetime.datetime(2021, 12, 4))
print(daily_reports, end='\n\n')

############################## NEWLY ADDED!##########################################
# admin access path for problem 3
#####################################################################################

salinity_sensors = ['DS001', 'DS002', 'DS003', 'DS004', 'DS005']

calcium_sensors = ['DC001', 'DC002', 'DC003', 'DC004', 'DC005']

first_salinity_anomaly = daily_report_service.find_first_anomaly_by_date_range(salinity_sensors,
                                                                                1050,
                                                                                datetime.datetime(2021, 12, 2),
                                                                                datetime.datetime(2021, 12, 5))
print(first_salinity_anomaly, end='\n\n')

first_calcium_anomaly = daily_report_service.find_first_anomaly_by_date_range(calcium_sensors,
                                                                                80,
                                                                                datetime.datetime(2021, 12, 2),
                                                                                datetime.datetime(2021, 12, 5))
print(first_calcium_anomaly, end='\n\n')