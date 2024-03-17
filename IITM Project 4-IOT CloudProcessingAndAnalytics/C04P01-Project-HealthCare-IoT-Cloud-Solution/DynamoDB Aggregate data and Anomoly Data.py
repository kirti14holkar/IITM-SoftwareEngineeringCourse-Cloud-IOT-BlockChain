import boto3
from boto3.dynamodb.conditions import Attr
import os
import requests
import tqdm
import pandas as pd
import json
import datetime
import awswrangler as wr

### Create session on aws with dynamodb resource.
print("creating DynamoDB session and client.")

IoTDeviceLogs = 'bsm_data'
IoTAggregatedLogs = 'bsm_agg_data'
IoTOutliersLogs = 'bsm_alert_data'

botosession=boto3.Session(region_name="us-east-1",
              aws_access_key_id = 'AKIAZ3TCWWD3ULZDX2HN',
              aws_secret_access_key = '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')

dynamo_client  =  botosession.resource(service_name = 'dynamodb')

### getting the table status
product_table = dynamo_client.Table(IoTDeviceLogs)
aggregate_table = dynamo_client.Table(IoTAggregatedLogs)
table_status = product_table.table_status

### Query Dynamodb table by timestamp to get the data for 15 minutes duration

print("Get DynamoDB raw data based on specific time period (15 minutes)")

start_dt = "2024-03-15 00:00:00.000000"
end_dt = "2024-03-15 23:00:00.000000"

rawdata = product_table.scan(Select = "ALL_ATTRIBUTES",
                  FilterExpression = Attr("timestamp").gte(start_dt) & Attr("timestamp").lt(end_dt))
print("Raw data",rawdata)
#Transform DynamoDB table data to Pandas Dataframe.
print("Transform raw data to Pandas dataframe")
devicedf=pd.DataFrame.from_dict(rawdata['Items'])

###Process data to get the aggregated output in dataframe and then upload the dataframe to DynamoDB table.

print("Starting data processing in python for aggregation by minute")
# Deep copy device dataframe to aggregate dataframe..
aggdf = devicedf.copy()

# Trucate seconds from timestamp feature.
aggdf["timestamp"] = pd.to_datetime(aggdf['timestamp'],format='%Y-%m-%d %H:%M:%S.%f').apply(lambda x: x.replace(second=0, microsecond=0))

# Group by deviceid,datatype,timestamp and get mean of value.
print("Aggregating data for all devices using Pandas Group by aggregate function")

aggdf = aggdf.groupby(['deviceid','datatype','timestamp'],as_index=False).agg({'value': ['mean', 'min', 'max','count']})
aggdf.columns = ['deviceid','datatype','timestamp','value_mean', 'value_min', 'value_max', 'value_count']

# Change datatype of each feature of dataframe to string to upload same in DynamoDB
aggdf = aggdf.astype(str)
print("Data aggregation completed.")

# Upload aggregated dataframe to Dynamo DB table
print("Uploading aggregated data to Dynamodb table.")
wr.dynamodb.put_df(df=aggdf,table_name=IoTAggregatedLogs,boto3_session=botosession)
print("Aggregate Data Upload Completed !")

### Anomaly Detection threshold Config
print("Creating Anomly detection threshold config")

anomaly_config = {'HeartRate' : {'avg_min': 65, 'avg_max': 115,'trigger_count': 60},
                  'Temperature' : {'avg_min': 90, 'avg_max': 95,'trigger_count': 4},
                  'SPO2' : {'avg_min': 87, 'avg_max': 95,'trigger_count': 5}}

print('Processing anomly rules for all devices on aggregated data....')

print ('sorting aggregate data to be able iterate through and raise alerts if 5 continuos breach found.')
aggdf.sort_values(by=['deviceid','datatype','timestamp'],ascending=False)
min_cnt = 0
max_cnt = 0
outlierdf = pd.DataFrame({
        "deviceid": [],
        "datatype": [],
        "timestamp": [],
        "value_mean": [],
        "value_min": [],
        "value_max": [],
        "value_count": [],
    })
for index, row in aggdf.iterrows():
    if (float(row["value_min"]) < float(anomaly_config[row["datatype"]]['avg_min'])):
        if(min_cnt < 5):
            min_cnt = min_cnt + 1
        else:
            print('Alert for device_id '+ row["deviceid"] + ' on rule for ' + row["datatype"] + ' and Rule Number 101 starting at '+ row["timestamp"] +' with breach type min at avg_value:' + row["value_min"] )
            olrow = pd.Series(row, index=outlierdf.columns)
            outlierdf = outlierdf.append(olrow)
            min_cnt = 0
    else:
        min_cnt = 0


    if (float(row["value_max"]) > float(anomaly_config[row["datatype"]]['avg_max'])):
        if(max_cnt < 5):
            max_cnt = max_cnt + 1
        else:
            print('Alert for device_id '+ row["deviceid"] + ' on rule for ' + row["datatype"] + ' and Rule Number 102 starting at '+ row["timestamp"] +' with breach type max at avg_value:' + row["value_max"])
            olrow = pd.Series(row, index=outlierdf.columns)
            outlierdf = outlierdf.append(olrow)
            min_cnt = 0
    else:
        max_cnt = 0
print ('Processing anomly rules for all devices on aggregated data....')

print ('sorting aggregate data to be able iterate through and raise alerts if 5 continuos breach found.')
aggdf.sort_values(by=['deviceid','datatype','timestamp'],ascending=False)
min_cnt = 0
max_cnt = 0
outlierdf = pd.DataFrame({
        "deviceid": [],
        "datatype": [],
        "timestamp": [],
        "value_mean": [],
        "value_min": [],
        "value_max": [],
        "value_count": [],
    })
for index, row in aggdf.iterrows():
    if (float(row["value_min"]) < float(anomaly_config[row["datatype"]]['avg_min'])):
        if(min_cnt < 5):
            min_cnt = min_cnt + 1
        else:
            print('Alert for device_id '+ row["deviceid"] + ' on rule for ' + row["datatype"] + ' and Rule Number 101 starting at '+ row["timestamp"] +' with breach type min at avg_value:' + row["value_min"] )
            olrow = pd.Series(row, index=outlierdf.columns)
            outlierdf = outlierdf.append(olrow)
            min_cnt = 0
    else:
        min_cnt = 0


    if (float(row["value_max"]) > float(anomaly_config[row["datatype"]]['avg_max'])):
        if(max_cnt < 5):
            max_cnt = max_cnt + 1
        else:
            print('Alert for device_id '+ row["deviceid"] + ' on rule for ' + row["datatype"] + ' and Rule Number 102 starting at '+ row["timestamp"] +' with breach type max at avg_value:' + row["value_max"])
            olrow = pd.Series(row, index=outlierdf.columns)
            outlierdf = outlierdf.append(olrow)
            min_cnt = 0
    else:
        max_cnt = 0


# Upload outlier data to DynamoDB

print("Preparing upload of outlier data to Dynamodb table...")
wr.dynamodb.put_df(df=outlierdf,table_name=IoTOutliersLogs,boto3_session=botosession)
print("Anomly Data Upload Completed !")
