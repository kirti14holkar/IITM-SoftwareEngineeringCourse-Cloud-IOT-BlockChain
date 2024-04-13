#Connect to EC2 Instance with SSH as below
#ssh -i "IITM-Project5-EC2Key.pem" ubuntu@ec2-13-232-249-110.ap-south-1.compute.amazonaws.com
#Upload the file to EC2 Instance from S3 bucket.
#aws s3 cp s3://iitm-project5-s3/StockPriceIngestion.py
#Run the file
#python3 StockPriceIngestion.py
#Check the terminal for output

import json
import boto3
#import sys
import yfinance as yf
import pandas as pd
#import time
#import random
import datetime
#import awswrangler as wr
#from decimal import Decimal


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

#kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(1)

# Example of pulling the data between 2 dates from yfinance API
#data = yf.download('MSFT', start= yesterday, end= today, interval = '1h' )


# Creating Mast list of Stocks and Empty StockTicker Dataframe
StockList = ['MSFT','MVIS','GOOG','SPOT','INO','OCGN','ABML','RLLCF','JNJ','PSFE']
StockTicker = pd.DataFrame({"Stock":[],"Open":[],"High":[],"Low":[],"Close":[],"Adj Close":[],"Volume":[],"High52w":[],"Low52w":[]})
StockTicker52w = {}

# Get Daily StockTicker for 52 weeks and then take max and min to get 52Week High and Low for each stock.
print("Getting 52 Week High and Low for each stock....\n")
for Stock in StockList:
    data53w = yf.download(tickers = Stock,  # list of tickers
                #start= yesterday,
                #end= today,
                period = "1y",
                prepost = False,
                interval = '1d')
    Stock52w = []
    print("52wHigh : " + str(data53w['Close'].max()) + "      52wLow : " + str(data53w['Close'].min()))
    Stock52w.append(data53w['Close'].max())
    Stock52w.append(data53w['Close'].min())
    StockTicker52w[Stock] = Stock52w


print("Download completed of 52 Week High and Low for each stock....")
print(StockTicker52w)

# Get Hourly StockTicker for last 3 days
print("\nGetting hourly stock data for each stock....")
for Stock in StockList:
    data = yf.download(tickers = Stock,  # list of tickers
                #start= yesterday,
                #end= today,
                period = "1d",
                prepost = False,
                interval = '1h')
    data.insert(loc = 0,column = 'Stock',value = Stock)
    print("Merging 52 week high and low data with Stock hourly data")
    data['High52w'] = StockTicker52w[Stock][0]
    data['Low52w'] = StockTicker52w[Stock][1]
    StockTicker = pd.concat([StockTicker,data],ignore_index = False)
print("\nHourly data for each stock completed and merged with 52 week high and low....")


# Reset Index of StockTicker dataframe.
StockTicker = StockTicker.reset_index()

# Rename Index column name to DateTime.
StockTicker.rename(columns = {'index':'DateTime'}, inplace = True)

# Convert datetime to string
StockTicker['DateTime'] = StockTicker['DateTime'].astype(str)

# Print top 5 records from Stock ticker DataFrame
StockTicker.head(5)

# Create Boto3 session and Kinesis client to push data to Kinesis

print("Creating Boto3 session...\n")
botosession=boto3.Session(region_name="us-east-1",
              aws_access_key_id = 'AKIAZ3TCWWD3ULZDX2HN',
              aws_secret_access_key = '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')

print("Creating kinesis client object session...\n")
kinesis_client = botosession.client('kinesis', region_name='us-east-1')


print("Pushing StockTicker Data merged with 52 week High and Low to Kinesis Stream...please wait...\n")
for index, row in StockTicker.iterrows():
    Record = row.to_json(orient='columns')
    print('Pushing record to Kinesis Stream : ' + str(Record))
    kinesis_client.put_record(
      StreamName="stock_data_stream",
      Data= Record, # put_record expects a string
      PartitionKey=str(hash(json.loads(Record)['Stock'])) # partition key
    )
print("StockTicker Data Push to Kinesis Stream completed !\n")
