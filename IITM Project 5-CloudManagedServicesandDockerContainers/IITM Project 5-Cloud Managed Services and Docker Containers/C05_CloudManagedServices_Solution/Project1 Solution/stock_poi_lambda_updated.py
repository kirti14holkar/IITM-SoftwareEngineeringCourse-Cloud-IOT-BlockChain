import json
import base64
import boto3
from decimal import Decimal


def lambda_handler(event, context):
    # TODO implement
    # print(event)

    # Create boto3 SNS Client object.
    sns = boto3.client('sns')

    # Crete boto3 DynamoDB client object.
    dynamodb = boto3.resource('dynamodb')

    # DynamoDB table name
    table = dynamodb.Table('stock_poi_data')

    def raise_alert(Stock, DateTime, Close, High52w, Low52w, Alert):
        # inserting values into DynamoDB table
        # DateT = str(datetime.datetime.fromtimestamp(int(DateT)/1000))
        response = table.put_item(
            Item={"Stock": Stock, "DateTime": str(DateTime), "Close": Decimal(str(Close)),
                  "High52w": Decimal(str(High52w)), "Low52w": Decimal(str(Low52w)), "AlertType": Alert}
        )

        # publish message to SNS
        msg = "Stock price alert !" + Alert + " | Stock : " + Stock + " | DateTime : " + str(
            DateTime) + " | Close : " + str(Close) + " | High52w : " + str(High52w) + " | Low52w : " + str(Low52w)
        response = sns.publish(TopicArn='arn:aws:sns:ap-south-1:067657029978:IITM-Cloud-Project5-Notifications',
                               Message=msg)
        print(Alert + " Alert Raised !" + msg)

    if (len(event['Records']) > 0):
        i = 0
        while i < len(event['Records']):
            Record = json.loads(base64.b64decode(event['Records'][i]['kinesis']['data']))
            i += 1
            # print(Record)
            if Record["Close"] >= (Record["High52w"] * 80 / 100):
                raise_alert(Record["Stock"], Record["DateTime"], Record["Close"], Record["High52w"], Record["Low52w"],
                            "HIGH")
            if Record["Close"] <= (Record["Low52w"] * 120 / 100):
                raise_alert(Record["Stock"], Record["DateTime"], Record["Close"], Record["High52w"], Record["Low52w"],
                            "LOW")

    return {'statusCode': 200, 'context': "Kinesis Stream processing completed."}

