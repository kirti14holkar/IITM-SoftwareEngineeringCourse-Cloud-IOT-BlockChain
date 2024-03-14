import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


class Database:
	# Initialization, mapping each instance to a specific table, and storing the partition and sort key names
	def __init__(self, table_name, partition_key, sort_key):
		self._dynamodb = boto3.resource('dynamodb')
		self._table = self._dynamodb.Table(table_name)
		self._partition_key = partition_key
		self._sort_key = sort_key

	# To get exact single data based on partition and sort attribute values
	def query_exact(self, partition_value, sort_value):
		try:
			response = self._table.get_item(
										Key={
											self._partition_key: partition_value,
											self._sort_key: sort_value
										}
									)
			item = response['Item']
			return item
		except ClientError as error:
			error_code = error.response['Error']['Code']
			error_message = error.response['Error']['Message']
			print(f"ERROR: In query_exact for table {self._table} - Code: {error_code}, Message: {error_message}")
			return None

	# To get a continuous range of data based on partition attribute value and sort attribute range
	def query_range(self, partition_value, sort_start_value, sort_end_value):
		try:
			response = self._table.query(
											KeyConditionExpression=
												Key(self._partition_key).eq(partition_value) & 
												Key(self._sort_key).between(sort_start_value, sort_end_value)
										)
			items = response['Items']
			return items
		except ClientError as error:
			error_code = error.response['Error']['Code']
			error_message = error.response['Error']['Message']
			print(f"ERROR: In query_range for table {self._table} - Code: {error_code}, Message: {error_message}")
			return None

	# Inserts (or overwrites) a single item based on already formed item dict
	def put_item(self, item):
		try:
			response = self._table.put_item(Item=item)
			return response
		except ClientError as error:
			error_code = error.response['Error']['Code']
			error_message = error.response['Error']['Message']
			print(f"ERROR: In put_item for table {self._table} - Code: {error_code}, Message: {error_message}")
			return None



