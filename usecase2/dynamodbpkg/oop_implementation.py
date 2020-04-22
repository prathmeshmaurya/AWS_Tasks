import boto3
import json
import decimal

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class DynamoOps:
    tables = []

    def __init__(self, is_local=False):
        # To connect with DynamoDB local
        if is_local == False:
            self.ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
        else:
            self.ddb = boto3.resource('dynamodb')
        self.tables = [table.name for table in self.ddb.tables.all()]

    def exists_table(self, tbl_name):
            if tbl_name not in self.tables:
                return False
            else:
                return True

    def create_table(self, tbl_name, pk_name, pk_type, sk_name, sk_type):
        if self.exists_table(tbl_name):
            print("Table %s already exists !" % tbl_name)
        else:
            try:
                response = self.ddb.create_table(
                    KeySchema=[
                        {
                            'AttributeName': pk_name,
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': sk_name,
                            'KeyType': 'RANGE'
                        },
                    ],
                    TableName=tbl_name,
                    AttributeDefinitions=[
                        {
                            'AttributeName': pk_name,
                            'AttributeType': pk_type
                        },
                        {
                            'AttributeName': sk_name,
                            'AttributeType': sk_type
                        },
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                )
                # Wait until the table exists.
                response.meta.client.get_waiter('table_exists').wait(TableName=tbl_name)
                print("Created table %s..." % tbl_name)
                return response

            except ClientError as ce:
                print("Failed to create table ")
                print(ce)

    def add_item(self, tbl_name, item_dict):
        if self.exists_table(tbl_name):
            table = self.ddb.Table(tbl_name)
            try:
                # Replace the pk_name and sk_name accordingly
                ce = 'attribute_not_exists({}) AND attribute_not_exists({})'.format(item_dict['title'], item_dict['year'])
                response = table.put_item(item_dict, ce)
                if response['ResponseMetaData']['HTTPStatusCode'] == 200:
                    print("Put Item successful..")
                else:
                    print("Put Item Failed..")
            except ClientError as ce:
                if ce.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    print("Item already exists..")
                else:
                    print(ce)
        else:
            print("Table does not exist..")

    def put_batch(self, tbl_name, file_name):
        if self.exists_table(tbl_name):
            try:
                table = self.ddb.Table(tbl_name)
                with open(file_name) as json_data:
                    items = json.load(json_data)
                    with table.batch_writer() as batch:
                        for item in items:
                            batch.put_item(Item=item)
                print("Batch write successful..")
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table does not exist..")

    def read_item(self, tbl_name, pk_name, pk_value, sk_name, sk_value):
        if self.exists_table(tbl_name):
            try:
                table = self.ddb.Table(tbl_name)
                response = table.get_item(
                    Key={
                        pk_name: pk_value,
                        sk_name: sk_value
                    }
                )
                item = response['Item']
                print(json.dumps(item, indent=2, cls=DecimalEncoder))
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table does not exist..")

    def update_item(self, tbl_name, pk_name, pk_value, col_dict):
        if self.exists_table(tbl_name):
            try:
                update_expression = 'SET {}'.format(','.join(f'#{k}=:{k}' for k in col_dict))
                expression_attribute_values = {f':{k}': v for k, v in col_dict.items()}
                expression_attribute_names = {f'#{k}': k for k in col_dict}

                table = self.ddb.Table(tbl_name)
                response = table.update_item(
                    Key={
                        pk_name: pk_value
                    },
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_attribute_values,
                    ExpressionAttributeNames=expression_attribute_names,
                    ReturnValues='UPDATED_NEW',
                )
                print("Update Item successful..")
                return response
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table does not exist..")

    def delete_item(self, tbl_name, pk_name, pk_value):
        if self.exists_table(tbl_name):
            try:
                table = self.ddb.Table(tbl_name)
                response = table.delete_item(Key={pk_name: pk_value})
                print("Deleted Item..")
                return response
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table does not exist..")

    def delete_table(self, tbl_name):
        if self.exists_table(tbl_name):
            try:
                table = self.ddb.Table(tbl_name)
                response = table.delete()
                self.tables.remove(tbl_name)
                print("Deleted Table..")
                return response
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table doesn't exist...")

    def scan_table(self, tbl_name, filter_key=None, filter_val=None):
        if self.exists_table(tbl_name):
            try:
                table = self.ddb.Table(tbl_name)
                if filter_key and filter_val:
                    filtering_exp = Key(filter_key).eq(filter_val)
                    response = table.scan(FilterExpression=filtering_exp)
                else:
                    response = table.scan()
                items = response['Items']
                for item in items:
                    print(json.dumps(item, indent=2 ,cls=DecimalEncoder))
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table does not exist..")

    def query_table(self, tbl_name, filter_key=None, filter_val=None):
        if self.exists_table(tbl_name):
            try:
                table = self.ddb.Table(tbl_name)
                if filter_key and filter_val:
                    filtering_exp = Key(filter_key).eq(filter_val)
                    response = table.query(KeyConditionExpression=filtering_exp)
                else:
                    response = table.query()
                items = response['Items']
                for item in items:
                    print(json.dumps(item, indent=2, cls=DecimalEncoder))
            except ClientError as ce:
                print("Exception Occurred : " + ce.response['Error']['Code'])
        else:
            print("Table does not exist..")






