# This code connects to the running Dynamo instance, creates and loads data into tables.
import boto3


def create_table(table_name):
    table = ddb.create_table(
        KeySchema=[
            {
                'AttributeName': 'Name',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Number',
                'KeyType': 'RANGE'
            },
        ],
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'Name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Number',
                'AttributeType': 'N'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )


# For a boto3 service client.
ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

print("Successfully connected to DynamoDB ...")

TABLE_NAME = "Contacts"
tables = [table.name for table in ddb.tables.all()]

# Create table TABLE_NAME if it doesn't exist already.
if TABLE_NAME not in tables:
    create_table(TABLE_NAME)
    print("Created table %s..." % TABLE_NAME)
else:
    print("Table %s already exists !" % TABLE_NAME)

# Create new item and put it into table.
table = ddb.Table('Contacts')
table.put_item(
   Item={
        'Name': 'Prathmesh',
        'Number': 9876543210,
        'City': 'Delhi',
        'Age': 21
    }
)
print("Put successful!")

# Retrieve an item.
response = table.get_item(
    Key={
        'Name': 'Prashant',
        'Number': 1234567890
    }
)

item = response['Item']
print(item)