# Implement Query and Scan
import decimal
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from dynamodbpkg import createTable


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


TABLE_NAME = "MoviesR"

# For a boto3 service client.
ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

tables = [table.name for table in ddb.tables.all()]

# Create table TABLE_NAME if it doesn't exist already.
if TABLE_NAME not in tables:
    createTable.create_table(ddb, TABLE_NAME, 'title', 'S', 'year', 'N')
    print("Created table %s..." % TABLE_NAME)
else:
    print("Table %s already exists !" % TABLE_NAME)

# Retrieve item based on Partition key and Sort key
table = ddb.Table(TABLE_NAME)

response = table.get_item(
    Key={
        'title': 'Now You See Me',
        'year': 2013
    }
)
items = response['Item']
for item in items:
    print(json.dumps(item, cls=DecimalEncoder))

# Scan operation to retrieve all movies released after 2010
print("Movies released after 2013")
table = ddb.Table(TABLE_NAME)
response = table.scan(
    FilterExpression=Attr('year').gt(2010)
)
items = response['Items']
for item in items:
    print(json.dumps(item, cls=DecimalEncoder))

# Scan operation to retrieve all movies released after 2013 and rating is greater than 7
print("Movies released after 2010 and have ratings gte 7")
response = table.scan(
    FilterExpression=Attr('year').gt(2010) & Attr('rating').gt(7)
)
items = response['Items']
for item in items:
    print(json.dumps(item, indent=2 ,cls=DecimalEncoder))
