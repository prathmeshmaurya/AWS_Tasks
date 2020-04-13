# This code connects to the running Dynamo instance, creates and loads data into table.
import boto3
import json
from . import createTable


# For a boto3 service client.
ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

print("Connected to DynamoDB ...")

TABLE_NAME = "MoviesR"
tables = [table.name for table in ddb.tables.all()]

# Create table TABLE_NAME if it doesn't exist already.
if TABLE_NAME not in tables:
    createTable.create_table(ddb, TABLE_NAME, 'title', 'S', 'year', 'N')
    print("Created table %s..." % TABLE_NAME)
else:
    print("Table %s already exists !" % TABLE_NAME)


# Batch writing to fill the table using moviedata.json.

table = ddb.Table(TABLE_NAME)
with open('moviesdata.json') as json_data:
    items = json.load(json_data)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

print("Batch write success !")

