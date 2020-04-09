import boto3
import json

ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
TABLE_NAME = "Contacts"

tables = [table.name for table in ddb.tables.all()]

if TABLE_NAME not in tables:
    print("Table does not exist, first create one.")
else:
    table = ddb.Table(TABLE_NAME)
    with open('data.json') as json_data:
        items = json.load(json_data)
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

print("Batch write success !")




