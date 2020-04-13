# Function to create a table.
def create_table(ddb, table_name, partition_key_name, part_key_type, sort_key_name, sort_key_type):
    table = ddb.create_table(
        KeySchema=[
            {
                'AttributeName': partition_key_name,
                'KeyType': 'HASH'
            },
            {
                'AttributeName': sort_key_name,
                'KeyType': 'RANGE'
            },
        ],
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': partition_key_name,
                'AttributeType': part_key_type
            },
            {
                'AttributeName': sort_key_name,
                'AttributeType': sort_key_type
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )