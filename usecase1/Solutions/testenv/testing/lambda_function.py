def lambda_handler(event, context):
    global bucket
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
    print(bucket)
