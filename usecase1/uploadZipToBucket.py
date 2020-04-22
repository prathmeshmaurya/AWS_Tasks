import boto3
s3 = boto3.resource('s3')
s3.Object('mybucket-1056070', 'function.zip').upload_file(Filename='function.zip')