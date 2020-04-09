
# This code launches the DynamoDB instance locally.
import os

path = "/Users/prathmesh/Desktop/Lab/DynamoDB/dynamodb_local_latest"

print("Started DynamoDB instance!!")
os.chdir(path)
os.system('java -Djava.library.path=./DynamoDBLocal_lib/ -jar DynamoDBLocal.jar')
