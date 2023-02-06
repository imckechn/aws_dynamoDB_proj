import configparser
import os
import sys
import pathlib
import boto3
import requests
from tables import Table
config = configparser.ConfigParser()
config.read("dynamoDB.conf")
aws_access_key_id = config['default' ]['aws_access_key_id' ]
aws_secret_access_key = config['default']['aws_secret_access_key']

#Try making a connection to S3 using boto3
try:
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    client_res = boto3.resource('dynamodb', region_name='ca-central-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    client = boto3.client('dynamodb', region_name='ca-central-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # dynamo = session.client('dynamodb')
    # dynamo_res = session.resource('dynamodb')
    print("Welcome to the AWS DynamoDB Shell(D5)")
    print("You are now conneced to your DB storage")

except:
    print("Welcome to the AWS DynamoDB Shell(D5)")
    print("You are now conneced to your DB storage")
    print("Error: Please review procedures for authenticating your account on AWS DynamoDB")
    quit()


print("creating a table")
table = Table(client_res, 'test_table')

print("Now deleting the table")
table.delete_self(client)
print("Done")

