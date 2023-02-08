import configparser
import os
import sys
import pathlib
import boto3
import requests
from tables import Table
import asyncio
config = configparser.ConfigParser()
config.read("dynamoDB.conf")
aws_access_key_id = config['default']['aws_access_key_id']
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


# Check if the tables exist
existing_tables = client.list_tables()
tables = []

for tableName in existing_tables["TableNames"]:
    table = Table(tableName)
    table.loadTable(client_res)
    tables.append(table)

print()
while(True):
    print("Please enter a command")
    print("Commands: create, delete, print, print head, bulkload, exit, add row, delete row")
    print("Current Tables: ", end="")
    for table in tables:
        print(table.get_name(), end=" ")
    print()

    command = input()

    if command[:4] == "exit":
        print("Goodbye!")
        quit()

    elif command[:len("create")] == "create":
        name = input("Table name?" )
        attyA = input("Attribute A? ")
        attyAType = input("Attribute A data type (Number or String)? ")
        attyAType = attyAType.upper()
        attyAType = attyAType[0]
        if attyAType != "N" and attyAType != "S":
            print("Invalid data type")
            continue

        attyB = input("Attribute B? ")
        attyBType = input("Attribute B data type (Number or String)? ")
        attyBType = attyBType.upper()
        attyBType = attyBType[0]
        if attyBType != "N" and attyBType != "S":
            print("Invalid data type")
            continue

        table = Table(name)
        success = table.create(client_res, attyA, attyAType, attyB, attyBType)

        if success:
            tables.append(table)
            print("Table created")

    elif command[:len("bulkload")] == "bulkload":
        name = input("Table name? ")

        nameFound = False
        for table in tables:
            if table.get_name() == tableName:
                nameFound = True
                filename = input("CSV Filename? ")
                table.bulk_load_csv(filename)
                break

        if not nameFound:
            print("Failed to find a table with that name")

    elif command[:len("print head")] == "print head":
        tableName = input("Table name? ")
        nameFound = False
        for table in tables:
            if table.get_name() == tableName:
                nameFound = True
                table.print_head()
                break

        if not nameFound:
            print("Failed to find a table with that name")

    elif command[:len("print")] == "print":
        tableName = input("Table name? ")
        nameFound = False
        for table in tables:
            if table.get_name() == tableName:
                nameFound = True
                table.print_all_rows()
                break

        if not nameFound:
            print("Failed to find a table with that name")

    elif command[:len("add row")] == "add row":
        tableName = input("Table name? ")
        nameFound = False
        for table in tables:
            if table.get_name() == tableName:
                nameFound = True
                columns = table.get_column_headers()
                data = {}

                for column in columns:
                    dat = input("Enter data for column '" + column + "': ")

                    if dat.isdigit():
                        dat = int(dat)
                    else:
                        dat = str(dat)
                    data[column] = dat

                moreColumns = input("Add another column value? (y/n) ")
                while moreColumns == "y":
                    column = input("Column name? ")

                    if column in data.keys():
                        print("Column value already given")
                        continue

                    dat = input("Enter data for column '" + column + "': ")

                    if dat.isdigit():
                        dat = int(dat)
                    else:
                        dat = str(dat)

                    data[column] = dat

                    moreColumns = input("Add another column value? (y/n) ")

                table.add_row(data)
                break

        if not nameFound:
            print("Failed to find a table with that name")


    elif command[:len("delete row")] == "delete row":
        tableName = input("Table name? ")
        nameFound = False
        for table in tables:
            if table.get_name() == tableName:
                nameFound = True
                columns = table.get_column_headers()
                data = []

                for column in columns:
                    dat = input("Enter data for column '" + column + "': ")
                    data.append(dat)

                table.delete_row(data)
                break

        if not nameFound:
            print("Failed to find a table with that name")


    elif command[:len("delete")] == "delete":
        name = input("Table name? ")
        nameFound = False
        for table in tables:
            if table.get_name() == name:
                nameFound = True
                ans = table.delete_self(client)
                if ans:
                    tables.remove(table)
                    print("Table removed")

        if not nameFound:
            print("Failed to find a table with that name")