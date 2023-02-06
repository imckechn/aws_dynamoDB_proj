import pandas as pd
import json
import boto3
import asyncio

class Table:

    # The constructor
    # Just takes in the table name
    def __init__(self, name):
        self.name = name

    # This function creates a table
    # It takes in the following parameters:
    # dynamo: the dynamoDB client
    # attributeA: the first attribute
    # attributeAKeyType: the key type of the first attribute
    # attributeB: the second attribute
    # attributeBKeyType: the key type of the second attribute
    def create(self, dynamo, attributeA, attributeAKeyType, attributeB, attributeBKeyType):
        #self.table = dynamo.Table(name)
        try:
            self.table = dynamo.create_table(
                TableName = self.name,
                KeySchema = [
                    {
                        'AttributeName': attributeA,
                        'KeyType': "HASH"
                    },
                    {
                        'AttributeName': attributeB,
                        'KeyType': "RANGE"
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': attributeA,
                        'AttributeType': attributeAKeyType
                    },
                    {
                        'AttributeName': attributeB,
                        'AttributeType': attributeBKeyType
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
            print("Table Status: ", self.table.table_status, self.table.table_name)
        except Exception as e:
            print("Error: ", e)


    # This function loads a table from DynamoDB
    # It takes in the following parameters:
    # dynamo: the dynamoDB client
    def loadTable(self, dynamo):
        try:
            self.table = dynamo.Table(self.name)
            attrs = self.table.attribute_definitions
            print("atters: ", attrs)
        except Exception as e:
            print("Error: ", e)

    # This function deletes a table from DynamoDB
    # It takes in the following parameters:
    # client: the dynamoDB client
    def delete_self(self, client):
        try:
            client.delete_table(TableName=self.name)
        except Exception as e:
            print("Error: ", e)


    # This function bulk loads a csv file into a table
    # It takes in the following parameters:
    # csv_file_name: the name of the csv file
    def bulk_load_csv(self, csv_file_name):
        records = json.loads(pd.read_csv(csv_file_name).to_json(orient='records'))

        self.header = records[0].keys()

        for record in records:
            items = {}
            for key in self.header:
                items[key] = record[key]

            self.table.put_item(
                Item = items
            )

    def add_item(self, item):
        item.split(",")

        self.table.put_item(
            Item = item
        )