import pandas as pd
import json
import boto3
import asyncio

class Table:

    def __init__(self, name):
        self.name = name

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

    def loadTable(self, dynamo):
        self.table = dynamo.Table(self.name)

    def delete_self(self, client):
        try:
            client.delete_table(TableName=self.name)
        except Exception as e:
            print("Error: ", e)


    def bulk_load_csv(self, csv_file_name):
        records = json.loads(pd.read_csv(csv_file_name).to_json(orient='records'))

        header = records[0].keys()

        for record in records:
            items = {}
            for key in header:
                items[key] = record[key]

            self.table.put_item(
                Item = items
            )