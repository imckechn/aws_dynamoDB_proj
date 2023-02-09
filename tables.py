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
    async def create(self, dynamo, attributeA, attributeAKeyType, attributeB, attributeBKeyType):
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

            attributes = self.table.attribute_definitions
            self.column_headers = []

            for atty in attributes:
                self.column_headers.append(atty['AttributeName'])

        except Exception as e:
            print("Error: ", e)

    # This function deletes a table from DynamoDB
    # It takes in the following parameters:
    # client: the dynamoDB client
    def delete_self(self, client):
        try:
            client.delete_table(TableName=self.name)
            return True
        except Exception as e:
            print("Error: ", e)
            return False


    # This function bulk loads a csv file into a table
    # It takes in the following parameters:
    # csv_file_name: the name of the csv file
    def bulk_load_csv(self, csv_file_name):
        try:
            records = json.loads(pd.read_csv(csv_file_name).to_json(orient='records'))

            self.column_headers = records[0].keys()

            for record in records:
                items = {}
                for key in self.column_headers:
                    if type(record[key]) == float:
                        items[key] = int(record[key])
                    else:
                        print(key, record[key])
                        items[key] = record[key]

                self.table.put_item(
                    Item = items
                )

            return True
        except Exception as e:
            print("Error: ", e)
            return False


    # Adds an item to the table
    # It takes in the following parameters:
    # item: the item to add which is all the rows in the table, seperated by commas
    def add_row(self, item):
        try:
            # counter = 0
            # table_row = {}
            # for key in self.column_headers:
            #     if item[counter].isdigit():
            #         table_row[key] = int(item[counter])
            #     else:
            #         table_row[key] = item[counter]
            #     counter += 1

            print("Item: ", item)

            self.table.put_item(
                Item = item
            )

            return True
        except Exception as e:
            print("Error: ", e)
            return False

    # Deletes an item fro the table
    # It takes in the following parameters:
    # item: the item to delete which is all the rows in the table, seperated by commas
    def delete_row(self, item):

        print("Item: ", item)

        counter = 0
        table_row = {}
        for key in self.column_headers:
            elem = str(item[counter])
            if elem.isdigit():
                table_row[key] = int(elem)
            else:
                table_row[key] = elem
            counter += 1

        ans = self.table.delete_item(
            Key = table_row
        )

        print(ans)


    def get_name(self):
        return self.name


    def get_column_headers(self):
        return self.column_headers


    def print_all_rows(self):
        rows =  self.table.scan()['Items']

        if not rows:
            print("No rows in table")
            return

        header = rows[0].keys()
        for elem in header:
            print(elem, end=", ")

        print()
        for row in rows:
            for key in header:

                #This try catch is to handle the case where a row has a null value
                try:
                    print(row[key], end=", ")
                except Exception as e:
                    continue
            print()


    def print_head(self):
        print("Printing head")
        rows =  self.table.scan()['Items']

        header = rows[0].keys()
        self.column_headers = []
        for elem in header:
            self.column_headers.append(elem)
            print(elem, end=", ")

        print()
        i = 0
        for row in rows:
            if i == 5:
                return

            for key in header:
                print(row[key], end=", ")
            print()
            i += 1

    def get_table_as_pd_df(self):
        rows =  self.table.scan()['Items']

        if not rows:
            print("No rows in table")
            return

        header = rows[0].keys()
        df = pd.DataFrame(columns=header)

        for row in rows:
            df = df.append(row, ignore_index=True)

        return df

    def update_table_from_pd_df(self, df, row_id, column_name, new_value):

        row = df.iloc[row_id]

        columns = self.table.get_column_headers()
        oldData = []

        for column in columns:
            oldData.append(row[column])

        self.delete_row(oldData)

        newData = {}
        df.at[row_id, column_name] = new_value
        for column in df.columns:
            newData[column] = df.at[row_id, column]

        self.add_row(newData)
        return

        # print("Row id: ", row_id)
        # print("Column name: ", column_name)
        # print("New value: ", new_value)

        # print("A, ", self.column_headers[0])
        # print("iloc, ", df.iloc[row_id])
        # print("B, ", df.iloc[row_id][column_name])
        # print("C, ", df.at[row_id, column_name])

        # # self.table.update_item(
        # #     Key={'pkey': 'asdf12345'},
        # #     AttributeUpdates={
        # #         'status': 'complete',
        # #     },
        # # )
        # self.table.update_item(
        #     Key={column_name: 'Ya mum'},
        #     AttributeUpdates={
        #         'Ya mum': 'ya dad',
        #     },
        # )



        # response = self.table.update_item(
        #     TableName=self.name,
        #     Key={
        #         column_name: new_value,
        #     },
        #     UpdateExpression=f'SET {column_name} = :{new_value}',
        #     ConditionExpression='attribute_not_exists(deletedAt)', # Do not update if deleted
        #     ExpressionAttributeValues={
        #         ':newCountry': "Canada"
        #     },
        #     ReturnValues="UPDATED_NEW"
        # )