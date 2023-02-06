class Table:
    def __init__(self, dynamo, name, attributeA, attributeAKeyType, attributeB, attributeBKeyType):
        self.name = name
        #self.table = dynamo.Table(name)
        self.table = dynamo.create_table(
            TableName=name,
            KeySchema=[
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
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': attributeB,
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("Table Status: ", self.table.table_status, self.table.table_name)

    def delete_self(self, client):
        client.delete_table(TableName=self.name)

    def bulk_load_csv(self, csv_file):
        header = True
        with open(csv_file, 'r') as f:
            data = f.read()

        #if header:

        #self.table.put_item(Item=data)