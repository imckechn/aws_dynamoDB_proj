class Table:
    def __init__(self, dynamo, name):
        self.name = name
        self.table = dynamo.Table(name)
        print("Table created")
        print(self.table)

    def delete_self(self, client):
        client.delete_table(TableName=self.name)

    def bulk_load_csv(self, csv_file):
        header = True
        with open(csv_file, 'r') as f:
            data = f.read()

        if header:
            
        self.table.put_item(Item=data)