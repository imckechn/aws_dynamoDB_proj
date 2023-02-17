import configparser
import boto3
from tables import Table
import pandas as pd
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, TableStyle
from reportlab.platypus import Table as Tbl
from reportlab.platypus import Paragraph


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

#Load in area.csv
num = "N"
string = "S"

table1 = Table("area")
table1.create(client_res, client, "ISO3", string, "Country", string)
table1.bulk_load_csv("csvData/area.csv")
print("Area table created")

table2 = Table("population")
table2.create(client_res, client, "Country", string, "Currency", string)
table2.bulk_load_csv("csvData/curpop.csv")
print("Population table created")

table3 = Table("gdppc")
table3.create(client_res, client, "Country", string, "1970", num)
table3.bulk_load_csv("csvData/gdppc.csv")
print("GDP table created")

table4 = Table("languages")
table4.create(client_res, client, "ISO3", string, "Country", string)
table4.bulk_load_csv("csvData/languages.csv")
print("Languages table created")

table5 = Table("unitedNations")
table5.create(client_res, client, "ISO3", string, "Country", string)
table5.bulk_load_csv("csvData/un.csv")
print("United Nations table created")

table6 = Table("capitals")
table6.create(client_res, client, "ISO3", string, "Country", string)
table6.bulk_load_csv("csvData/capitals.csv")
print("Capitals table created")