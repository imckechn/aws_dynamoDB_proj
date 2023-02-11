import configparser
import os
import sys
import pathlib
import boto3
import requests
from tables import Table
import pandas as pd
import asyncio
import reportlab
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
    print("Commands: create, delete, print, print head, bulkload, exit, add row, delete row, update record")
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
        tableName = input("Table name? ")

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

    elif command[:len("update record")] == "update record":
        name = input("Table name? ")
        nameFound = False
        for table in tables:
            if table.get_name() == name:
                nameFound = True

                #load the table and turn it into a pandas dataframe
                df = table.get_table_as_pd_df()
                print(df)

                row = input("Enter the row number you want to update: ")
                if row.isdigit():
                    row = int(row)

                else:
                    print("Please give a valid row number (integer)")
                    continue

                if row > len(df.index):
                    print("Please give a valid row number")
                    continue

                print("Row selected = ", df.iloc[row])

                column = input("Enter the column name you want to update: ")

                if column in df.columns:
                    print("Column selected = ", column)
                    print("Current value = ", df.iloc[row][column])
                    new_value = input("Enter the new value: ")

                    if new_value.isdigit():
                        new_value = int(new_value)

                    else:
                        new_value = str(new_value)

                    #df.at[row, column] = new_value
                    print("New value = ", df.iloc[row][column])

                    #update the table
                    table.update_table_from_pd_df(df, row, column, new_value)

        if not nameFound:
            print("Failed to find a table with that name")

    elif command[:len("A")] == "A":
        elements = []
        countryName = input("Country name? ")

        doc = SimpleDocTemplate("pdfs/" + "hello.pdf")
        header1 = Paragraph("Name of Country")
        header2 = Paragraph(countryName)
        elements.append(header1)
        elements.append(header2)


        #Get the area of the country
        for table in tables:
            if table.get_name() == "Area":
                df = table.get_table_as_pd_df()
                area = df.loc[df['Country Name'] == countryName, "Area"].iloc[0]
                break

        # Get the Official Languages of the country
        for table in tables:
            if table.get_name() == "capitals":
                df = table.get_table_as_pd_df()
                capital = df.loc[df['Country Name'] == countryName, "Capital"].iloc[0]
                break

        # Get the Capital of the country
        for table in tables:
            if table.get_name() == "official_languages":
                df = table.get_table_as_pd_df()
                languages = df.loc[df['Country Name'] == countryName, "Languages"].iloc[0]
                break

        title = [["Area: " + str(area) + " sq km"],
                ["Offical/National Languages: " + languages],
                ["Capital City: " + capital]]
        t = Tbl(title)
        t.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t)

        # Getting the Population Table
        header3 = Paragraph("Population")
        header4 = Paragraph("Table of Population, Population Density, and their respective world ranking for that year, ordered by year:")
        elements.append(header3)
        elements.append(header4)


        #Find the population density of each country for each year
        CountriesAndDensities = {}
        areas = {}

        for table in tables:
            if table.get_name() == "Area":
                df = table.get_table_as_pd_df()
                for i in range(len(df)):
                    areas[df.iloc[i]['Country Name']] = df.iloc[i]['Area']

        for table in tables:
            if table.get_name() == "Populations":
                populationsDf = table.get_table_as_pd_df()
                years = list(populationsDf.columns)   #Get all the years
                years.sort()
                years.pop(-1)
                years.pop(-1)

                for i in range(len(populationsDf.index)):   #Loop through each country
                    densities = {}
                    for year in years:  #Loop through each year
                        a = populationsDf[year][i]
                        b = areas[populationsDf['Country'][i]]

                        if a == None or b == None:
                            densities[year] = 0
                            #densities.append(0)
                        else:
                            densities[year] = int(populationsDf[year][i]) / int(areas[populationsDf['Country'][i]])
                            #densities.append( int(populationsDf[year][i]) / int(areas[populationsDf['Country'][i]]))

                    CountriesAndDensities[populationsDf['Country'][i]] = densities


        # --- Build out the table for the population ---
        for table in tables:
            if table.get_name() == "Population":
                df = table.get_table_as_pd_df()

                newDf = pd.DataFrame(columns=['Year', 'Population', 'Population Rank', 'Population Density (ppl/sq km)', 'Density Rank'])

                #Add in all the years as columns
                years = df.columns[1:]
                populations = []
                for year in years:
                    populations.append(df.loc[df['Country Name'] == countryName, year].iloc[0])


                #Population density
                densities = []
                for i in range(len(populations)):
                    densities.append(populations[i]/area)

                #Get the population ranks
                populationRanks = []
                for i in range(len(populations)):
                    populationRanks.append(df[years[i]].rank(ascending=False)[df['Country Name'] == countryName].iloc[0])

                #Get the density ranks
                densityRanks = []
                for i in range(len(densities)):
                    densityRanks.append(df[years[i]].rank(ascending=False)[df['Country Name'] == countryName].iloc[0])




        doc.build(elements)