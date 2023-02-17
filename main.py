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
    print("Commands: create, delete, print, print head, bulkload, exit, add row, delete row, update record, tableA, tableB")
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
        print("AttyA,  ", attyA, "  ", attyAType)
        print("AttyB,  ", attyB, "  ", attyBType)
        success = table.create(client, client_res, attyA, attyAType, attyB, attyBType)

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

                    #update the table
                    table.update_table_from_pd_df(df, row, column, new_value)

        if not nameFound:
            print("Failed to find a table with that name")

    elif command[:len("tableA")] == "tableA":

        elements = []
        countryName = input("Country name? ")

        doc = SimpleDocTemplate("pdfs/" + "TableA.pdf")
        header1 = Paragraph("Name of Country")
        header2 = Paragraph(countryName)
        elements.append(header1)
        elements.append(header2)


        #Get the area of the country
        for table in tables:
            if table.get_name() == "area":
                df = table.get_table_as_pd_df()
                area = df.loc[df['Country'] == countryName, "Area"].iloc[0]
                break

        # Get the Official Languages of the country
        try:
            for table in tables:
                if table.get_name() == "capitals":
                    df = table.get_table_as_pd_df()
                    capital = df.loc[df['Country'] == countryName, "Capital"].iloc[0]
                    break
        except:
            print("Country not found, please try again")
            continue

        # Get the Capital of the country
        for table in tables:
            if table.get_name() == "languages":
                df = table.get_table_as_pd_df()
                languages = df.loc[df['Country'] == countryName, "Languages"].iloc[0]
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
            if table.get_name() == "area":
                df = table.get_table_as_pd_df()
                for i in range(len(df)):
                    areas[df.iloc[i]['Country']] = df.iloc[i]['Area']

        for table in tables:
            if table.get_name() == "population":
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
                        else:
                            densities[year] = int(populationsDf[year][i]) / int(areas[populationsDf['Country'][i]])

                    CountriesAndDensities[populationsDf['Country'][i]] = densities

        # --- Build out the table for the population ---
        for table in tables:
            if table.get_name() == "population":
                df = table.get_table_as_pd_df()

                #Add in all the years as columns
                years = df.columns.sort_values()
                for year in years:
                    if not year.isdigit():
                        years = years.drop(year)

                populations = []
                for year in years:
                    value = str(df.loc[df['Country'] == countryName, year].iloc[0])
                    if value != None and value.isdigit():
                        populations.append(int(value))


                #Population density
                densities = []
                for i in range(len(populations)):
                    densities.append(int(populations[i])/int(area))

                #Get the population ranks
                populationRanks = []
                for i in range(len(populations)):
                    populationRanks.append(df[years[i]].rank(ascending=False)[df['Country'] == countryName].iloc[0])

                #Get the density ranks
                densityRanks = []
                places = {}

                years = years.tolist()

                difference = len(years) - len(densities)

                for i in range(len(years) - difference):    #Take into account years that have null values
                    year = years[i]
                    allDensities = [densities[i]]
                    #Add every countries population density for that year
                    for country in CountriesAndDensities.keys():
                        allDensities.append(CountriesAndDensities[country][year])

                    #Sort the densities from largest to smallest, save the position of the given country
                    allDensities.sort(reverse=True)
                    places[year] = allDensities.index(densities[years.index(year)]) + 1

                #Create the table that will be shown to the user
                newDf = pd.DataFrame(columns=['Year', 'Population', 'Population Rank', 'Population Density (ppl/sq km)', 'Density Rank'])

                lenOfValidValues = len(populations)
                newDf['Year'] = years[:lenOfValidValues]
                newDf['Population'] = populations
                newDf['Population Rank'] = populationRanks[:lenOfValidValues]
                newDf['Population Density (ppl/sq km)'] = densities[:lenOfValidValues]
                newDf['Density Rank'] = list(places.values())[:lenOfValidValues]


        #Create headers
        column1Heading = Paragraph("<para align=center>Year</para>")
        column2Heading = Paragraph("<para align=center>Population</para>")
        column3Heading = Paragraph("<para align=center>Population Rank</para>")
        column4Heading = Paragraph("<para align=center>Population Density (ppl/sq km)</para>")
        column5Heading = Paragraph("<para align=center>Density Rank</para>")
        row_array = [column1Heading, column2Heading, column3Heading, column4Heading, column5Heading]
        tableHeading = [row_array]


        t2 = Tbl(tableHeading + newDf.values.tolist())
        t2.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t2)

        #Now build the economics Tbale
        header5 = Paragraph("Economics")
        elements.append(header5)
        currency = ""
        header6 = None

        for table in tables:
            if table.get_name() == "population":
                df = table.get_table_as_pd_df()

                #Get the row with the country name
                row = df.loc[df['Country'] == countryName]
                currency = row['Currency'].iloc[0]
                header6 = Paragraph("Currency: " + currency)
                elements.append(header6)


        header7 = Paragraph("Table of GDP per capita (GDPCC) <from earliest year to latest year> and rank within the world for that year")
        elements.append(header7)

        #Years is already done
        #Get the GDP per capita for each year

        #Create the table that will be shown to the user
        economicsDf = pd.DataFrame(columns=['Year', 'GDPPC', 'Ranks'])
        GDPs = []
        df = None
        gdpRank = []
        for table in tables:
            if table.get_name() == "gdppc":
                df = table.get_table_as_pd_df()

                for i in range(len(years)):
                    year = years[i]
                    GDPs.append(df.loc[df['Country'] == countryName, year].iloc[0])
                    ranks = []
                    for country in df['Country']:
                        ranks.append(df.loc[df['Country'] == country, year].iloc[0])

                    ranks.sort(reverse=True)
                    gdpRank.append( ranks.index(GDPs[i]) + 1 )

                economicsDf['Year'] = years
                economicsDf['GDPPC'] = GDPs
                economicsDf['Ranks'] = gdpRank

        # Create Headers
        column1Heading = Paragraph("<para align=center>Year</para>")
        column2Heading = Paragraph("<para align=center>GDPPC</para>")
        column3Heading = Paragraph("<para align=center>Rank</para>")
        row_array = [column1Heading, column2Heading, column3Heading]
        tableHeading = [row_array]

        t3 = Tbl(tableHeading + economicsDf.values.tolist())
        t3.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t3)

        doc.build(elements)

    elif command[:len("tableB")] == 'tableB':
        elements = []
        doc = SimpleDocTemplate("pdfs/" + "TableB.pdf")

        #Create the header
        requestedYear = input("For which year would you like it for? ")

        elements.append(Paragraph("Global Report"))
        elements.append(Paragraph("Year: " + requestedYear))


        #Get the areas
        areas = None
        capitals = None
        curpop = None
        gdppc = None
        languages = None
        unData = None
        numCountries = 0
        countries = []
        for table in tables:
            if table.get_name() == "population":
                curpop = table.get_table_as_pd_df()
                if len(curpop.index) > numCountries:
                    numCountries = len(curpop.index)
                    countries = curpop['Country'].tolist()

            elif table.get_name() == "gdppc":
                gdppc = table.get_table_as_pd_df()
                if len(gdppc.index) > numCountries:
                    numCountries = len(gdppc.index)
                    countries = gdppc['Country'].tolist()

            elif table.get_name() == "languages":
                languages = table.get_table_as_pd_df()
                if len(languages.index) > numCountries:
                    numCountries = len(languages.index)
                    countries = languages['Country'].tolist()

            elif table.get_name() == "area":
                areas = table.get_table_as_pd_df()
                if len(areas.index) > numCountries:
                    numCountries = len(areas.index)
                    countries = areas['Country'].tolist()

            elif table.get_name() == "capitals":
                capitals = table.get_table_as_pd_df()
                if len(capitals.index) > numCountries:
                    numCountries = len(capitals.index)
                    countries = capitals['Country'].tolist()

            elif table.get_name() == "unitedNations":
                unData = table.get_table_as_pd_df()
                if len(unData.index) > numCountries:
                    numCountries = len(unData.index)
                    countries = unData['Country'].tolist()

        elements.append(Paragraph("Number of Countries: " + str(numCountries)))

        #Sort each table by the requested year
        areas = areas.sort_values(by=['Area'], ascending=False)

        #Get all the countries populations for that year
        populations = {}
        try:
            for country in countries:
                populations[country] = int(curpop.loc[curpop['Country'] == country, requestedYear].iloc[0])
        except:
            print("Invalid year")
            continue

        #sort largest to smallest
        populations = sorted(populations.items(), key=lambda x: x[1], reverse=True)

        populationsTable = []
        for i in range(len(populations)):
            populationsTable.append([populations[i][0], populations[i][1], i + 1])

        #Create the table that will be shown to the user
        # Create Headers
        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>Population</para>")
        column3Heading = Paragraph("<para align=center>Rank</para>")
        row_array = [column1Heading, column2Heading, column3Heading]
        tableHeading = [row_array]
        t1 = Tbl(tableHeading + populationsTable)
        t1.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))
        elements.append(t1)


        #Now rank the countries by area
        areas = areas.sort_values(by=['Area'], ascending=False)

        areasTable = []
        for i in range(len(areas)):
            areasTable.append([areas.iloc[i]['Country'], areas.iloc[i]['Area'], i + 1])

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>Area (sq km)</para>")
        column3Heading = Paragraph("<para align=center>Rank</para>")
        row_array = [column1Heading, column2Heading, column3Heading]
        tableHeading = [row_array]
        t2 = Tbl(tableHeading + areasTable)
        t2.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        header3 = Paragraph("Table of Countries Ranked by Area (Largest to smallest)" + requestedYear)
        elements.append(header3)
        elements.append(t2)

        #Now rank the countries by population density
        populationsDictionary = {}
        for row in populationsTable:
            populationsDictionary[row[0]] = row[1]

        populationDensity = {}
        for country in countries:
            a = int(populationsDictionary[country])
            b = int(areas.loc[areas['Country'] == country]['Area'].iloc[0])
            populationDensity[country] = round(a/b, 2)

        populationDensity = sorted(populationDensity.items(), key=lambda x: x[1], reverse=True)

        densityTable = []
        for i in range(len(populationDensity)):
            densityTable.append([populationDensity[i][0], populationDensity[i][1], i + 1])

        header3 = Paragraph("Table of Countries Ranked by Density (Largest to smallest)")
        elements.append(header3)

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>Density (People / sq km)</para>")
        column3Heading = Paragraph("<para align=center>Rank</para>")
        row_array = [column1Heading, column2Heading, column3Heading]
        tableHeading = [row_array]
        t3 = Tbl(tableHeading + densityTable)
        t3.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t3)
        elements.append(Paragraph("GDP Per Capita for all Countries!"))

        #GDP per capital for each country for each year in the 70s
        columns = gdppc.columns.tolist()
        years = []
        for column in columns:
            if column.isdigit():
                if int(column) < 1980:
                    years.append(column)

        years.sort()
        seventies = pd.DataFrame([['Country'] + years])

        for country in countries:
            gdps = []
            for year in years:
                if int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]) > 0:
                    gdps.append(int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]))
                else:
                    gdps.append("No Data")

            gdps = [country] + gdps

            seventies.loc[len(seventies)] = gdps

        header4 = Paragraph("1970's Table")
        elements.append(header4)

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>1970</para>")
        column3Heading = Paragraph("<para align=center>1971</para>")
        column4Heading = Paragraph("<para align=center>1972</para>")
        column5Heading = Paragraph("<para align=center>1973</para>")
        column6Heading = Paragraph("<para align=center>1974</para>")
        column7Heading = Paragraph("<para align=center>1975</para>")
        column8Heading = Paragraph("<para align=center>1976</para>")
        column9Heading = Paragraph("<para align=center>1977</para>")
        column10Heading = Paragraph("<para align=center>1978</para>")
        column11Heading = Paragraph("<para align=center>1979</para>")
        row_array = [column1Heading, column2Heading, column3Heading, column4Heading, column5Heading, column6Heading, column7Heading, column8Heading, column9Heading, column10Heading, column11Heading]
        tableHeading = [row_array]
        t4 = Tbl(tableHeading + seventies.values.tolist(), colWidths=[100, 50, 50,50, 50, 50,50, 50, 50,50 ])
        t4.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('FONT', (0, 0), (-1, -1), 'Helvetica', 5),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t4)


        #GDP per capital for each country for each year in the 80s
        columns = gdppc.columns.tolist()
        years = []
        for column in columns:
            if column.isdigit():
                if int(column) < 1990 and int(column) > 1979:
                    years.append(column)

        years.sort()
        eighties = pd.DataFrame([['Country'] + years])

        for country in countries:
            gdps = []
            for year in years:
                if int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]) > 0:
                    gdps.append(int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]))
                else:
                    gdps.append("No Data")

            gdps = [country] + gdps

            eighties.loc[len(eighties)] = gdps

        header5 = Paragraph("1980's Table")
        elements.append(header5)

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>1980</para>")
        column3Heading = Paragraph("<para align=center>1981</para>")
        column4Heading = Paragraph("<para align=center>1982</para>")
        column5Heading = Paragraph("<para align=center>1983</para>")
        column6Heading = Paragraph("<para align=center>1984</para>")
        column7Heading = Paragraph("<para align=center>1985</para>")
        column8Heading = Paragraph("<para align=center>1986</para>")
        column9Heading = Paragraph("<para align=center>1987</para>")
        column10Heading = Paragraph("<para align=center>1988</para>")
        column11Heading = Paragraph("<para align=center>1989</para>")
        row_array = [column1Heading, column2Heading, column3Heading, column4Heading, column5Heading, column6Heading, column7Heading, column8Heading, column9Heading, column10Heading, column11Heading]
        tableHeading = [row_array]
        t5 = Tbl(tableHeading + eighties.values.tolist(), colWidths=[100, 50, 50,50, 50, 50,50, 50, 50,50 ])
        t5.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('FONT', (0, 0), (-1, -1), 'Helvetica', 5),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t5)


        #GDP per capital for each country for each year in the 90s
        columns = gdppc.columns.tolist()
        years = []
        for column in columns:
            if column.isdigit():
                if int(column) < 2000 and int(column) > 1989:
                    years.append(column)

        years.sort()
        ninties = pd.DataFrame([['Country'] + years])

        for country in countries:
            gdps = []
            for year in years:
                if int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]) > 0:
                    gdps.append(int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]))
                else:
                    gdps.append("No Data")

            gdps = [country] + gdps
            ninties.loc[len(ninties)] = gdps


        header6 = Paragraph("1990's Table")
        elements.append(header6)

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>1990</para>")
        column3Heading = Paragraph("<para align=center>1991</para>")
        column4Heading = Paragraph("<para align=center>1992</para>")
        column5Heading = Paragraph("<para align=center>1993</para>")
        column6Heading = Paragraph("<para align=center>1994</para>")
        column7Heading = Paragraph("<para align=center>1995</para>")
        column8Heading = Paragraph("<para align=center>1996</para>")
        column9Heading = Paragraph("<para align=center>1997</para>")
        column10Heading = Paragraph("<para align=center>1998</para>")
        column11Heading = Paragraph("<para align=center>1999</para>")
        row_array = [column1Heading, column2Heading, column3Heading, column4Heading, column5Heading, column6Heading, column7Heading, column8Heading, column9Heading, column10Heading, column11Heading]
        tableHeading = [row_array]
        t6 = Tbl(tableHeading + ninties.values.tolist(), colWidths=[100, 50, 50,50, 50, 50,50, 50, 50,50 ])
        t6.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('FONT', (0, 0), (-1, -1), 'Helvetica', 5),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t6)


        #GDP per capital for each country for each year in the 00s
        columns = gdppc.columns.tolist()
        years = []
        for column in columns:
            if column.isdigit():
                if int(column) < 2010 and int(column) > 1999:
                    years.append(column)

        years.sort()
        twoThousands = pd.DataFrame([['Country'] + years])

        for country in countries:
            gdps = []
            for year in years:
                if int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]) > 0:
                    gdps.append(int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]))
                else:
                    gdps.append("No Data")

            gdps = [country] + gdps
            twoThousands.loc[len(twoThousands)] = gdps

        header7 = Paragraph("2000's Table")
        elements.append(header7)

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>2000</para>")
        column3Heading = Paragraph("<para align=center>2001</para>")
        column4Heading = Paragraph("<para align=center>2002</para>")
        column5Heading = Paragraph("<para align=center>2003</para>")
        column6Heading = Paragraph("<para align=center>2004</para>")
        column7Heading = Paragraph("<para align=center>2005</para>")
        column8Heading = Paragraph("<para align=center>2006</para>")
        column9Heading = Paragraph("<para align=center>2007</para>")
        column10Heading = Paragraph("<para align=center>2008</para>")
        column11Heading = Paragraph("<para align=center>2009</para>")
        row_array = [column1Heading, column2Heading, column3Heading, column4Heading, column5Heading, column6Heading, column7Heading, column8Heading, column9Heading, column10Heading, column11Heading]
        tableHeading = [row_array]
        t7 = Tbl(tableHeading + ninties.values.tolist(), colWidths=[100, 50, 50,50, 50, 50,50, 50, 50,50 ])
        t7.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('FONT', (0, 0), (-1, -1), 'Helvetica', 5),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t7)


        #GDP per capital for each country for each year in the 10s
        columns = gdppc.columns.tolist()
        years = []
        for column in columns:
            if column.isdigit():
                if int(column) > 2009:
                    years.append(column)

        years.sort()
        tens = pd.DataFrame([['Country'] + years])

        for country in countries:
            gdps = []
            for year in years:
                if int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]) > 0:
                    gdps.append(int(gdppc.loc[gdppc['Country'] == country][year].iloc[0]))
                else:
                    gdps.append("No Data")

            gdps = [country] + gdps
            tens.loc[len(tens)] = gdps


        header8 = Paragraph("2010's Table")
        elements.append(header8)

        column1Heading = Paragraph("<para align=center>Country Name</para>")
        column2Heading = Paragraph("<para align=center>2010</para>")
        column3Heading = Paragraph("<para align=center>2011</para>")
        column4Heading = Paragraph("<para align=center>2012</para>")
        column5Heading = Paragraph("<para align=center>2013</para>")
        column6Heading = Paragraph("<para align=center>2014</para>")
        column7Heading = Paragraph("<para align=center>2015</para>")
        column8Heading = Paragraph("<para align=center>2016</para>")
        column9Heading = Paragraph("<para align=center>2017</para>")
        column10Heading = Paragraph("<para align=center>2018</para>")
        column11Heading = Paragraph("<para align=center>2019</para>")
        row_array = [column1Heading, column2Heading, column3Heading, column4Heading, column5Heading, column6Heading, column7Heading, column8Heading, column9Heading, column10Heading, column11Heading]
        tableHeading = [row_array]
        t8 = Tbl(tableHeading + ninties.values.tolist(), colWidths=[100, 50, 50,50, 50, 50,50, 50, 50,50 ])
        t8.setStyle(TableStyle([('INNERGRID', (0, 0), (-1, -1), 0.25, (0, 0, 0)),
                                        ('FONT', (0, 0), (-1, -1), 'Helvetica', 5),
                                        ('BOX', (0, 0), (-1, -1), 0.25, (0, 0, 0))]))

        elements.append(t8)

        doc.build(elements)


