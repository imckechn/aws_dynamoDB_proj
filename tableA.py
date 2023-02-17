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

elements = []
countryName = input("Country name? ")

doc = SimpleDocTemplate("pdfs/" + "TableA.pdf")

header1 = Paragraph("<b>Name of Country</b>")
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
    exit()

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
            densities.append(round(int(populations[i])/int(area), 2))

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
                allDensities.append(round(CountriesAndDensities[country][year], 2))

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