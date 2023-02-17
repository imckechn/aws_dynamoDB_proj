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
    exit()

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