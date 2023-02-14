# Cis 4010 - Assignment 2

By Ian McKechnie
Written Feb 14, 2023

## Description
Allows the user to upload the CSV files to their AWS DynamoDB database. The user can then query the database for the data they need.
Program then builds two pdfs giving the user details on a country or a specific year.

## Installation
1. Clone the repository
2. Install the requirements.txt file using pip
```python3 -m pip install -r requirements.txt```
4. Put your AWS credentials in a file named dynamoDB.conf in the following format:
```
[default]
aws_access_key_id = XXXXX
aws_secret_access_key = XXXXXX
```
5. Make sure you have a folder called 'pdfs' in the same directory as the program
6. Run the program using ```python3 main.py```

## Usage
To build the pdfs run the following commands:
```
create
area
ISO3
String
Country
String
bulkload
area
csvData/area.csv

create
population
Country
String
Currency
String
bulkload
population
csvData/curpop.csv

create
gdppc
Country
String
1970
Number
bulkload
gdppc
csvData/gdppc.csv

create
languages
ISO3
String
Country
String
bulkload
languages
csvData/languages.csv

create
unitedNations
ISO3
String
Country
String
bulkload
unitedNations
csvData/un.csv

create
capitals
ISO3
String
Country
String
bulkload
capitals
csvData/capitals.csv
```

To Generate PDF A
Type ```pdfA``` and hit enter

Then follow the instructions given on the screen


To Generate PDF B
Type ```pdfB``` and hit enter

Then follow the instructions given on the screen
