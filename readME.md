# Cis 4010 Cloud Computing - Assignment 2
## Moving CSV data to the cloud, updating it, then making pdfs from it

By Ian McKechnie
Written Feb 17, 2023

## Description
Allows the user to upload the CSV files to their AWS DynamoDB database. The user can then query the database for the data they need.
Program then builds two pdfs giving the user details on a country or a specific year.

## Installation
1. Clone the repository
2. Install the requirements.txt file using pip
```python3 -m pip install -r requirements.txt```
4. Put your AWS credentials in a file named dynamoDB.conf in the same directory as the program. The file should look like this:
```
[default]
aws_access_key_id = XXXXX
aws_secret_access_key = XXXXXX
```
5. Make sure you have a folder called 'pdfs' in the same directory as the program


To autoload all the data into the database you can run the following command:

```python3 populateAWS.py```
<br/>
<br/>
If you would like to generate the pdfs you can run the following command:

```python3 tableA.py```

or

```python3 tableB.py```

If you would like to edit the row elements, run

```python3 main.py```

and follow the prompts to edits the rows.
