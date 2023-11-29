# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import numpy as np

# Essentials

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29#Table'
db_name = 'World_Economics.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
table_attribs = pd.DataFrame(columns=["Country", "GDP_USD_millions"])


# Extraction
def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_message = requests.get(url).text
    data = BeautifulSoup(html_message, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {
                    "Country": col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    # Extract and convert GDP values to float
    gdp_values = df["GDP_USD_millions"].str.replace(',', '').astype(float)

    # Convert GDP from millions to billions and round to 2 decimal places
    converted_gdp = np.round(gdp_values / 1000, 2)

    # Update GDP values in the DataFrame
    df["GDP_USD_millions"] = converted_gdp

    # Rename the column to reflect the updated GDP unit
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})

    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()
