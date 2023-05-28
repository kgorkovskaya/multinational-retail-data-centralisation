'''
AiCore Multinational Retail Data Centralisation Project
Extract data from various sources: API, JSON, PDF, RDS, S3

Author: Kristina Gorkovskaya
'''

import boto3
import io
import pandas as pd
import re
import requests
import tabula
from data_ingestion.database_utils import DatabaseConnector
from sqlalchemy import text
from utilities.decorators import print_newline, time_it


class DataExtractor:
    '''This class extracts data from a variety of input sources
    into a Pandas dataframe. Input sources: API, JSON, PDF, RDS, S3

    Attributes:
        None
    '''

    @staticmethod
    @time_it
    @print_newline
    def extract_from_s3(path):
        '''Download CSV data from S3 bucket; create Pandas DataFrame.

        Read data directly into a DataFrame without downloading the file locally.

        Arguments:
            path (string): full path to file on S3

        Returns:
            Pandas DataFrame
        '''

        try:
            print('Downloading file from S3: ' + path)
            s3_client = boto3.client('s3')
            path_split = re.split(r'/+', path)
            bucket = path_split[1]
            key = path_split[-1]
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            return df
        except Exception as err:
            print(f'Failed to retrieve store details')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    @time_it
    @print_newline
    def list_number_of_stores(url, headers=dict()):
        '''Retrive number of stores from API.

        Arguments:
            url (str): URL of API endpoint
            headers (dict): headers for GET request

        Returns:
            int: number of stores
        '''

        try:
            print('Getting number of stores from API: ' + url)
            r = DataExtractor.send_get_request(url, headers)
            num_stores = r.json()['number_stores']
            print(f'\t{num_stores:,} stores')
            return num_stores
        except Exception as err:
            print(f'Failed to get number of stores')
            print(f'{err.__class__.__name__}: {err}')
            return 0

    @staticmethod
    @time_it
    @print_newline
    def read_json(url, headers=dict()):
        '''Read JSON data from URL into Pandas dataframe.

        Arguments:
            url (string): URL of JSON file
            headers (dict): headers for GET request

        Returns:
            Pandas DataFrame
        '''

        try:
            print('Reading JSON data: ' + url)
            r = DataExtractor.send_get_request(url, headers)
            df = pd.DataFrame(r.json())
            return df
        except Exception as err:
            print(f'Failed to get number of stores')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    @time_it
    @print_newline
    def read_rds_table(db_connector, table_name):
        '''Read all rows from database table into Pandas dataframe.

        Arguments:
            db_connector (DatabaseConnector): contains a SQLAlchemy engine for executing queries
            table_name: string

        Returns:
            Pandas DataFrame
        '''

        try:
            db_name = db_connector.engine.url.database
            print(f'Reading data from RDS table: {db_name}.dbo.{table_name}')
            query = f"SELECT * from {table_name};"
            with db_connector.engine.connect() as con:
                df = pd.read_sql_query(sql=text(query), con=con)
                return df

        except Exception as err:
            print(f'Failed to read data')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    @time_it
    @print_newline
    def retrieve_pdf_data(url):
        '''Read PDF file into a Pandas dataframe.

        Arguments:
            url (string): location of PDF file

        Returns:
            Pandas DataFrame
        '''

        try:
            print('Reading PDF data from url: ' + url)
            df = tabula.read_pdf(url, pages='all')
            df = pd.concat(df).reset_index(drop=True)
            return df

        except Exception as err:
            print(f'Failed to read data')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    @time_it
    @print_newline
    def retrieve_stores_data(endpoint, headers, num_stores=1):
        '''Retrieve store details from API.

        Arguments:
            endpoint (str): URL of API endpoint
            headers (dict): headers for GET request
            num_stores (int): number of stores to retrieve

        Returns:
            Pandas DataFrame
        '''

        try:
            print('Retrieving store details from API: ' + endpoint)
            store_data = []
            for store_no in range(num_stores):
                url = endpoint.format(store_no=store_no)
                r = DataExtractor.send_get_request(url, headers)
                store_data.append(r.json())
            df = pd.DataFrame(store_data)
            return df
        except Exception as err:
            print(f'Failed to retrieve store details')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    def send_get_request(url, headers=dict()):
        '''Send HTTP GET request

        Arguments:
            url (string)
            headers (dict): request headers (optional)

        Returns:
            Requests object instance
        '''

        r = requests.get(url, headers=headers)
        assert r.status_code == 200, f'Request failed with status {r.status_code}'
        return r


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    db_connector.init_db_engine()

    print('\n')
    users = DataExtractor.read_rds_table(db_connector, 'legacy_users')
    print(users.head())

    print('\n')
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df = DataExtractor.retrieve_pdf_data(url)
    print(df.head())

    print('\n')
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores = DataExtractor.list_number_of_stores(url, headers)

    print('\n')
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_no}'
    df = DataExtractor.retrieve_stores_data(url, headers, num_stores)
    print(df.head())

    print('\n')
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_no}'
    df = DataExtractor.retrieve_stores_data(url, headers, num_stores)
    print(df.head())

    print('\n')
    s3_address = 's3://data-handling-public/products.csv'
    df = DataExtractor.extract_from_s3(s3_address)
    print(df.head())

    print('\n')
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df = DataExtractor.read_json(url)
    print(df.head())
