'''
AiCore Multinational Retail Data Centralisation Project
Data extraction

Author: Kristina Gorkovskaya
Date: 2023-05-07
'''

import boto3
import io
import pandas as pd
import re
import requests
import tabula
from database_utils import DatabaseConnector
from sqlalchemy import text
from utils import time_it


class DataExtractor:
    '''This class extracts data from a variety of input sources
    into a Pandas dataframe. Input sources:
        - Database tables
        - PDF files
        - API

    Attributes:
        None
    '''

    @staticmethod
    @time_it
    def read_rds_table(db_connector, table_name):
        '''Read all rows from database table into Pandas dataframe.

        Arguments:
            db_connector (DatabaseConnector): contains a SQLAlchemy engine for executing queries
            table_name: string

        Returns:
            Pandas DataFrame
        '''

        try:
            print('Reading database table ' + table_name)
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
    def list_number_of_stores(url, headers):
        '''Retrive number of stores from API.

        Arguments:
            url (str): URL of API endpoint
            headers (dict): headers for GET request

        Returns:
            int: number of stores
        '''

        try:
            print('Getting number of stores from API: ' + url)
            r = requests.get(url, headers=headers)
            assert r.status_code == 200, f'Request failed with status {r.status_code}'
            num_stores = r.json()['number_stores']
            print(f'\t{num_stores:,} stores')
            return num_stores
        except Exception as err:
            print(f'Failed to get number of stores')
            print(f'{err.__class__.__name__}: {err}')
            return 0

    @staticmethod
    @time_it
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
                r = requests.get(url, headers=headers)
                assert r.status_code == 200, f'Request failed with status {r.status_code}'
                store_data.append(r.json())
            df = pd.DataFrame(store_data)
            return df
        except Exception as err:
            print(f'Failed to retrieve store details')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    @time_it
    def extract_from_s3(address):
        '''Download CSV data from S3 bucket; create Pandas DataFrame.

        Read data directly into a DataFrame without downloading the file locally.

        Arguments:
            address (string): full path to file on S3

        Returns:
            Pandas DataFrame
        '''

        try:
            print('Downloading file from S3: ' + address)
            s3_client = boto3.client('s3')
            address_split = re.split(r'/+', address)
            bucket = address_split[1]
            key = address_split[-1]
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            return df
        except Exception as err:
            print(f'Failed to retrieve store details')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()


if __name__ == '__main__':

    '''
    db_connector = DatabaseConnector()
    db_connector.init_db_engine()

    print('\n')
    users = DataExtractor.read_rds_table(db_connector, 'legacy_users')
    print(users.head())

    print('\n')
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_details = DataExtractor.retrieve_pdf_data(url)
    print(card_details.head())
    '''

    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores = DataExtractor.list_number_of_stores(url, headers)

    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_no}'
    store_details = DataExtractor.retrieve_stores_data(
        url, headers, num_stores)
    print(store_details.head())
