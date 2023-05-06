'''
AiCore Multinational Retail Data Centralisation Project
Data extraction

Author: Kristina Gorkovskaya
Date: 2023-05-06
'''


import pandas as pd
import tabula
from database_utils import DatabaseConnector
from sqlalchemy import text


class DataExtractor:
    '''This class extracts data into a Pandas dataframe.
    Input sources:
        - Database tables
        - PDF files

    Attributes:
        None
    '''

    @staticmethod
    def read_rds_table(db_connector, table_name):
        '''Read all rows from database table into Pandas dataframe.

        Arguments:
            db_connector (DatabaseConnector): contains a SQLAlchemy engine for executing queries
            table_name: string

        Returns:
            Pandas DataFrame
        '''

        print(f'Reading table {table_name}')
        try:
            query = f"SELECT * from {table_name};"
            with db_connector.engine.connect() as con:
                df = pd.read_sql_query(sql=text(query), con=con)
                print(f'Records loaded: {len(df):,}')
                return df

        except Exception as err:
            print(f'Failed to read data')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()

    @staticmethod
    def retrieve_pdf_data(url):
        '''Read PDF file into a Pandas dataframe.

        Arguments:
            url (string): location of PDF file

        Returns:
            Pandas DataFrame
        '''

        print(f'Reading PDF data from url: {url}')
        try:
            df = tabula.read_pdf(url, pages='all')
            df = pd.concat(df)
            print(f'Records loaded: {len(df):,}')
            return df

        except Exception as err:
            print(f'Failed to read data')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    db_connector.init_db_engine()

    print('\n')
    df = DataExtractor.read_rds_table(db_connector, 'legacy_users')
    print(df.head())

    print('\n')
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df = DataExtractor.retrieve_pdf_data(url)
    print(df.head())
