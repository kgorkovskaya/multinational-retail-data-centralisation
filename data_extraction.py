'''
AiCore Multinational Retail Data Centralisation Project
Data extraction

Author: Kristina Gorkovskaya
Date: 2023-05-06
'''

from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import text


class DataExtractor:
    '''This class extracts data from a database into a Pandas dataframe.

    Attributes:
        None
    '''

    @staticmethod
    def read_rds_table(db_connector, table_name):
        '''Read all rows from table into Pandas dataframe.

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
            print(f'Failed to read table {table_name}')
            print(f'{err.__class__.__name__}: {err}')
            return pd.DataFrame()


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    db_connector.init_db_engine()
    df = DataExtractor.read_rds_table(db_connector, 'legacy_users')
    print(df.head())
