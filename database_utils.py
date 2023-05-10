'''
AiCore Multinational Retail Data Centralisation Project
Database utilities

Author: Kristina Gorkovskaya
Date: 2023-05-06
'''

from sqlalchemy import create_engine, inspect
import yaml


class DatabaseConnector:
    '''This class connects to a database using SQLAlchemy.

    Attributes:
        engine (Engine): SQLAlchemy Engine, connected to DB
        file_name (str): path to YAML file containing connection details and
            login credentials
    '''

    def __init__(self, file_name='db_creds.yaml'):
        '''See help(DatabaseConnector) for accurate signature.'''

        self.engine = None
        self.file_name = file_name

    def read_db_creds(self):
        '''Read database credentials from YAML file.

        Returns: 
            dict
        '''

        print(f'Loading database credentials from file: {self.file_name}')
        try:
            with open(self.file_name, 'r') as file:
                return yaml.safe_load(file)
        except Exception as err:
            print('Failed to read database credentials')
            print(f'{err.__class__.__name__}: {err}')
            return dict()

    def init_db_engine(self, db_type='postgresql', db_api='psycopg2', autocommit=False):
        '''Initialize a SQLAlchemy Engine object.

        Arguments: 
            db_type (str): database type
            db_api (str): database API
            autocommit (bool): if True, transactions will be auto-commited, to 
                enable durable updates to tables.

        Returns: 
            SQLAlchemy database engine or None.
        '''

        try:
            db_creds = self.read_db_creds()
            user = db_creds['RDS_USER']
            password = db_creds['RDS_PASSWORD']
            host = db_creds['RDS_HOST']
            port = db_creds['RDS_PORT']
            database = db_creds['RDS_DATABASE']

            print(f'Connecting to database: {database}')
            cxn_string = f'{db_type}+{db_api}://{user}:{password}@{host}:{port}/{database}'
            if autocommit:
                self.engine = create_engine(
                    cxn_string, isolation_level='AUTOCOMMIT')
            else:
                self.engine = create_engine(cxn_string)
            return self.engine

        except Exception as err:
            print('Failed to establish database connection')
            print(f'{err.__class__.__name__}: {err}')
            return None

    def list_db_tables(self):
        '''List tables on database.

        Returns: 
            list of table names
        '''

        try:
            inspector = inspect(self.engine)
            table_names = inspector.get_table_names()
            return table_names
        except Exception as err:
            print('Failed to get table names')
            print(f'{err.__class__.__name__}: {err}')
            return []

    def print_db_tables(self):
        '''Pretty-print list of tables on database.
        '''
        print(f'\nTables in {self.engine.url.database}:')
        tables = self.list_db_tables()
        print('\t' + '\n\t'.join(sorted(tables)))

    def upload_to_db(self, df, table_name, if_exists='replace'):
        '''Upload Pandas dataframe to a table in the database.

        Arguments:
            df (Pandas Dataframe): data to upload
            table_name (str): name of table being created
            if_exists (str): how to behave if table exists 
                ('fail', 'replace', or 'append')

        Returns:
            None
        '''

        try:
            db_name = self.engine.url.database
            print(
                f'Writing data to RDS table: {db_name}.dbo.{table_name}')
            msg = 'Incorrect if_exists parameter passed'
            assert if_exists in ['fail', 'replace', 'append'], msg
            df.to_sql(table_name, self.engine, if_exists=if_exists)
        except Exception as err:
            print(f'Failed to upload table {table_name} to database')
            print(f'{err.__class__.__name__}: {err}')


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    engine = db_connector.init_db_engine()
    table_names = db_connector.list_db_tables()
    print(table_names)
