from sqlalchemy import create_engine, inspect, text
import yaml


class DatabaseConnector:

    def __init__(self, file_name="db_creds.yaml"):
        self.engine = None
        self.file_name = file_name

    def read_db_creds(self):
        '''Reads database credentials from YAML file.

        Returns: 
            dict
        '''

        try:
            with open(self.file_name, "r") as file:
                return yaml.safe_load(file)
        except Exception as err:
            print("Failed to read database credentials")
            print(f"{err.__class__.__name__}: {err}")
            return dict()

    def init_db_engine(self, db_type='postgresql', db_api='psycopg2'):
        '''Initializes a SQLAlchemy database engine.

        Args: 
            db_type: string
            db_api: string

        Returns:
            SQLAlchemy database engine or None.
        '''

        try:
            print('Connecting to database')
            db_creds = self.read_db_creds()
            user = db_creds['RDS_USER']
            password = db_creds['RDS_PASSWORD']
            host = db_creds['RDS_HOST']
            port = db_creds['RDS_PORT']
            database = db_creds['RDS_DATABASE']

            cxn_string = f"{db_type}+{db_api}://{user}:{password}@{host}:{port}/{database}"
            self.engine = create_engine(cxn_string)
            return self.engine

        except Exception as err:
            print("Failed to establish database connection")
            print(f"{err.__class__.__name__}: {err}")
            return None

    def list_db_tables(self):
        '''List tables on database.

        Returns:
            list
        '''
        try:
            inspector = inspect(self.engine)
            table_names = inspector.get_table_names()
            return table_names
        except Exception as err:
            print("Failed to get table names")
            print(f"{err.__class__.__name__}: {err}")
            return []

    def upload_to_db(self, df, table_name):
        '''Upload Pandas dataframe to a table in the database.

        Arguments:
            df: Pandas DataFrame
            table_name: string (name of new table)

        Returns:
            None
        '''

        try:
            print(f'Uploading table {table_name} to database')
            df.to_sql(table_name, self.engine, if_exists="append")
        except Exception as err:
            print(f"Failed to upload table {table_name} to database")
            print(f"{err.__class__.__name__}: {err}")


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    engine = db_connector.init_db_engine()
    table_names = db_connector.list_db_tables()
    print(table_names)
