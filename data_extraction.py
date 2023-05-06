from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import text


class DataExtractor:

    @staticmethod
    def read_rds_table(db_connector, table_name):
        '''Read all rows from table into Pandas dataframe.

        Arguments:
            db_connector: DatabaseConnector
            table_name: string

        Returns:
            Pandas DataFrame
        '''

        try:
            query = f"SELECT * from {table_name};"
            with db_connector.engine.connect() as con:
                df = pd.read_sql_query(sql=text(query), con=con)
                return df

        except Exception as err:
            print(f"Failed to read from table {table_name}")
            print(f"{err.__class__.__name__}: {err}")
            return pd.DataFrame()


if __name__ == '__main__':

    db_connector = DatabaseConnector()
    db_connector.init_db_engine()

    df = DataExtractor.read_rds_table(db_connector, 'legacy_users')
    print(df.shape)
    print(df.head())
