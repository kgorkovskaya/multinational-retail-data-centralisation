
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

if __name__ == '__main__':

    db_connector_1 = DatabaseConnector("db_creds.yaml")
    db_connector_1.init_db_engine()
    df = DataExtractor.read_rds_table(db_connector_1, 'legacy_users')
    df = DataCleaning().clean_data(df)

    db_connector_2 = DatabaseConnector("db_creds_sales_data.yaml")
    db_connector_2.init_db_engine()
    db_connector_2.upload_to_db(df, 'dim_users')

    print(f'Table names: {db_connector_2.list_db_tables()}')
