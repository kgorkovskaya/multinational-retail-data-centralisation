'''
AiCore Multinational Retail Data Centralisation Project
Extract and clean user data.

Connect to PostgreSQL AWS database; read the legacy_users
table into a Pandas dataframe; write to a new table (dim_users)
on local PostgreSQL database.

Author: Kristina Gorkovskaya
Date: 2023-05-06
'''

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

if __name__ == '__main__':

    # Establish database connections
    db_connector_aws = DatabaseConnector("db_creds.yaml")
    db_connector_aws.init_db_engine()

    db_connector_local = DatabaseConnector("db_creds_sales_data.yaml")
    db_connector_local.init_db_engine()

    # Clean and load user data
    users = DataExtractor.read_rds_table(db_connector_aws, 'legacy_users')
    users = DataCleaning().clean_user_data(users)
    db_connector_local.upload_to_db(users, 'dim_users')
    print(f'Table names: {db_connector_local.list_db_tables()}')

    # Clean and load card data
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_details = DataExtractor.retrieve_pdf_data(url)
    card_details = DataCleaning().clean_card_data(card_details)
    db_connector_local.upload_to_db(card_details, 'dim_card_details')
    print(f'Table names: {db_connector_local.list_db_tables()}')
