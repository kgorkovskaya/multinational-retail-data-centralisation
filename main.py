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
    db_connector_local.print_db_tables()

    # Clean and load card data
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_details = DataExtractor.retrieve_pdf_data(url)
    card_details = DataCleaning().clean_card_data(card_details)
    db_connector_local.upload_to_db(card_details, 'dim_card_details')
    db_connector_local.print_db_tables()

    # Clean and load store data
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores = DataExtractor.list_number_of_stores(url, headers)

    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_no}'
    store_details = DataExtractor.retrieve_stores_data(
        url, headers, num_stores)
    store_details = DataCleaning().clean_store_data(store_details)
    db_connector_local.upload_to_db(store_details, 'dim_store_details')
    db_connector_local.print_db_tables()
