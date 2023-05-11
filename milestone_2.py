'''
AiCore Multinational Retail Data Centralisation Project

Milestone 2: Extract and clean source data

Extract data from various sources; load each dataset into a Pandas Dataframe; 
standardize; identify and remove invalid records; load each dataset to a table 
on a local PostgreSQL database.

Author: Kristina Gorkovskaya
Date: 2023-05-08
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

    # Clean and load users
    df = DataExtractor.read_rds_table(db_connector_aws, 'legacy_users')
    df = DataCleaning().clean_user_data(df)
    db_connector_local.upload_to_db(df, 'dim_users')

    # Clean and load card data
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df = DataExtractor.retrieve_pdf_data(url)
    df = DataCleaning().clean_card_data(df)
    db_connector_local.upload_to_db(df, 'dim_card_details')

    # Clean and load stores
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores = DataExtractor.list_number_of_stores(url, headers)

    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_no}'
    df = DataExtractor.retrieve_stores_data(url, headers, num_stores)
    df = DataCleaning().clean_store_data(df)
    db_connector_local.upload_to_db(df, 'dim_store_details')

    # Clean and load products
    s3_address = 's3://data-handling-public/products.csv'
    df = DataExtractor.extract_from_s3(s3_address)
    df.to_excel('products.xlsx', freeze_panes=(1, 0))
    df = DataCleaning().clean_products_data(df)
    db_connector_local.upload_to_db(df, 'dim_products')

    # Clean and load orders
    tables = db_connector_aws.list_db_tables()
    orders_table = list(filter(lambda x: 'order' in x.lower(), tables))[0]
    df = DataExtractor.read_rds_table(db_connector_aws, orders_table)
    df = DataCleaning().clean_orders_data(df)
    db_connector_local.upload_to_db(df, 'orders_table')

    # Clean and load date events
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df = DataExtractor.read_json(url)
    df = DataCleaning().clean_date_time_data(df)
    db_connector_local.upload_to_db(df, 'dim_date_times')

    # Display table names on local DB
    db_connector_local.print_db_tables()
