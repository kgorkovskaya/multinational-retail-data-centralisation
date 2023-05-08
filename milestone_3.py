'''
AiCore Multinational Retail Data Centralisation Project

Milestone 3: Create the database schema

Develop the star-based schema of the database, ensuring that columns 
have the correct data types.

Author: Kristina Gorkovskaya
Date: 2023-05-08
'''

# %%

import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy import text
from utils import time_it


def get_column_names(engine, table_name):
    '''Get a list of column names in a database table.

    Arguments:
        engine (SQLAlchemy engine)
        table_name (string)

    Returns:
        list of strings
    '''
    with engine.connect() as con:
        sql = f'SELECT * FROM {table_name} WHERE 0 = 1;'
        column_names = list(con.execute(text(sql)).keys())
        print('Column names: ' + ', '.join(column_names))
        return column_names


def get_max_length(engine, column_name, table_name):
    '''Run SQL to get maximum length of value in column.

    Arguments:
        engine (SQLAlchemy engine)
        table_name (string)
        column_name (string)

    Returns:
        int
    '''
    with engine.connect() as con:
        sql = F'SELECT MAX(LENGTH(CAST({column_name} AS TEXT))) from {table_name};'
        cur = con.execute(text(sql))
        return cur.fetchall()[0][0]


def get_data_type(engine, column_name, table_name):
    '''Get the required data type for each column.
    For most VARCHAR columns, the field length is set to
    the maximum length in the field.

    Arguments:
        engine (SQLAlchemy engine)
        table_name (string)
        column_name (string)

    Returns:
        string
    '''
    varchar = 'VARCHAR({})'
    uuid = 'UUID USING {}::uuid'
    data_types = {'card_number': varchar,
                  'continent': varchar,
                  'country_code': varchar,
                  'date_of_birth': 'DATE',
                  'date_uuid': uuid,
                  'first_name': varchar,
                  'join_date': 'DATE',
                  'last_name': varchar,
                  'latitude': 'FLOAT',
                  'locality': varchar,
                  'longitude': 'FLOAT',
                  'opening_date': 'DATE',
                  'product_code': varchar,
                  'product_quantity': 'SMALLINT',
                  'staff_numbers': 'SMALLINT',
                  'store_code': varchar,
                  'store_type': varchar,
                  'user_uuid': uuid,
                  }

    data_type = data_types.get(column_name, varchar)
    if 'VARCHAR' in data_type:
        if column_name in ['first_name', 'last_name', 'locality', 'continent', 'store_type']:
            max_len = 255
        else:
            max_len = get_max_length(engine, column_name, table_name)
        data_type = data_type.format(max_len)
        if column_name == 'store_type':
            data_type += f';\nALTER TABLE {table_name} ALTER COLUMN {column_name} DROP NOT NULL'
        return data_type.format(max_len)
    return data_type.format(column_name)


@time_it
def alter_table(engine, column_name, table_name, data_type):
    '''Alter column type.

    Arguments:
        engine (SQLAlchemy engine)
        table_name (string)
        column_name (string)
        data_type (string)

    Returns:
        None
    '''
    sql = f'ALTER TABLE {table_name} ALTER COLUMN {column} TYPE {data_type};'
    print('Executing SQL: ' + sql)
    with engine.connect() as con:
        con.execute(text(sql))


def update_table(engine, table_name, column_names):
    '''Update tables.
    dim_store_details:
        Merge LAT and LATITUDE columns in DIM_STORE_DETAILS table;
        drop the LAT column. Set null locality for web-based stores to
        'N/A'.
    dim_products:
        Remove currency symbol from product_price column; cast as
        double; derive weight_class from weight.


    Arguments:
        engine (SQLAlchemy engine)
        column_names (list)

    Returns:
        list of column names
    '''

    sql_1 = '''UPDATE dim_store_details
            SET latitude = COALESCE(latitude, lat);
            ALTER TABLE dim_store_details
            DROP COLUMN lat;'''

    sql_2 = '''UPDATE dim_store_details
            SET locality = 'N/A'
            WHERE LOWER(store_type) LIKE 'web%'
            AND locality IS NULL;'''

    sql_3 = '''UPDATE dim_products
            SET product_price = REPLACE(product_price, '£', '');
            ALTER TABLE dim_products
            ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;
            '''

    sql_4 = '''ALTER TABLE dim_products 
            ADD COLUMN weight_class VARCHAR(20) NULL;
            UPDATE dim_products
            SET weight_class = (
                SELECT CASE 
                    WHEN weight < 2 THEN 'Light'
                    WHEN weight < 40 THEN 'Mid_Sized'
                    WHEN weight < 140 THEN 'Heavy'
                    WHEN weight >= 140 THEN 'Truck_Required'
                    ELSE 'UNKNOWN'
                END
            );'''

    if table_name == 'dim_store_details':
        with engine.connect() as con:
            if 'lat' in column_names:
                print('Updating latitude')
                con.execute(text(sql_1))
                column_names.remove('lat')
            print('Updating null addresses')
            con.execute(text(sql_2))

    if table_name == 'dim_products' and 'weight_class' not in column_names:
        with engine.connect() as con:
            if 'product_price' in column_names:
                print('Updating product_price')
                con.execute(text(sql_3))
            if 'weight_class' not in column_names:
                print('Populating weight_class')
                con.execute(text(sql_4))

    return column_names


if __name__ == '__main__':

    db_connector = DatabaseConnector("db_creds_sales_data.yaml")
    db_connector.init_db_engine()
    engine = db_connector.engine

    tables = ['orders_table', 'dim_users', 'dim_store_details']

    for table_name in tables:
        print('\nUpdating data types for table: ' + table_name)

        column_names = get_column_names(engine, table_name)
        column_names = update_table(engine, table_name, column_names)

        for column in column_names:
            data_type = get_data_type(engine, column, table_name)
            alter_table(engine, column, table_name, data_type)
