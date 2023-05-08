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
    data_types = {'date_uuid': uuid,
                  'user_uuid': uuid,
                  'card_number': varchar,
                  'store_code': varchar,
                  'product_code': varchar,
                  'product_quantity': 'SMALLINT',
                  'first_name': varchar,
                  'last_name': varchar,
                  'date_of_birth': 'DATE',
                  'country_code': varchar,
                  'join_date': 'DATE'}

    data_type = data_types.get(column_name, varchar)
    if 'VARCHAR' in data_type:
        if column_name in ['first_name', 'last_name']:
            max_len = 255
        else:
            max_len = get_max_length(engine, column_name, table_name)
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


if __name__ == '__main__':

    db_connector = DatabaseConnector("db_creds_sales_data.yaml")
    db_connector.init_db_engine()
    engine = db_connector.engine

    tables = ['orders_table', 'dim_users']

    for table_name in tables:
        print('\nUpdating data types for table: ' + table_name)
        column_names = get_column_names(engine, table_name)

        for column in column_names:
            data_type = get_data_type(engine, column, table_name)
            alter_table(engine, column, table_name, data_type)
