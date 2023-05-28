'''
AiCore Multinational Retail Data Centralisation Project
Data cleaning - specific to the Multinational Retail Data Centralisation dataset.

Author: Kristina Gorkovskaya
'''

import pandas as pd
from data_cleaning.data_cleaning_generic import DataCleaningGeneric, standardize_nulls, drop_unwanted_columns, only_clean_nonempty_df
from utilities.decorators import time_it


class DataCleaning(DataCleaningGeneric):
    '''This class cleans the data in a Pandas dataframe.
    It is designed to work with specific datasets, and includes
    methods for cleaning user data, card data, date event data, 
    product data, and store data. 

    Attributes:
        continents (list): valid continents
        country_codes (list): valid country codes
        time_periods (list): valid time periods
    '''

    def __init__(self):
        '''See help(DataCleaning) for accurate signature.'''

        super().__init__()
        self.continents = ['Europe', 'America']
        self.country_codes = ['DE', 'GB', 'US']
        self.time_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
    @drop_unwanted_columns
    def clean_card_data(self, df):
        '''Clean credit card data: standardize card numbers and dates.

        Valid card numbers are expected to have a card_number 
        comprising at least 8 numeric digits; a valid expiry date;
        and a valid date_payment_confirmed not in the future. 
        Identify and drop invalid records.

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                card_number, expiry_date, card_provider,
                date_payment_confirmed

        Return:
            Pandas DataFrame
        '''

        df = self.clean_card_numbers(df, ['card_number'])

        df = self.clean_dates(df, ['expiry_date'], date_format='%m/%y')

        df = self.clean_dates(df, ['date_payment_confirmed'],
                              future_dates_valid=False)

        non_null_columns = ['card_number',
                            'date_payment_confirmed', 'expiry_date']
        df.dropna(subset=non_null_columns, inplace=True)
        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
    @drop_unwanted_columns
    def clean_date_time_data(self, df):
        '''Clean date time data. Identify invalid day, month,
        year, timestamp, and ptime period values; drop 
        invalid records.

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                timestamp, month, year, day, time_period, date_uuid

        Returns:
            Pandas DataFrame
        '''

        numeric_columns = ['month', 'day', 'year']
        df = self.clean_numeric_cols(df, numeric_columns)

        df = self.clean_categories(
            df, ['time_period'], expected_categories=self.time_periods)

        # Use pd.to_datetime to identify and drop records with invalid timestamps;
        # but don't convert the timestamp column to a datetime object, as this
        # will set the YYYY-MM-DD componen to 1901-01-01

        is_valid_timestamp = pd.to_datetime(
            df.timestamp, format='%H:%M:%S', errors='coerce').notnull()

        df = df.loc[is_valid_timestamp, :].dropna()

        # Set day / month / year columns to int;
        # because they contained NaN's, they were stored
        # as float. Now that NaN's have been removed, these
        # columns can be set to int to save space.

        for col in numeric_columns:
            df[col] = df[col].astype(int)
        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
    @drop_unwanted_columns
    def clean_orders_data(self, df):
        ''' Clean orders data. 
        Drop spurious columns. Standardize card number and product
        quantity columns; identify and drop invalid records.

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                level_0, index, date_uuid, first_name, last_name, user_uuid,
                card_number, store_code, product_code, 1, product_quantity

        Return:
            Pandas dataframe
        '''

        df.drop(['first_name', 'last_name'], axis=1, inplace=True)
        df = self.clean_card_numbers(df, ['card_number'])
        df = self.clean_numeric_cols(df, ['product_quantity'])
        df.dropna(axis=1, inplace=True)
        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
    @drop_unwanted_columns
    def clean_products_data(self, df):
        '''Clean product data.

        Convert product weights to kilograms; strip currency symbol 
        from product prices and convert product prices to float; standardize dates; 
        replace invalid categories with NaN, fix typo in removed column.

        Identify and drop invalid records (a product record is expected
        to be 100 % complete).

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                product_name, product_price, weight, category,
                EAN, date_added, uuid, removed, product_code

        Return:
            Pandas DataFrame
        '''

        df = self.convert_product_weights(df, ['weight'])

        df = self.clean_numeric_cols(df, ['product_price'], currency_code='£')

        df = self.clean_dates(df, ['date_added'])

        product_categories = ['diy', 'food-and-drink', 'health-and-beauty',
                              'homeware', 'pets', 'sports-and-leisure',
                              'toys-and-games']
        df = self.clean_categories(df, ['category'], product_categories)

        df['removed'].replace(
            'Still_avaliable', 'Still_available', inplace=True)

        df.dropna(inplace=True)
        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
    @drop_unwanted_columns
    def clean_store_data(self, df):
        '''Clean store data. Standardize dates, country codes,
        continents, numeric columns (latitude, longitude, staff numbers)
        and non-numeric columns (store type, locality).

        Identify and drop invalid records (a store is expected
        to have a valid store code, store_type, and country code)

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                index, address, longitude, lat, latitude, locality,
                store_code, staff_numbers, opening_date, store_type,
                country_code, continent

        Return:
            Pandas DataFrame
        '''

        df = self.clean_dates(df, ['opening_date'])

        df = self.clean_categories(
            df, ['country_code'], expected_categories=self.country_codes)

        # Some continents have been incorrectly entered
        # with an "ee" prefix ("eeAmerica", "eeEurope");
        # fix the typos before calling clean_categories

        df['continent'] = df['continent'].astype(str)
        df['continent'] = df['continent'].str.replace(r'^ee', '', regex=True)
        df['continent'] = df['continent'].str.capitalize()
        df = self.clean_categories(df, ['continent'], self.continents)

        alpha_columns = ['store_type', 'locality']
        df = self.clean_alpha_cols(df, alpha_columns)

        numeric_columns = ['latitude', 'lat', 'longitude', 'staff_numbers']
        df = self.clean_numeric_cols(df, numeric_columns)

        non_null_columns = ['store_code', 'store_type', 'country_code']
        df.dropna(subset=non_null_columns, inplace=True)

        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
    @drop_unwanted_columns
    def clean_user_data(self, df):
        '''Clean user data.

        (1) Clean name and country fields; any records containing
            numerals are treated as invalid and replaced with NaN. 
        (2) Valid users are expected to have a complete first_name 
            and last_name; drop rows without valid names.
        (3) Clean dates, country codes, phone numbers, email addresses.
            Identify invalid values; replace with NaN.

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                first_name, last_name, country, date_of_birth, 
                join_date, phone_number, email_address

        Return:
            Pandas DataFrame
        '''

        alpha_columns = ['first_name', 'last_name', 'country']
        df = self.clean_alpha_cols(df, alpha_columns)

        date_columns = ['date_of_birth', 'join_date']
        df = self.clean_dates(df, date_columns, future_dates_valid=False)

        df['country_code'].replace('GGB', 'GB', inplace=True)
        df = self.clean_categories(
            df, ['country_code'], expected_categories=self.country_codes)

        df = self.clean_phone_numbers(df, ['phone_number'])

        df = self.clean_emails(df, ['email_address'])

        non_null_columns = ['first_name', 'last_name']
        df.dropna(subset=non_null_columns, inplace=True)

        return df
