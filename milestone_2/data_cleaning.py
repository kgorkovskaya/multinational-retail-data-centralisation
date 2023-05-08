'''
AiCore Multinational Retail Data Centralisation Project
Data cleaning

Author: Kristina Gorkovskaya
Date: 2023-05-08
'''

import numpy as np
import pandas as pd
import re
from datetime import datetime
from utils import time_it
from warnings import filterwarnings
filterwarnings(
    'ignore', 'The default value of regex will change from True to False in a future version.')


def standardize_nulls(func):
    '''Decorator to Standardize nulls in Pandas DataFrame; replace "NULL" with NaN.
    '''
    def wrapper(*args, **kwargs):
        for i in range(len(args)):
            if isinstance(args[i], pd.core.frame.DataFrame):
                args[i].replace([r'^NULL$', '^N/A$'], np.nan,
                                regex=True, inplace=True)
        result = func(*args, **kwargs)
        return result
    return wrapper


def only_clean_nonempty_df(func):
    '''Decorator to only process non-empty dataframes;
    DataExtractor methods return empty dataframes if the
    data extraction fails; attempting to clean an empty df
    will result in an error.'''

    def wrapper(*args, **kwargs):
        non_empty_df = False
        for i in range(len(args)):
            if isinstance(args[i], pd.core.frame.DataFrame):
                non_empty_df = len(args[i]) > 0
                if non_empty_df:
                    break
        if non_empty_df:
            print('Cleaning data')
            result = func(*args, **kwargs)
            return result
        else:
            print('Cleaning not attempted; method requires a non-empty dataframe')
            return pd.DataFrame()
    return wrapper


class DataCleaningGeneric:
    '''This class contains static methods for cleaning
    specific types of columns (e.g. numeric columns; dates;
    phone numbers; email addresses; etc.). These methods standardize 
    valid data, identify invalid records, and replace invalid
    records with NaN.
    '''

    @staticmethod
    def clean_alpha_cols(df, columns):
        '''Some fields (e.g. names, countries) are not expected to contain numerals;
        Treat any values with numerals as invalid and replace with NaN

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col].replace({r'.*[0-9].*': np.nan}, regex=True, inplace=True)
        return df

    @staticmethod
    def clean_card_numbers(df, columns):
        '''Clean credit card numbers.

        Remove non-numeric characters. Credit card numbers are expected
        to contain > 8 digits (https://en.wikipedia.org/wiki/Payment_card_number);
        identify values with less than 8 digits and replace with NaN.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col] = df[col].replace('[^0-9]+', '', regex=True)
            df[col] = df[col].replace('^[0-9]{,7}$', np.nan, regex=True)
        return df

    @staticmethod
    def clean_categories(df, columns, expected_categories):
        '''Clean columns that are expected to contain categorical
        values (e.g. country codes, continents, etc.) 
        Identify invalid values; replace with NaN.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning
            expected_categories (list): list of valid values

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].astype('category')
            invalid = ~df[col].isin(expected_categories)
            df.loc[invalid, col] = np.nan

        return df

    @staticmethod
    def clean_dates(df, columns, date_format=None, future_dates_valid=True):
        '''Set invalid dates to NaT.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning
            date_format (string or None): format string 
                describing the date format. If not passed,
                format will be inferred.
            future_dates_valid (bool): If False, dates in the
                future will be treated as invalid.

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            if date_format:
                df[col] = pd.to_datetime(
                    df[col], format=date_format, errors='coerce')
            else:
                df[col] = pd.to_datetime(
                    df[col], infer_datetime_format=True, errors='coerce')

            if not future_dates_valid:
                future_dates = df[col] > datetime.now()
                df.loc[future_dates, col] = np.nan

        return df

    @staticmethod
    def clean_emails(df, columns):
        '''Clean email addresses.

        Replace multiple consecutive @ signs with one @.
        ("abc@@def.com" becomes "abc@def.com")

        Replace invalid values with NaN.
        A valid email address is assumed to follow
        the format "string@string.string", and to contain
        only one "@" sign.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r'@{2,}', '@')
            is_valid_email = df[col].str.contains(r'^[^@]+@[^@]+\.[^@\.]+$')
            df.loc[~is_valid_email, col] = np.nan
        return df

    @staticmethod
    def clean_numeric_cols(df, columns, currency_code=None):
        '''Some fields are expected to be numeric.
        These might be stored as ints, floats, or strings.
        Identify invalid records (records containing characters
        other than digits, minus signs and decimal points) and set to NaN.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning
            currency_code (str or regex): this will be stripped
                before searching for non-numeric data.

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col] = df[col].astype(str).str.strip()

            if currency_code:
                df[col] = df[col].str.replace(currency_code, '')

            df[col].replace({r'.*[^0-9\.-].*': np.nan},
                            regex=True, inplace=True)
            df[col] = df[col].astype(float)
        return df

    @staticmethod
    def clean_phone_numbers(df, columns):
        '''Clean phone numbers.

        Keep numerals, parentheses, x's and plus signs; strip 
        all other characters. Parentheses, x's and plus signs are 
        likely to be meaningful components of the phone number. 

        - Parens indicate an optional area code or prefix
        - Plus sign indicates a country code,
        - "X" indicates an extension.

        Input data is inconsistently formatted and may be corrupted if 
        components such as country codes and area codes are deleted; therefore 
        a cautious approach is used.

        A phone number is expected to contain at least 7 digits; anything with 
        less than 7 digits after cleaning is invalid and is therefore replaced 
        with NaN.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col].replace(r"[^0-9\(\)Xx\+]", "", regex=True, inplace=True)
            df[col] = df[col].astype(str)
            invalid_phone_no = df[col].str.replace(r'\D+', '').apply(len) < 7
            df.loc[invalid_phone_no, col] = np.nan
        return df

    @staticmethod
    def convert_product_weights(df, columns):
        '''Parse product weights and convert to kilograms.
        The following valid formats have been observed:
        999ml
        999g
        999kg
        999.99ml
        999.99g
        999.99kg
        99kg .
        10 x 999.99ml
        10 x 999.99g
        10 x 999.99kg

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        regex_multiplier = r'([0-9]+)\s*x\s*[0-9]'
        regex_weight_per_item = r'([0-9\.]+)\s*(?:g|kg|ml)\b'
        regex_unit = r'(?:[0-9\.]+)\s*(g|kg|ml)\b'

        conversions = {'kg': 1, 'g': 1/1000, 'ml': 1/1000}

        for col in columns:

            df['multiplier'] = df[col].str.extract(
                regex_multiplier, re.IGNORECASE)
            df.loc[df['multiplier'].isnull(), 'multiplier'] = 1
            df['multiplier'] = df['multiplier'].astype(int)

            df['weight_per_item'] = df[col].str.extract(
                regex_weight_per_item, re.IGNORECASE).astype(float)

            df['unit'] = df[col].str.extract(regex_unit, re.IGNORECASE)
            df['unit'] = df['unit'].str.lower()

            df[col] = df['weight_per_item'] * df['multiplier']
            df[col] = df[col] * df['unit'].map(conversions)

            df.drop(['multiplier', 'weight_per_item', 'unit'],
                    axis=1, inplace=True)

        return df


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

        unwanted_columns = ['level_0', 'index', 'first_name', 'last_name', '1']
        for column in unwanted_columns:
            if column in df:
                df.drop(column, axis=1, inplace=True)

        df = self.clean_card_numbers(df, ['card_number'])
        df = self.clean_numeric_cols(df, ['product_quantity'])
        df.dropna(axis=1, inplace=True)
        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
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
    def clean_store_data(self, df):
        '''Clean store data. Standardize dates, country codes,
        continents, numeric columns (latitude, longitude, staff numbers)
        and non-numeric columns (store type, locality).

        Identify and drop invalid records (a store is expected
        to have a valid store code, store_type, and country code)

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                index, address, longitude, latitude, locality,
                store_code, staff_numbers, opening_date, store_type,
                country_code, continent

        Return:
            Pandas DataFrame
        '''

        # Input data has duplicate fields: lat and latitude
        # lat is redundant and not populated with valid data;
        # therefore is dropped if more than 50% null

        if 'lat' in df:
            if sum(df['lat'].isnull()) / len(df) > 0.5:
                df.drop('lat', axis=1, inplace=True)

        df = self.clean_dates(df, ['opening_date'])

        df = self.clean_categories(
            df, ['country_code'], expected_categories=self.country_codes)

        # Some continents have been incorrectly entered
        # with an "ee" prefix ("eeAmerica", "eeEurope");
        # fix the typos before calling clean_categories

        df['continent'] = df['continent'].astype(str)
        df['continent'] = df['continent'].str.replace(r'^ee', '')
        df['continent'] = df['continent'].str.capitalize()
        df = self.clean_categories(df, ['continent'], self.continents)

        alpha_columns = ['store_type', 'locality']
        df = self.clean_alpha_cols(df, alpha_columns)

        numeric_columns = ['latitude', 'longitude', 'staff_numbers']
        df = self.clean_numeric_cols(df, numeric_columns)

        non_null_columns = ['address', 'store_type', 'country_code']
        df.dropna(subset=non_null_columns, inplace=True)

        return df

    @only_clean_nonempty_df
    @time_it
    @standardize_nulls
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
