'''
AiCore Multinational Retail Data Centralisation Project
Data cleaning

Author: Kristina Gorkovskaya
Date: 2023-05-06
'''

import numpy as np
import pandas as pd
from datetime import datetime
from utils import time_it
from warnings import filterwarnings
filterwarnings(
    'ignore', 'The default value of regex will change from True to False in a future version.')


class DataCleaning:
    '''This class cleans the data in a Pandas dataframe.
    Includes methods for cleaning user data, card data, store data; 
    and static methods for cleaning specific types of columns.

    Attributes:
        continents (list): valid continents
        country_codes (list): valid country codes
    '''

    def __init__(self):
        '''See help(DataCleaning) for accurate signature.'''

        self.continents = ['Europe', 'America']
        self.country_codes = ['DE', 'GB', 'US']

    @staticmethod
    def standardize_nulls(func):
        '''Decorator to Standardize nulls in Pandas DataFrame; replace "NULL" with NaN.
        '''
        def wrapper(*args, **kwargs):
            for i in range(len(args)):
                if isinstance(args[i], pd.core.frame.DataFrame):
                    args[i].replace([r'^NULL$', '^N/A$'],
                                    np.nan, regex=True, inplace=True)
            result = func(*args, **kwargs)
            return result
        return wrapper

    @time_it
    @standardize_nulls
    def clean_user_data(self, df):
        '''Clean user data (names, birthdates, contact details).

        (1) Standardize nulls; replace the string "NULL" with NaN.
        (2) Clean name and country fields; any records containing
            numerals are treated as invalid and replaced with NaN. 
        (3) Valid users are expected to have a complete first_name 
            and last_name; drop rows without valid names.
        (4) Clean dates, country codes, phone numbers, email addresses.
            Identify invalid values; replace with NaN.

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                first_name, last_name, country, date_of_birth, 
                join_date, phone_number, email_address

        Return:
            Pandas DataFrame
        '''

        print('Cleaning user data')

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

    @time_it
    @standardize_nulls
    def clean_card_data(self, df):
        '''Clean card data: card numbers and dates.

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

        print('Cleaning card data')

        df = self.clean_card_numbers(df, ['card_number'])

        df = self.clean_dates(df, ['expiry_date'], date_format='%m/%y')

        df = self.clean_dates(df, ['date_payment_confirmed'],
                              future_dates_valid=False)

        non_null_columns = ['card_number',
                            'date_payment_confirmed', 'expiry_date']
        df.dropna(subset=non_null_columns, inplace=True)
        return df

    @time_it
    @standardize_nulls
    def clean_store_data(self, df):
        '''Clean store data.
        Identify and drop invalid records (a store is expected
        to have a store code, store_type, and country code)

        Arguments:
            df (Pandas dataframe): input data for cleaning.
                Expected to contain the following fields:
                index, address, longitude, latitude, locality,
                store_code, staff_numbers, opening_date, store_type,
                country_code, continent

        Return:
            Pandas DataFrame
        '''

        print('Cleaning store details')

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
    def clean_numeric_cols(df, columns):
        '''Some fields are expected to be numeric.
        These might be stored as ints, floats, or strings.
        Identify invalid records (records containing characters
        other than digits, minus signs and decimal points) and set to NaN.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col] = df[col].astype(str).str.strip()
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
