'''
AiCore Multinational Retail Data Centralisation Project
Data cleaning - generic methods for cleaning dates, weights,
card numbers, etc.

Author: Kristina Gorkovskaya
'''

import numpy as np
import pandas as pd
import re
from datetime import datetime


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
            df[col] = df[col].str.replace(r'@{2,}', '@', regex=True)
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
            currency_code (str or regex): this will be incorporated
            into the regex for identifying valid records.

        Returns:
            Pandas DataFrame
        '''

        for col in columns:
            df[col] = df[col].astype(str).str.strip()

            validation_regex = r'^[0-9\.-]+$'
            if currency_code:
                validation_regex = r'^' + currency_code + r'\s*[0-9\.-]+$'

            is_valid = df[col].str.contains(validation_regex)
            df.loc[~is_valid, col] = np.nan

            if not currency_code:
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
            invalid_phone_no = df[col].str.replace(
                r'\D+', '', regex=True).apply(len) < 7
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
        regex_weight_per_item = r'([0-9\.]+)\s*(?:g|kg|ml|oz)\b'
        regex_unit = r'(?:[0-9\.]+)\s*(g|kg|ml|oz)\b'

        conversions = {'kg': 1, 'g': 1/1000, 'ml': 1/1000, 'oz': 0.0283495}

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
