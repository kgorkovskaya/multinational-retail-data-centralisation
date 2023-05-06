'''
AiCore Multinational Retail Data Centralisation Project
Data cleaning

Author: Kristina Gorkovskaya
Date: 2023-05-06
'''

import numpy as np
import pandas as pd
from datetime import datetime
from warnings import filterwarnings
filterwarnings(
    'ignore', 'The default value of regex will change from True to False in a future version.')


class DataCleaning:
    '''This class cleans the data in a Pandas dataframe.
    Includes methods for cleaning user data, card data; 
    and static methods for cleaning specific types of columns.

    Attributes:
        None
    '''

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

        alpha_cols = ['first_name', 'last_name', 'country']
        date_cols = ['date_of_birth', 'join_date']

        df = self.standardize_nulls(df)
        df = self.clean_alpha_cols(df, alpha_cols)
        df.dropna(subset=['first_name', 'last_name'], inplace=True)

        df = self.clean_date_cols(df, date_cols, future_dates_valid=False)
        df = self.clean_country_codes(df, ['country_code'], {'GGB': 'GB'})
        df = self.clean_phone_number(df, ['phone_number'])
        df = self.clean_email(df, ['email_address'])

        print(f"Records in clean table: {len(df):,}")
        return df

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

        df = self.standardize_nulls(df)
        df = self.clean_card_number(df, ['card_number'])

        df = self.clean_date_cols(df, ['expiry_date'], date_format='%m/%y')
        df = self.clean_date_cols(df, ['date_payment_confirmed'],
                                  future_dates_valid=False)

        columns = ['card_number', 'date_payment_confirmed', 'expiry_date']
        df.dropna(subset=columns, inplace=True)
        print(f"Records in clean table: {len(df):,}")
        return df

    @staticmethod
    def standardize_nulls(df):
        '''Standardize nulls; replace 'NULL' with NaN.

        Arguments:
            df (Pandas DataFrame)

        Returns:
            Pandas DataFrame

        '''
        df.replace(r'^NULL$', np.nan, regex=True, inplace=True)
        return df

    @staticmethod
    def clean_date_cols(df, columns, date_format=None, future_dates_valid=True):
        '''Set invalid birthdates to NaT.

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
    def clean_country_codes(df, columns, replacements=dict()):
        '''Country code fields are expected to contain alpha characters only.
        The replacements parameter is an optional dict of known incorrect country
        codes and their replacements.

        Arguments:
            df (Pandas DataFrame)
            columns (list): column names for cleaning

        Returns:
            Pandas DataFrame
        '''

        country_code_replacements = {r'.*[^A-Z].*': np.nan}
        country_code_replacements.update(replacements)

        for col in columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col].replace(country_code_replacements,
                            regex=True, inplace=True)
        return df

    @staticmethod
    def clean_phone_number(df, columns):
        '''Clean phone numbers.

        Keep numerals, parentheses, x's and plus signs; replace 
        everything else with empty string. Parentheses, x's and 
        plus signs are likely to be meaningful components of the phone number. 

             - Parens indicate an optional area code or prefix
             - Plus sign indicates a country code,
             - "X" indicates an extension.

        Input data is inconsistently formatted and may be corrupted if 
        components such as country codes and area codes are deleted; therefore 
        a cautious approach is used.

        A phone number is expected to contain at least 7 digits; anything with 
        less than 7 digits after cleaning is invalid and is therefore replaced with NaN.

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
    def clean_email(df, columns):
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
            df[col] = df[col].str.replace(r'@+', '@')
            is_valid_email = df[col].str.contains(r'^[^@]+@[^@]+\.[^@\.]+$')
            df.loc[~is_valid_email, col] = np.nan
        return df

    @staticmethod
    def clean_card_number(df, columns):
        '''Clean credit card numbers.

        Remove non-numeric characters. Credit card numbers are 
        expected to contain > 8 digits (https://en.wikipedia.org/wiki/Payment_card_number);
        identify credit card numbers with less than 8 digits and replace with NaN.

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
