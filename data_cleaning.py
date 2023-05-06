import numy as np
import pandas as pd


class DataCleaning:

    @staticmethod
    def clean_date_cols(df, columns):
        '''Set invalid birthdates to NaT'''

        for col in columns:
            df[col] = pd.to_datetime(
                df[col], infer_datetime_format=True, errors="coerce")
        return df

    @staticmethod
    def clean_alpha_cols(df, columns):
        '''Some fields (e.g. names, countries) are not expected to contain numerals;
        Treat any values with numerals as invalid and replace with NaN'''

        for col in columns:
            df[col].replace({r'.*[0-9].*': np.nan}, regex=True, inplace=True)
        return df

    @staticmethod
    def clean_country_codes(df, columns, replacements=dict()):
        '''Country code fields are expected to contain alpha values (and possibly spaces) only.
        The replacements parameter is an optional dict of known incorrect country
        codes and their replacements.
        '''

        country_code_replacements = {r'.*[^A-Za-z\s].*': np.nan}
        country_code_replacements.update(replacements)

        for col in columns:
            df[col].replace(country_code_replacements,
                            regex=True, inplace=True)
        return df


    @staticmethod
    df clean_phone_number(df, columns):
       '''Clean phone number.
        
        Keep numerals, parens, x's and plus signs; replace everything else with empty string.
            - Parens indicate an optional area code or prefix
            - Plus sign indicates a country code,
            - "X" indicates an extension.

        A phone number should contain at least 7 digits; anything with less than 7 digits 
        after cleaning is invalid and is therefore replaced with a nan
        '''

        for col in columns:
        
            df[col].replace(r"[^0-9\(\)Xx\+]", "", regex=True, inplace=True)
            df[col] = df[col].astype(str)

            invalid_phone_no = df[col].str.replace(r'\D+', '').apply(len) < 7
            df.loc[invalid_phone_no, col] = np.nan

        return df

    @staticmethod
    def clean_email(df, columns):

        for col in columns:
            df[col] = df[col]].astype(str).str.strip()
            df[col].str.replace(r'@+', '@', inplace=True)
            is_valid_email = df[col].str.contains(r'^[^@]+@[^@]+\.[^@\.]+$')
            df.loc[~is_valid_email, col] = np.nan
        return df

    @staticmethod
    def clean_data(df):
        '''Clean data.

        (1) Deal with "NULL"s; replace the string "NULL" with NaN.
        (2) Start by cleaning name and country fields; any records containing
            numerals are treated as invalid and replaced with NaN. 
        (3) Valid users are expected to have a complete first_name and last_name;
            drop any rows without valid names.
        (4) Clean dates, country codes, phone numbers, email addresses.
            Identify invalid values; replace with NaN.

        Arguments:
            df: Pandas DataFrame

        Return:
            Pandas DataFrame
        '''

        alpha_cols = ['first_name', 'last_name', 'country']
        date_cols = ['date_of_birth', 'join_date']
        country_code_replacements = {'GGB': 'GB'}

        df.replace(r'^NULL$', np.nan, regex=True, inplace=True)

        df = self.clean_alpha_cols(df, alpha_cols)
        df.dropna(subset=['first_name', 'last_name'], inplace=True)

        df = self.clean_date_cols(df, date_cols)
        df = self.clean_country_code(df, ['country_code'], country_code_replacements)
        df = self.clean_phone_number(['phone_number'])
        df = self.clean_email(['email_address'])

        return df



