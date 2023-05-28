'''
AiCore Multinational Retail Data Centralisation Project
Data cleaning - decorators for cleaning Pandas DataFrames:
standardize nulls, drop spurious columns, check that DataFrames
are non-empty before attempting to clean them.

Author: Kristina Gorkovskaya
'''

import numpy as np
import pandas as pd


def standardize_nulls(func):
    '''Decorator function for standardizing nulls in a Pandas dataframe.
    Iterate over all arguments; if an argument is a Pandas dataframe,
    replace all occurrences of "NULL" or "N/A" with NaN. Then call
    the wrapped function with the modified arguments.
    '''
    def wrapper(*args, **kwargs):
        for i in range(len(args)):
            if isinstance(args[i], pd.core.frame.DataFrame):
                args[i].replace([r'^NULL$', '^N/A$'], np.nan,
                                regex=True, inplace=True)
        result = func(*args, **kwargs)
        return result
    return wrapper


def drop_unwanted_columns(func):
    '''Decorator function to drop spurious columns in Pandas dataframe.
    Iterate over all arguments; if an argument is a Pandas dataframe,
    drop unwanted columns. Then call the wrapped function with 
    the modified arguments.'''

    def wrapper(*args, **kwargs):

        # Exploratory data analysis indicates that these columns are spurious
        unwanted_columns = ['index', 'Unnamed: 0', 'level_0', '1']
        for i in range(len(args)):
            if isinstance(args[i], pd.core.frame.DataFrame):
                cols = [c for c in args[i].columns if c in unwanted_columns]
                args[i].drop(cols, axis=1, inplace=True)
        result = func(*args, **kwargs)
        return result
    return wrapper


def only_clean_nonempty_df(func):
    '''Decorator function to only process non-empty dataframes;
    DataExtractor methods return empty dataframes if the
    data extraction fails; attempting to clean an empty df
    can result in an error. The wrapper function iterates
    over all the arguments; the wrapped function will only be
    executed if at least one of the arguments is a non-empty
    DataFrame.'''

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
