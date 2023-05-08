'''
AiCore Multinational Retail Data Centralisation Project
Utility functions

Author: Kristina Gorkovskaya
Date: 2023-05-07
'''

import pandas as pd
from time import time


def print_newline(func):
    '''Decorator which prints line break before executing a function'''

    def wrapper(*args, **kwargs):
        print('\n')
        return func(*args, **kwargs)
    return wrapper


def time_it(func):
    '''Function decorator. Times the execution, and prints the number of 
    records returned (if function returns a dataframe)'''
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        if isinstance(result, pd.core.frame.DataFrame):
            print(f'\tRecords: {len(result):,}')
        print(f'\tRun time: {time() - start:.4f} seconds')
        return result
    return wrapper
