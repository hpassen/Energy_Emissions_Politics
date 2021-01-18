'''
Module for wrangling data: cleans and prepares all data sets for analysis 
'''

import numpy as np
import pandas as pd

def load_clean_pop(filepath):
    '''
    Imports and cleans a census estimates dataframe

    Inputs: 
        filepath (str): the string for the filepath

    Returns: 
        pop_df (pandas df): cleaned dataframe of population data
    '''

    df = pd.read_csv(filepath, header=4)

    return df