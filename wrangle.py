'''
Module for wrangling data: cleans and prepares all data sets for analysis 
'''

import numpy as np
import pandas as pd

CODE = "data/state_codes.csv"
POPS = ["data/pop_90-99.csv", "data/pop_00-10.csv", "data/pop_10-19.csv"]
LEG = "data/leg_90-19.csv"
ENG = ["data/generation_annual.csv", "data/emission_annual.csv"]

PUNCTUATION = "!@#$%^&*."


def load_codes(filename=CODE):
    '''
    Imports and cleans a mapping of state names to two-letter codes

    Inputs: 
        filename (str): the string for the filepath

    Returns: 
        letters (pandas df): cleaned dataframe of state codes data
    '''
    letters = pd.read_csv(filename)
    letters.columns = letters.columns.str.lower()
    letters = letters[["state", "code"]]
    
    for col in letters.columns:
        letters[col] = letters[col].str.lower()

    return letters


def load_clean_pop(filename):
    '''
    Imports and cleans a census estimates dataframe

    Inputs: 
        filename (str): the string for the filepath

    Returns: 
        pop_df (pandas df): cleaned dataframe of population data
    '''
    df = pd.read_csv(filename, header=3, thousands=",")
    df.columns = df.columns.str.lower()
    df = df.dropna()

    keep_cols = [col for col in df.columns if "-" not in col]
    df_yrs = df[keep_cols]

    states_mask = df_yrs.iloc[:, 0].str.startswith(".")
    df_states = df_yrs.loc[states_mask, :]
    df_states.reset_index(drop=True, inplace=True)
    
    if "unnamed" in df_states.columns[0]:
        df_states = df_states.rename(columns={"unnamed: 0": "state"})
    elif "geography" in df_states.columns:
        df_states = df_states.rename(columns={"geography": "state"})

    return df_states


def build_pop(files=POPS):
    '''
    Loads, cleans, and merges all three population data sets

    Inputs: 
        files (lst): list of filepaths for the three data sets (constant)
        codes (str): the filepath to the state codes data

    Returns:
        pop_df (pandas df): a dataframe of population data from 1990-2019
    '''
    letters = load_codes()
    pop_df = load_clean_pop(files[0])

    for filename in files[1:]:
        df = load_clean_pop(filename)
        pop_df = pop_df.merge(df, how="inner", on="state")

    pop_df["state"] = pop_df["state"].str.lower().str.strip(PUNCTUATION)
    pop_df = letters.merge(pop_df, how="inner", on="state")

    drop_cols = [col for col in pop_df.columns if \
                 col != "state" and len(col) > 4]
    pop_df.drop(columns=drop_cols, inplace=True)

    return pop_df


def load_clean_pol(filename=LEG):
    '''
    Loads and cleans a data set with energy data

    Inputs: 
        filename (str): the string for the filepath

    Returns: 
        pol_df (pandas df): cleaned dataframe of power generation data  
    '''
    letters = load_codes()

    df = pd.read_csv(filename)
    df.columns = df.columns.str.lower()

    for col in df.columns:
        df[col] = df[col].str.lower()
        df[col] = df[col].str.strip(PUNCTUATION)
        df[col] = df[col].str.replace("divided", "split")

    pol_df = letters.merge(df, how="inner", on="state")
    pol_df.fillna("unicam", inplace=True)

    return pol_df


def load_clean_eng(filename):
    '''
    Loads and cleans a data set with energy data

    Inputs: 
        filename (str): the string for the filepath

    Returns: 
        eng_df (pandas df): cleaned dataframe of power generation data
    '''
    df = pd.read_csv(filename, thousands=",")
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_", regex=True)
    df.columns = df.columns.str.replace(r"\n", "_", regex=True)
    # df["year"] = pd.to_datetime(df["year"], format="%Y")
    # df["year"] = df["year"].dt.year

    if "generation" in filename:
        for col in df.columns[1:-1]:
            df[col] = df[col].str.lower()
            df[col] = df[col].str.replace(" ", "_")

        df = df.rename(columns={"energy_source": "source", 
                                "generation_(megawatthours)": "gen_mwh"})

        totals_mask = df.loc[:, "type_of_producer"] == "total_electric_power_industry"
        keep_cols = [col for col in df.columns if col != "type_of_producer"]
     
    elif "emission" in filename:
        for col in df.columns[1:-3]:
            df[col] = df[col].str.lower()
            df[col] = df[col].str.replace(" ", "_")

        df = df.rename(columns={"energy_source": "source", 
                                "co2_(metric_tons)": "co2_tons",
                                "so2_(metric_tons)": "so2_tons",
                                "nox_(metric_tons)": "nox_tons"}) 

        totals_mask = df.loc[:, "producer_type"] == "total_electric_power_industry"
        keep_cols = [col for col in df.columns if col != "producer_type"]

    eng_df = df.loc[totals_mask, keep_cols]
    eng_df.reset_index(drop=True, inplace=True)

    return eng_df


def build_eng(files=ENG):
    '''
    Loads, cleans, and merges both energy data sets

    Inputs: 
        files (lst): list of filepaths for the three data sets (constant)
        codes (str): the filepath to the state codes data

    Returns:
        pop_df (pandas df): a dataframe of population data from 1990-2019
    '''
    eng_df = load_clean_eng(files[0])
    
    for filename in files[1:]:
        df = load_clean_eng(filename)
        eng_df = eng_df.merge(df, how="left", on=["state", "year", "source"])

    eng_df.fillna(0, inplace=True) 

    return eng_df


def widen(input_df):
    '''
    Turns a dataframe into a widened pivot table - each row is state, each col 
    is all of the other variables

    Inputs:
        input_df (pandas df): a dataframe to widen

    Returns:
        pivot_df (pandas df): a pivoted dataframe
    '''
    df = input_df.copy()

    df["year_source"] = df["year"] + "_" + df["source"]

    if "gen_mwh" in df.columns:
        wide = df.pivot(index="state", columns="year_source", 
                    values="gen_mwh")   

    if "co2_tons" in df.columns:
        df = df[["state", "year_source", "co2_tons"]]
        wide = df.pivot(index="state", columns="year_source", 
                        values=["co2_tons"])

    wide = wide.reset_index()
    wide.fillna(0, inplace=True)

    return wide


    