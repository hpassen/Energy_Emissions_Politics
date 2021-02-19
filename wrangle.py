'''
Module for wrangling data: cleans and prepares all data sets for analysis 
'''

import numpy as np
import pandas as pd

CODE = "data/state_codes.csv"
POPS = ["data/pop_90-99.csv", "data/pop_00-10.csv", "data/pop_10-19.csv"]
LEG = "data/leg_90-19.csv"
ENG = ["data/generation_annual.csv", "data/emission_annual.csv"]
GEO = "Data/shapefiles/cb_2019_us_state_500k.shp"

PUNCTUATION = "!@#$%^&*."


# FUNCTIONS TO IMPORT AND CLEAN DATA
def load_states(filename=GEO):
    gdf = gpd.read_file(filename)
    gdf.columns = gdf.columns.str.lower()

    gdf.rename(columns={"stusps": "code", "name": "state"}, inplace=True)
    gdf = gdf[["statefp", "code", "state", "geometry"]]

    gdf['centroid_lon'] = gdf['geometry'].centroid.x
    gdf['centroid_lat'] = gdf['geometry'].centroid.y

    gdf["code"] = gdf["code"].str.upper()

    return gdf


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
    
    letters["code"] = letters["code"].str.upper()

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

    pop_df["state"] = pop_df["state"].str.strip(PUNCTUATION)
    pop_df = letters.merge(pop_df, how="inner", on="state")

    drop_cols = [col for col in pop_df.columns if \
                 col != "state" and len(col) > 4]
    pop_df.drop(columns=drop_cols, inplace=True)

    pop_df = pop_df.melt(id_vars=["state", "code"], 
                         value_vars=[col for col in pop_df.columns if 
                                     col not in ["state", "code"]])
    
    pop_df = pop_df.rename(columns={"variable": "year", "value": "pop"})

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

    for col in [col for col in df.columns if col != "state"]:
        df[col] = df[col].str.strip(PUNCTUATION)
        df[col] = df[col].str.replace("Divided", "Split")

    pol_df = letters.merge(df, how="inner", on="state")
    #Nebraska has a unicameral legislature, so I am including it as split
    pol_df.fillna("Split", inplace=True)

    pol_df = pol_df.melt(id_vars=["state", "code"], 
                         value_vars=[col for col in pol_df.columns if 
                                     col not in ["state", "code"]])
    
    pol_df = pol_df.rename(columns={"variable": "year", "value": "pol"})

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

    if "generation" in filename:
        df = df.rename(columns={"energy_source": "src", 
                                "generation_(megawatthours)": "gen_mwh"})
        
        df["renew"] = np.where((df["src"] == "Coal") | 
                               (df["src"] == "Natural Gas") | 
                               (df["src"] == "Petroleum"), "Nonrenewable", "Renewable")

        totals_mask = df.loc[:, "type_of_producer"] == "Total Electric Power Industry"
        keep_cols = [col for col in df.columns if col != "type_of_producer"]

        df = df.loc[df.loc[:, "src"] != "Total", :]    
     
    elif "emission" in filename:
        df = df.rename(columns={"energy_source": "src", 
                                "co2_(metric_tons)": "co2_tons",
                                "so2_(metric_tons)": "so2_tons",
                                "nox_(metric_tons)": "nox_tons"}) 

        totals_mask = df.loc[:, "producer_type"] == "Total Electric Power Industry"
        keep_cols = [col for col in df.columns if col != "producer_type"]

    eng_df = df.loc[totals_mask, keep_cols]
    eng_df.reset_index(drop=True, inplace=True)

    eng_df["src"] = eng_df["src"].str.replace("Hydroelectric Conventional", 
                                              "Hydroelectric", regex=True)
    eng_df["src"] = eng_df["src"].str.replace("Wood and Wood Derived Fuels", 
                                              "Wood Derived Fuels", regex=True)
    eng_df["src"] = eng_df["src"].str.replace("Solar Thermal and Photovoltaic", 
                                              "Solar", regex=True)
    eng_df["state"] = eng_df["state"].str.upper()

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
        eng_df = eng_df.merge(df, how="left", on=["state", "year", "src"])

    eng_df.fillna(0, inplace=True) 
    eng_df = eng_df.loc[eng_df.loc[:, "state"] != "US-Total", :] 
    eng_df = eng_df.loc[eng_df.loc[:, "state"] != "US-TOTAL", :] 
    eng_df = eng_df.loc[eng_df.loc[:, "state"] != "  ", :] 
    ##remove data from DC bc there's limited data
    eng_df = eng_df.loc[eng_df.loc[:, "state"] != "DC", :] 

    eng_df = eng_df.rename(columns={"state": "code"})

    return eng_df


def build_full():
    '''
    Loads, cleans, and merges all energy data sets

    Inputs: 
        none (defaults in all functions)

    Returns: 
        data (pandas df) a dataframe with all the data
    '''
    eng_df = build_eng()
    pop = build_pop()
    pol = load_clean_pol()

    #Merge 3 data sets together
    data = pop.merge(pol, how="left", on=["state", "code", "year"])
    data["year"] = data["year"].astype(int)

    for state in data["code"].unique():
        state_filter = data["code"] == state
        data.loc[state_filter, "pol"] = data.loc[state_filter, "pol"].fillna(method="ffill")

    data = data.merge(eng_df, how="right", on=["year", "code"])

    #Calculate per person emissions/energy
    data["co2_pp"] = data["co2_tons"] / data["pop"]
    data["mwh_pp"] = data["gen_mwh"] / data["pop"]

    #Calculate % share of energy generation by source
    sum_mwh = data.groupby(["year", "code"])[["gen_mwh", "mwh_pp"]].sum().reset_index()
    sum_mwh.rename(columns={"gen_mwh": "sum_gen_mwh",
                            "mwh_pp": "sum_mwh_pp"}, inplace=True)

    data = data.merge(sum_mwh, how="left", on=["year", "code"])
    data["mwh_pp_pct"] = data["mwh_pp"] / data["sum_mwh_pp"]
    data["gen_mwh_pct"] = data["gen_mwh"] / data["sum_gen_mwh"]

    #Create a "dirtiness ranking" for each source based on how much emissions 
    #it creates per mwh
    mwh_co2 = data.groupby(["src"])[["gen_mwh", "co2_tons"]].sum()
    mwh_co2 = mwh_co2.reset_index()
    mwh_co2["co2_mwh"] = mwh_co2["co2_tons"] / mwh_co2["gen_mwh"]
    mwh_co2.sort_values("co2_mwh", ascending=False, inplace=True)
    mwh_co2.reset_index(drop=True, inplace=True)
    mwh_co2["rank"] = mwh_co2.index + 1

    data = data.merge(mwh_co2[["src", "rank"]], how="left", on="src")
    data["year"] = data["year"].astype(int)

    return data

