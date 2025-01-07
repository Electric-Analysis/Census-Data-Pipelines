import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc
import time

# Write a function that data for each url does all the conversions and returns a dataframe
def API_Data_Collector(URL):
    request = requests.get(URL)
    data =  request.json()
    df1 = pd.DataFrame(data[1:], columns = data[0], )
    return df1

def Table_Concatenator(tables):
    combined = pd.concat(tables, ignore_index=True)
    return combined

def Community_cleaner(dataframe):
    # Clean up the community names
    patterns = [", Franklin County, Massachusetts",
                ", Hampden County, Massachusetts",
                ", Hampshire County, Massachusetts",
                ", Massachusetts",
                " town",
                " city",
                " Town"]
    # Create a single regex pattern that matches any of the unwanted substrings
    combined_pattern = "|".join(map(re.escape, patterns))
    # Apply the regex substitution to the entire column
    dataframe["NAME"] = dataframe["NAME"].apply(lambda x: re.sub(combined_pattern, "", x))
    # To strip any leading/trailing whitespace
    dataframe["NAME"] = dataframe["NAME"].str.strip()
    dataframe = dataframe.sort_values(by="NAME")
    return dataframe

def String_to_Numeric(dataframe):
    # Change all but the descriptive columns to numeric for calculations
    dataframe = pd.concat([pd.DataFrame([pd.to_numeric(dataframe[e], errors = 'coerce')
                            for e in dataframe.columns if e not in
                            ['GEO_ID','NAME']]).T,
                            dataframe[['GEO_ID','NAME']]], axis = 1)
    return dataframe

def Table_Math(dataframe):
    pass
    print("No math necessary for this table")
    return dataframe

def Database_Dataframe_Initializer(dataframe,year,year2):
    # Initialize the Database Dataframe, use NANs for missing values then fill the columns in from B19001
    ordered_communities = dataframe["NAME"].to_list()
    Database_df_Headers = ['POV_AGE_U5_TOT','POV_AGE_5_17_TOT','POV_AGE_18_34_TOT','POV_AGE_35_64_TOT','POV_AGE_O65_TOT',
                           'POV_AGE_U5','POV_AGE_5_17','POV_AGE_18_34','POV_AGE_35_64','POV_AGE_O65','POV_AGE_U5_PERC',
                           'POV_AGE_5_17_PERC','POV_AGE_18_34_PERC','POV_AGE_35_64_PERC','POV_AGE_O65_PERC',
                           'POV_SEX_FEMALE_TOT','POV_SEX_MALE_TOT','POV_SEX_BELOWPOV_MALE','POV_SEX_BELOWPOV_FEMALE',
                           'POV_SEX_BELOWPOV_MALE_PERC','POV_SEX_BELOWPOV_FEMALE_PERC','POV_RACE_WHITE_TOT',
                           'POV_RACE_BLACK_TOT','POV_RACE_NATAMER_TOT','POV_RACE_ASIAN_TOT','POV_RACE_PACISLAND_TOT',
                           'POV_RACE_OTHER_TOT','POV_RACE_HISPANIC_TOT','POV_RACE_WHITE_NOTHISPANIC_TOT',
                           'POV_RACE_WHITE','POV_RACE_BLACK','POV_RACE_NATAMER','POV_RACE_ASIAN','POV_RACE_PACISLAND',
                           'POV_RACE_OTHER','POV_RACE_HISPANIC','POV_RACE_WHITE_NOTHISPANIC','POV_RACE_WHITE_PERC',
                           'POV_RACE_BLACK_PERC','POV_RACE_NATAMER_PERC','POV_RACE_ASIAN_PERC','POV_RACE_PACISLAND_PERC',
                           'POV_RACE_OTHER_PERC','POV_RACE_HISPANIC_PERC','POV_RACE_WHITE_NOTHISPANIC_PERC',
]
    community_series = pd.Series(ordered_communities)
    Database_df = pd.DataFrame(np.nan, index=range(len(ordered_communities)), columns=Database_df_Headers)
    Database_df.insert(0, "YEAR", year)
    Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
    Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
    Database_df.insert(0, "COMMUNITY", dataframe["NAME"])
    Database_df.insert(0, "STATE", "MA")
    return Database_df

def Dataframe_Allocator(Database_):
    pass
def main():
    start_time = time.time()

    # Change the year to get a different vintage
    year = 2023
    year2 = year - 4
    URL = ["https://api.census.gov/data/"+str(year)+"/acs/acs5/subject?get=group(S1701)&ucgid=pseudo(0500000US25013$0600000)",
           "https://api.census.gov/data/"+str(year)+"/acs/acs5/subject?get=group(S1701)&ucgid=pseudo(0500000US25015$0600000)",
           "https://api.census.gov/data/"+str(year)+"/acs/acs5/subject?get=group(S1701)&ucgid=0400000US25,0500000US25011,0500000US25013,0500000US25015",
           "https://api.census.gov/data/"+str(year)+"/acs/acs5/subject?get=group(S1701)&ucgid=pseudo(0500000US25011$0600000)"]
    tables = []
    # Loop through URLs and use them to build dataframes, then stick them into a list
    for link in URL:
        output_table = API_Data_Collector(link)
        tables.append(output_table)

    # Run function to concatenate tables
    Poverty_Dataframe = Table_Concatenator(tables)

    # Run a function to clean up the town names
    Poverty_Dataframe = Community_cleaner(Poverty_Dataframe)

    # Run a function to convert string data into numerical data
    Poverty_Dataframe = String_to_Numeric(Poverty_Dataframe)

    # Run a function to create new columns and do math
    Poverty_Dataframe = Table_Math(Poverty_Dataframe)

    # Run a function to create the Database_Dataframe filled with NANs, our town names, and headers
    Database_df = Database_Dataframe_Initializer(Poverty_Dataframe,year,year2)
    print(Database_df.to_string())
    print(Poverty_Dataframe.to_string())
    # Run a function that will insert data from the original dataframe into the database dataframe




    end_time = time.time()
    print(f"Elapsed Runtime: {round(end_time - start_time, 4)} seconds")























main()
