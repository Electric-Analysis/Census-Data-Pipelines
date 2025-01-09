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

def Community_Cleaner(dataframe):
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
    Database_df_Headers = ['EMP_MALE','EMP_FEMALE','UNEMP_MALE','UNEMP_FEMALE','EMP_TOT_POV','UNEMP_TOT_POV',
                           'EMP_MALE_POV','EMP_FEMALE_POV','UNEMP_MALE_POV','UNEMP_FEMALE_POV','EMP_TOT_POV_PERC',
                           'UNEMP_TOT_POV_PERC','EMP_MALE_POV_PERC','EMP_FEMALE_POV_PERC','UNEMP_MALE_POV_PERC',
                           'UNEMP_FEMALE_POV_PERC']
    community_series = pd.Series(ordered_communities)
    Database_df = pd.DataFrame(np.nan, index=range(len(ordered_communities)), columns=Database_df_Headers)
    Database_df.insert(0, "YEAR", year)
    Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
    Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
    Database_df.insert(0, "COMMUNITY", dataframe["NAME"])
    Database_df.insert(0, "STATE", "MA")
    return Database_df

def Dataframe_Allocator(Database_df, Poverty_Dataframe):
    # Allocate data to each column as indicated in the data dictionary
    Database_df['EMP_MALE'] = Poverty_Dataframe.loc[:, ["S1701_C01_029E"]].sum(axis=1)
    Database_df['EMP_FEMALE'] = Poverty_Dataframe.loc[:, ["S1701_C01_030E"]].sum(axis=1)
    Database_df['UNEMP_MALE'] = Poverty_Dataframe.loc[:, ["S1701_C01_032E"]].sum(axis=1)
    Database_df['UNEMP_FEMALE'] = Poverty_Dataframe.loc[:, ["S1701_C01_033E"]].sum(axis=1)
    Database_df['EMP_TOT_POV'] = Poverty_Dataframe.loc[:, ["S1701_C02_028E"]].sum(axis=1)
    Database_df['UNEMP_TOT_POV'] = Poverty_Dataframe.loc[:, ["S1701_C02_031E"]].sum(axis=1)

    Database_df['EMP_MALE_POV'] = Poverty_Dataframe.loc[:, ["S1701_C02_029E"]].sum(axis=1)
    Database_df['EMP_FEMALE_POV'] = Poverty_Dataframe.loc[:, ["S1701_C02_030E"]].sum(axis=1)
    Database_df['UNEMP_MALE_POV'] = Poverty_Dataframe.loc[:, ["S1701_C02_032E"]].sum(axis=1)
    Database_df['UNEMP_FEMALE_POV'] = Poverty_Dataframe.loc[:, ["S1701_C02_033E"]].sum(axis=1)
    Database_df['EMP_TOT_POV_PERC'] = round(Poverty_Dataframe.loc[:, ["S1701_C03_028E"]].sum(axis=1)/100, 4)

    Database_df['UNEMP_TOT_POV_PERC'] = round(Poverty_Dataframe.loc[:, ["S1701_C03_031E"]].sum(axis=1)/100, 4)
    Database_df['EMP_MALE_POV_PERC'] = round(Poverty_Dataframe.loc[:, ["S1701_C03_029E"]].sum(axis=1)/100, 4)
    Database_df['EMP_FEMALE_POV_PERC'] = round(Poverty_Dataframe.loc[:, ["S1701_C03_030E"]].sum(axis=1)/100, 4)
    Database_df['UNEMP_MALE_POV_PERC'] = round(Poverty_Dataframe.loc[:, ["S1701_C03_032E"]].sum(axis=1)/100, 4)
    Database_df['UNEMP_FEMALE_POV_PERC'] = round(Poverty_Dataframe.loc[:, ["S1701_C03_033E"]].sum(axis=1)/100, 4)

    # Replace large neg values with NAN
    Database_df.replace(-6666666.660, np.nan, inplace=True)

    return Database_df

def Main(year):
    start_time = time.time()
    year = year
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
    Poverty_Dataframe = Community_Cleaner(Poverty_Dataframe)

    # Run a function to convert string data into numerical data
    Poverty_Dataframe = String_to_Numeric(Poverty_Dataframe)

    # Run a function to create new columns and do math
    Poverty_Dataframe = Table_Math(Poverty_Dataframe)

    # Run a function to create the Database_Dataframe filled with NANs, our town names, and headers
    Database_df = Database_Dataframe_Initializer(Poverty_Dataframe,year,year2)

    # Run a function that will insert data from the original dataframe into the database dataframe, use data dictionary
    Database_df = Dataframe_Allocator(Database_df, Poverty_Dataframe)
    print(Poverty_Dataframe.to_string())
    print(Database_df.to_string())

    # Create a CSV file of the dataframe to be uploaded to the database
    Database_df.to_csv(
        "C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Poverty_Employment_ Data/Census_Poverty_Employment" + str(year) + ".csv",
        index = False)
    print(
        f"Dataframe printed to CSV in \nC:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Poverty_Employment_{str(year)}.csv")


    end_time = time.time()
    print(f"Elapsed Runtime: {round(end_time - start_time, 4)} seconds")


# Change the year to get a different vintage,
# If you want one year, comment out the loop and ranged variable years
# If you want a range of years comment out year and uncomment the years variable and the "for" loop
#   Keep in mind the range function is [year,year) or Inclusive in the first year, non-inclusive in the second year so
#   increment 1 more beyond what you want in the latter year

# year = 2023

years = range(2015,2024)
for year in years:
    Main(year)




Main(year)
