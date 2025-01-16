import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc
import time


def API_Data_Collector(link):
    request = requests.get(link)
    print(link)
    # print(request)
    data = request.json()
    # print(data)
    df = pd.DataFrame(data[1:], columns = data[0], )
    return df
def Table_Concatenator(year, table, api_key):
    # Conditionally handle table dp04 due to its different layout in terms of Rest API configuration otherwise concatenate
    # As in the usual way as demonstrated in the else statement
    if table == "S1401":
        URL_Town_Franklin = "https://api.census.gov/data/" + str(year) + "/acs/acs5/subject?get=group(S1401)&ucgid=pseudo(0500000US25011$0600000)" + api_key
        URL_Town_Hampshire = "https://api.census.gov/data/" + str(year) + "/acs/acs5/subject?get=group(S1401)&ucgid=pseudo(0500000US25015$0600000)" + api_key
        URL_Town_Hampden = "https://api.census.gov/data/" + str(year) + "/acs/acs5/subject?get=group(S1401)&ucgid=pseudo(0500000US25013$0600000)" + api_key
        URL_State_County = "https://api.census.gov/data/" + str(year) + "/acs/acs5/subject?get=group(S1401)&ucgid=0400000US25,0500000US25011,0500000US25013,0500000US25015" + api_key
        links = [URL_Town_Franklin, URL_Town_Hampden, URL_Town_Hampshire, URL_State_County]

        compiled_tables = []
        for link in links:
            req = API_Data_Collector(link)
            compiled_tables.append(req)
        combined_output = pd.concat(compiled_tables, ignore_index=True)
        # print(combined_output.to_string())
        return combined_output
    else:
        URL = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=county%20subdivision:*&in=state:25%20county:011,013,015" + api_key
        URL_Counties = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=county:011,013,015&in=state:25" + api_key
        URL_State = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=state:25" + api_key
        links = [URL,URL_Counties,URL_State]

        compiled_tables = []
        for link in links:
            req = API_Data_Collector(link)
            compiled_tables.append(req)
        combined_output = pd.concat(compiled_tables, ignore_index=True)
        # print(combined_output.to_string())
        return combined_output
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
    # Change all but the descriptive columns to numeric for calculations and handle numeric anomolies
    dataframe = pd.concat([pd.DataFrame([pd.to_numeric(dataframe[e], errors = 'coerce')
                            for e in dataframe.columns if e not in
                            ['GEO_ID','NAME']]).T,
                            dataframe[['GEO_ID','NAME']]], axis = 1)

    # Replace large neg values with NAN
    dataframe.replace(-6666666.660, np.nan, inplace=True)
    dataframe.replace( -666666666.0, np.nan, inplace=True)
    dataframe.replace("inf", np.nan, inplace=True)

    return dataframe
def Database_Dataframe_Initializer(dataframe, year,year2):
    # Initialize the Database Dataframe, use NANs for missing values then fill the columns in from B19001
    ordered_communities = dataframe["NAME"].to_list()
    # ordered_communities = ['Agawam','Amherst','Ashfield','Belchertown','Bernardston','Blandford','Brimfield','Buckland',
    #                        'Charlemont','Chester','Chesterfield','Chicopee','Colrain','Conway','Cummington','Deerfield',
    #                        'East Longmeadow','Easthampton','Erving','Franklin County','Gill','Goshen','Granby','Granville',
    #                        'Greenfield','Hadley','Hampden','Hampden County','Hampshire County','Hatfield','Hawley','Heath',
    #                        'Holland','Holyoke','Huntington','Leverett','Leyden','Longmeadow','Ludlow','Massachusetts',
    #                        'Middlefield','Monroe','Monson','Montague','Montgomery','New Salem','Northampton','Northfield',
    #                        'Orange','Palmer','Pelham','Plainfield','Rowe','Russell','Shelburne','Shutesbury','South Hadley',
    #                        'Southampton','Southwick','Springfield','Sunderland','Tolland','Wales','Ware','Warwick',
    #                        'Wendell','West Springfield','Westfield','Westhampton','Whately','Wilbraham','Williamsburg',
    #                        'Worthington']
    Database_df_Headers = ['EDUCATT_HS','EDUCATT_COLLEGE','CEN_EARLYED','RECENT_YEAR','HAVE_DATA',]
    # community_series = pd.Series(ordered_communities)
    Database_df = pd.DataFrame(np.nan, index=range(len(ordered_communities)), columns=Database_df_Headers)
    Database_df.insert(0, "YEAR", year)
    Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
    Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
    Database_df.insert(0, "COMMUNITY", ordered_communities)
    Database_df.insert(0, "STATE", "MA")
    # print(Database_df.to_string())
    return Database_df

def Dataframe_Allocator(Database_df, B15003, S1401):
    # Allocate data to each column as indicated in the data dictionary
    # Database_df['CEN_HOUSINGUNITS'] = DP04.loc[:, ['DP04_0001E']].sum(axis=1)
    Database_df['EDUCATT_COLLEGE'] = round(B15003.loc[:, ['B15003_022E','B15003_023E','B15003_024E','B15003_025E']].sum(axis=1) / B15003.loc[:, ['B15003_001E']].sum(axis=1), 4)
    Database_df['EDUCATT_HS'] = round(B15003.loc[:, ['B15003_017E','B15003_018E','B15003_019E','B15003_020E','B15003_021E','B15003_022E','B15003_023E','B15003_024E','B15003_025E']].sum(axis=1) / B15003.loc[:, ['B15003_001E']].sum(axis=1), 4)
    Database_df['CEN_EARLYED'] = round(S1401.loc[:, ['S1401_C02_014E']].sum(axis=1) / 100, 4)
    # Show the final database dataframe to make sure it is in good shape
    # print(Database_df.to_string())
    return Database_df

def Main(year):
    csv_file_path = "C:/Users/jtilsch/PycharmProjects/PVPC Data Pipelines/census api key.csv"
    api_key = pd.read_csv(csv_file_path, header=None).iloc[0, 0]

    start_time = time.time()
    year = year
    year2 = year - 4
    # This variable will contain a list of lists. The first list contains a sequence of levels of geography for each
    # Given table. B25004 for example has town, county, and state level data. The URLs for this table will be the first
    # Element in the list. The second element in the list of lists would be the next table wherein that list is contained
    # Those levels of geography. The intent is to loop through each table and feed it into a function that will instantiate
    # Each table like B25004 and B25008 into their own dataframes so we can operate on them to create columns and calcs
    # B25008 is not working skip it for now it only contains 1 column of data
    tables = ["B15003","S1401"]

    # Loop through URLs and use them to build dataframes, then stick them into a list
    dataframes = []
    for table in tables:
        # Gets the api request returns a dataframe with all levels of geography
        # print(table)
        output_table = Table_Concatenator(year, table, api_key)
        dataframes.append(output_table)

    cleaned_tables = [String_to_Numeric(Community_Cleaner(table)).reset_index(drop=True) for table in dataframes]


    # print(dataframes)

    # Assign dataframes to variables for manipulations
    B15003 = cleaned_tables[0]
    S1401 = cleaned_tables[1]

    # Run a function to create the database dataframe
    Database_Dataframe = Database_Dataframe_Initializer(B15003, year, year2)

    # Run a function to allocate data from imported tables to database structure
    Database_Dataframe = Dataframe_Allocator(Database_Dataframe, B15003, S1401)
    print(Database_Dataframe.to_string())
    # print(S1401.to_string())

    # Run a function to create regional values as aggregates of lower levels of geography
    # Database_Dataframe = Region_Calculations(Database_Dataframe,year, year2)


    Database_Dataframe.to_csv(
        "C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Education/Census_Education " + str(year) + ".csv",
        index = False)
    print(
        f"Dataframe printed to CSV in \nC:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Education {str(year)}.csv")
    end_time = time.time()
    print(f"Elapsed Runtime: {round(end_time - start_time, 4)} seconds")


# year = 2021
# Main(year)
# #
years = range(2012,2024)
for year in years:
    Main(year)
