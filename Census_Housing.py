import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc
import time

# def Printer(file_path, file_name):
#     file_path = r"C:\Users\jtilsch\OneDrive - Pioneer Valley Planning Commission\Desktop\Projects\Database Design\Data\Census_Poverty_Employment_ Data"
#     file_name = "Compiled_Census_Poverty_Employment"
#     # Get a list of all files in the directory (assuming they're CSV files)
#     files = [os.path.join(file_path, f) for f in os.listdir(file_path) if f.endswith('.csv')]
#
#     # Initialize a list to store DataFrames
#     dataframes = []
#
#     # Loop through each file
#     for idx, file in enumerate(files):
#         if idx == 0:
#             # For the first file, read it with the header
#             df = pd.read_csv(file)
#         else:
#             # For subsequent files, skip the header row
#             df = pd.read_csv(file, header=0)
#         dataframes.append(df)
#
#     # Concatenate all DataFrames into one
#     final_dataframe = pd.concat(dataframes, ignore_index=True)
#
#     # Save the concatenated DataFrame to a new file
#     output_path = os.path.join(file_path, file_name + ".csv")
#     final_dataframe.to_csv(output_path, index=False)
#     print(f"All files compiled successfully into \n{output_path}")



# Write a function that data for each url does all the conversions and returns a dataframe
def API_Data_Collector(link):
    request = requests.get(link)
    print(link)
    data = request.json()
    df = pd.DataFrame(data[1:], columns = data[0], )
    return df

def Table_Concatenator(year, table):
    # Conditionally handle table dp04 due to its different layout in terms of Rest API configuration otherwise concatenate
    # As in the usual way as demonstrated in the else statement
    if table == "DP04":
        URL_Town_Franklin = "https://api.census.gov/data/" + str(year) + "/acs/acs5/profile?get=group(DP04)&ucgid=pseudo(0500000US25011$0600000)"
        URL_Town_Hampden = "https://api.census.gov/data/" + str(year) + "/acs/acs5/profile?get=group(DP04)&ucgid=pseudo(0500000US25013$0600000)"
        URL_Town_Hampshire = "https://api.census.gov/data/" + str(year) + "/acs/acs5/profile?get=group(DP04)&ucgid=pseudo(0500000US25015$0600000)"
        URL_State_County = "https://api.census.gov/data/" + str(year) + "/acs/acs5/profile?get=group(DP04)&ucgid=0400000US25,0500000US25011,0500000US25013,0500000US25015"
        links = [URL_Town_Franklin, URL_Town_Hampden, URL_Town_Hampshire, URL_State_County]
        compiled_tables = []

        for link in links:
            req = API_Data_Collector(link)
            compiled_tables.append(req)
        combined_output = pd.concat(compiled_tables, ignore_index=True)
        print(combined_output.to_string())
        return combined_output
    else:
        URL = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=county%20subdivision:*&in=state:25%20county:011,013,015"
        URL_Counties = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=county:011,013,015&in=state:25"
        URL_State = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=state:25"
        links = [URL,URL_Counties,URL_State]
        compiled_tables = []
        for link in links:
            req = API_Data_Collector(link)
            compiled_tables.append(req)
        combined_output = pd.concat(compiled_tables, ignore_index=True)
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
    # Change all but the descriptive columns to numeric for calculations
    dataframe = pd.concat([pd.DataFrame([pd.to_numeric(dataframe[e], errors = 'coerce')
                            for e in dataframe.columns if e not in
                            ['GEO_ID','NAME']]).T,
                            dataframe[['GEO_ID','NAME']]], axis = 1)

    # Replace large neg values with NAN
    dataframe.replace(-6666666.660, np.nan, inplace=True)
    dataframe.replace( -666666666.0, np.nan, inplace=True)
    dataframe.replace("inf", np.nan, inplace=True)

    return dataframe

def Table_Math(dataframe):
    pass
    print("No math necessary for this table")
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
    Database_df_Headers = ['CEN_HOUSINGUNITS','CEN_OCCUPHU','CEN_VACHU','CEN_OWNOCCHU','CEN_RENOCCHU','CEN_SEAVACHU',
                           'CEN_HUYEARBLT','PER_OWN_OCC','CEN_HUNOVHCL','CEN_MEDRENT','CEN_RENT_INC','CEN_MEDOWNVAL',
                           'CEN_MEDOWNCOSTS','CEN_OWNCOSTS_INC','OWN_COSTS30','RENT_COSTS30','ALL_COSTS30','HOUS_AFFORD',
                           'HAVE_DATA','RECENT_YEAR',
]
    # community_series = pd.Series(ordered_communities)

    Database_df = pd.DataFrame(np.nan, index=range(len(ordered_communities)), columns=Database_df_Headers)
    Database_df.insert(0, "YEAR", year)
    Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
    Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
    Database_df.insert(0, "COMMUNITY", ordered_communities)
    Database_df.insert(0, "STATE", "MA")
    return Database_df

def Dataframe_Allocator(Database_df, B25004, B25035, B25045, B25064, B25071, B25088, B25092,
                        B25097, B25106, DP04):
    # Allocate data to each column as indicated in the data dictionary
    Database_df['CEN_HOUSINGUNITS'] = DP04.loc[:, ['DP04_0001E']].sum(axis=1)
    Database_df['CEN_OCCUPHU']      = B25106.loc[:, ['B25106_001E']].sum(axis=1)
    Database_df['CEN_VACHU'] = B25004.loc[:, ['B25004_001E']].sum(axis=1)
    Database_df['CEN_OWNOCCHU'] = B25106.loc[:, ['B25106_002E']].sum(axis=1)
    Database_df['CEN_RENOCCHU'] = B25106.loc[:, ['B25106_024E']].sum(axis=1)
    Database_df['CEN_SEAVACHU'] = B25004.loc[:, ['B25004_006E']].sum(axis=1)
    Database_df['CEN_HUYEARBLT'] = B25035.loc[:, ['B25035_001E']].sum(axis=1)
    Database_df['PER_OWN_OCC']    = round(DP04.loc[:, ['DP04_0046PE']].sum(axis=1) / 100, 4)
    Database_df['CEN_HUNOVHCL'] = B25045.loc[:, ['B25045_003E','B25045_012E']].sum(axis=1)
    Database_df['CEN_MEDRENT'] = B25064.loc[:, ['B25064_001E']].sum(axis=1)
    Database_df['CEN_RENT_INC'] = round(B25071.loc[:, ['B25071_001E']].sum(axis=1) / 100, 4)
    Database_df['CEN_MEDOWNVAL'] = B25097.loc[:, ['B25097_001E']].sum(axis=1)
    Database_df['CEN_MEDOWNCOSTS'] = B25088.loc[:, ['B25088_002E']].sum(axis=1)
    Database_df['CEN_OWNCOSTS_INC'] = round(B25092.loc[:, ['B25092_002E']].sum(axis=1) / 100, 4)
    Database_df['OWN_COSTS30'] = round(B25106.loc[:,['B25106_006E', 'B25106_010E', 'B25106_014E', 'B25106_018E', 'B25106_022E']].sum(axis=1) / B25106.loc[:, ['B25106_002E']].sum(axis=1), 4)
    Database_df['RENT_COSTS30'] = round(B25106.loc[:,['B25106_028E', 'B25106_032E', 'B25106_036E', 'B25106_040E', 'B25106_044E']].sum(axis=1) / B25106.loc[:, ['B25106_024E']].sum(axis=1), 4)
    Database_df['ALL_COSTS30'] = round(B25106.loc[:,['B25106_006E', 'B25106_010E', 'B25106_014E', 'B25106_018E', 'B25106_022E','B25106_028E', 'B25106_032E', 'B25106_036E', 'B25106_040E', 'B25106_044E']].sum(axis=1) / B25106.loc[:, ['B25106_002E', 'B25106_024E']].sum(axis=1), 4)
    Database_df['HOUS_AFFORD'] = B25106.loc[:, ['B25106_001E']].sum(axis=1)
    # Show the final database dataframe to make sure it is in good shape
    # print(Database_df.to_string())
    return Database_df

def Main(year):
    start_time = time.time()
    year = year
    year2 = year - 4
    # This variable will contain a list of lists. The first list contains a sequence of levels of geography for each
    # Given table. B25004 for example has town, county, and state level data. The URLs for this table will be the first
    # Element in the list. The second element in the list of lists would be the next table wherein that list is contained
    # Those levels of geography. The intent is to loop through each table and feed it into a function that will instantiate
    # Each table like B25004 and B25008 into their own dataframes so we can operate on them to create columns and calcs
    # B25008 is not working skip it for now it only contains 1 column of data
    tables = ["B25004","B25035","B25045","B25064","B25071","B25088","B25092","B25097","B25106","DP04"]
    # Loop through URLs and use them to build dataframes, then stick them into a list
    dataframes = []
    for table in tables:
        # Gets the api request returns a dataframe with all levels of geography
        output_table = Table_Concatenator(year, table)
        dataframes.append(output_table)

    # Run a function that cleans up the community names in the pulled tables
    # cleaned_tables=[]
    # for table in dataframes:
    #     table = Community_Cleaner(table)
    #     table = String_to_Numeric(table)
    #     cleaned_tables.append(table)
    #     # print(table)
    cleaned_tables = [String_to_Numeric(Community_Cleaner(table)).reset_index(drop=True) for table in dataframes]


    # print(dataframes)

    # Assign dataframes to variables for manipulations
    B25004 = cleaned_tables[0]
    B25035 = cleaned_tables[1]
    B25045 = cleaned_tables[2]
    B25064 = cleaned_tables[3]
    B25071 = cleaned_tables[4]
    B25088 = cleaned_tables[5]
    B25092 = cleaned_tables[6]
    B25097 = cleaned_tables[7]
    B25106 = cleaned_tables[8]
    DP04   = cleaned_tables[9]
    # Run a function to create the database dataframe
    Database_Dataframe = Database_Dataframe_Initializer(B25004, year, year2)
    Database_Dataframe = Dataframe_Allocator(Database_Dataframe, B25004, B25035, B25045, B25064, B25071, B25088, B25092, B25097, B25106, DP04)
    print(Database_Dataframe.to_string())

    Database_Dataframe.to_csv(
        "C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Housing/Census Housing " + str(year) + ".csv",
        index = False)
    print(
        f"Dataframe printed to CSV in \nC:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Housing/Census Housing {str(year)}.csv")




    end_time = time.time()
    print(f"Elapsed Runtime: {round(end_time - start_time, 4)} seconds")


# Change the year to get a different vintage,
# If you want one year, comment out the loop and ranged variable years
# If you want a range of years comment out year and uncomment the years variable and the "for" loop
#   Keep in mind the range function is [year,year) or Inclusive in the first year, non-inclusive in the second year so
#   increment 1 more beyond what you want in the latter year

# year = 2022
#
years = range(2012,2024)
for year in years:
    Main(year)

# Printer("C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census_Housing/Census Housing")


print("Ladies and gentlemen, we have lift off!!")

Main(year)
