def Census_Age():
    import requests
    import pandas as pd
    import numpy as np
    import re


    # year = int(input("Enter the year of data you want to pull: "))

    # Change this year to pull a different year of data
    year = 2023
    year2 = year - 4
    print(f"{year2} - {year}")

    # Community cleaner cleans/normalizes the column containing geographic data
    def Community_cleaner(table):
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
        table["NAME"] = table["NAME"].apply(lambda x: re.sub(combined_pattern, "", x))

        # To strip any leading/trailing whitespace
        table["NAME"] = table["NAME"].str.strip()

        table = table.sort_values(by="NAME")
        return table

    # B01001 API Calls
    B01001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B01001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"
    B01001_COUNTY_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B01001)&for=county:011,013,015&in=state:25"
    B01001_STATE_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B01001)&for=state:25"

    # B09001 API Calls
    B09001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B09001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"
    B09001_COUNTY_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B09001)&for=county:011,013,015&in=state:25"
    B09001_STATE_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B09001)&for=state:25"



    ordered_communities = ['Agawam','Amherst','Ashfield','Belchertown','Bernardston','Blandford','Brimfield','Buckland',
                           'Charlemont','Chester','Chesterfield','Chicopee','Colrain','Conway','Cummington','Deerfield',
                           'East Longmeadow','Easthampton','Erving','Franklin County','Gill','Goshen','Granby','Granville',
                           'Greenfield','Hadley','Hampden','Hampden County','Hampshire County','Hatfield','Hawley','Heath',
                           'Holland','Holyoke','Huntington','Leverett','Leyden','Longmeadow','Ludlow','Massachusetts',
                           'Middlefield','Monroe','Monson','Montague','Montgomery','New Salem','Northampton','Northfield',
                           'Orange','Palmer','Pelham','Plainfield','Rowe','Russell','Shelburne','Shutesbury','South Hadley',
                           'Southampton','Southwick','Springfield','Sunderland','Tolland','Wales','Ware','Warwick',
                           'Wendell','West Springfield','Westfield','Westhampton','Whately','Wilbraham','Williamsburg',
                           'Worthington']


    Database_df_Headers = ['UNDER_3_YEARS','3_4_YEARS','5_YEARS','6_8_YEARS','9_11_YEARS','12_14_YEARS','15_17_YEARS',
                           '18_19_YEARS','20_24_YEARS','25_29_YEARS','30_34_YEARS','35_39_YEARS','40_44_YEARS','45_49_YEARS',
                           '50_54_YEARS','55_59_YEARS','60_64_YEARS','65_69_YEARS','70_74_YEARS','75_79_YEARS','80_84_YEARS',
                           '85+_YEARS','CEN_POP_O4','CEN_POP_U18','CEN_POP_O64','CEN_POP_O24']

    B01001_Headers = ["18_19_YEARS", "20_24_YEARS", "25_29_YEARS", "30_34_YEARS", "35_39_YEARS", "40_44_YEARS",
                      "45_49_YEARS", "50_54_YEARS", "55_59_YEARS", "60_64_YEARS", "65_69_YEARS", "70_74_YEARS",
                      "75_79_YEARS", "80_84_YEARS", "85+_YEARS"]

    B09001_Headers = ["UNDER_3_YEARS","3_4_YEARS","5_YEARS","6_8_YEARS","9_11_YEARS","12_14_YEARS", "15_17_YEARS"]


    # Request B01001 Tables for town county and state
    B01001_request = requests.get(B01001_URL)
    B01001_COUNTY_request = requests.get(B01001_COUNTY_URL)
    B01001_STATE_request = requests.get(B01001_STATE_URL)

    B01001_list = [B01001_request,B01001_COUNTY_request,B01001_STATE_request]

    # Request B09001 Tables for town county and state
    B09001_request = requests.get(B09001_URL)
    B09001_COUNTY_request = requests.get(B09001_COUNTY_URL)
    B09001_STATE_request = requests.get(B09001_STATE_URL)

    B09001_list = [B09001_request,B09001_COUNTY_request,B09001_STATE_request]


    B01001_data = B01001_request.json()
    B01001_COUNTY_data = B01001_COUNTY_request.json()
    B01001_STATE_data = B01001_STATE_request.json()
    B01001_Age = pd.DataFrame(B01001_data[1:], columns = B01001_data[0], )
    B01001_Age_COUNTY = pd.DataFrame(B01001_COUNTY_data[1:], columns = B01001_COUNTY_data[0], )
    B01001_Age_STATE = pd.DataFrame(B01001_STATE_data[1:], columns = B01001_STATE_data[0], )

    B01001_Age  = pd.concat([B01001_Age,B01001_Age_COUNTY,B01001_Age_STATE], ignore_index= True)
    B01001_Age = Community_cleaner(B01001_Age)

    B09001_data = B09001_request.json()
    B09001_COUNTY_data = B09001_COUNTY_request.json()
    B09001_STATE_data = B09001_STATE_request.json()
    B09001_Age = pd.DataFrame(B09001_data[1:], columns = B09001_data[0], )
    B09001_Age_COUNTY = pd.DataFrame(B09001_COUNTY_data[1:], columns = B09001_COUNTY_data[0], )
    B09001_Age_STATE = pd.DataFrame(B09001_STATE_data[1:], columns = B09001_STATE_data[0], )

    B09001_Age  = pd.concat([B09001_Age,B09001_Age_COUNTY,B09001_Age_STATE], ignore_index= True)
    B09001_Age  = Community_cleaner(B09001_Age)


    # Sort by county subdivision
    B01001_Age.sort_values('GEO_ID', ascending = 0)

    # Sort by county subdivision
    B09001_Age.sort_values('GEO_ID', ascending = 0)


    #Preprocessing, zip two columns from imported tables together and check that they both match and have membership in the communities list
    # print(B01001_Age["NAME"])

    # https://stackoverflow.com/questions/44602139/pandas-convert-all-column-from-string-to-number-except-two
    # The above link will show how the line below converts all but set columns to numeric for calculations
    # df2 = pd.concat([pd.DataFrame([pd.to_numeric(df[e],errors='coerce') for e in df.columns if e not in ['Name','Job']]).T, df[['Name','Job']]],axis=1)

    # Change all but the descriptive columns to numeric for calculations
    B01001_Age = pd.concat([pd.DataFrame([pd.to_numeric(B01001_Age[e], errors = 'coerce')
                            for e in B01001_Age.columns if e not in
                            ['GEO_ID','NAME','state','county','county subdivision']]).T,
                            B01001_Age[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

    B09001_Age = pd.concat([pd.DataFrame([pd.to_numeric(B09001_Age[e], errors = 'coerce')
                            for e in B09001_Age.columns if e not in
                            ['GEO_ID','NAME','state','county','county subdivision']]).T,
                            B09001_Age[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

    #Instantiate new columns in the table
    for i in B01001_Headers:
        B01001_Age.insert(0, column= i, value= "NAN")

    for i in B09001_Headers:
        B09001_Age.insert(0, column= i, value= "NAN")


    # Check to make sure towns are aligned and data doesn't get mismatched, make this more robust if you feel the need
    def match_checker():
        truth_table = []
        for i, j in zip(B01001_Age["NAME"], B09001_Age["NAME"]):
            if i == j:
                truth_table.append(0)
                print(f"B01001: {i}\t\t\t\tB09001: {j} T")
            else:
                print(False)
                print(f"B01001: {i}\t\t\t\tB09001{j}\t FALSE")
                truth_table.append(1)
        if sum(truth_table) == 0:
            return True
        else:
            return False

    match_checker = match_checker()


    # Calculate each column's values for table B01001
    B01001_Age["18_19_YEARS"] = B01001_Age.loc[:, ["B01001_007E","B01001_031E"]].sum(axis = 1)
    B01001_Age["20_24_YEARS"] = B01001_Age.loc[:, ["B01001_008E","B01001_009E",
                                                   "B01001_010E", "B01001_032E",
                                                   "B01001_033E", "B01001_034E"]].sum(axis = 1)
    B01001_Age["25_29_YEARS"] = B01001_Age.loc[:, ["B01001_011E","B01001_035E"]].sum(axis = 1)
    B01001_Age["30_34_YEARS"] = B01001_Age.loc[:, ["B01001_012E","B01001_036E"]].sum(axis = 1)
    B01001_Age["35_39_YEARS"] = B01001_Age.loc[:, ["B01001_013E","B01001_037E"]].sum(axis = 1)
    B01001_Age["40_44_YEARS"] = B01001_Age.loc[:, ["B01001_014E","B01001_038E"]].sum(axis = 1)
    B01001_Age["45_49_YEARS"] = B01001_Age.loc[:, ["B01001_015E","B01001_039E"]].sum(axis = 1)
    B01001_Age["50_54_YEARS"] = B01001_Age.loc[:, ["B01001_016E","B01001_040E"]].sum(axis = 1)
    B01001_Age["55_59_YEARS"] = B01001_Age.loc[:, ["B01001_017E","B01001_041E"]].sum(axis = 1)
    B01001_Age["60_64_YEARS"] = B01001_Age.loc[:, ["B01001_018E","B01001_019E",
                                                   "B01001_042E", "B01001_043E"]].sum(axis = 1)
    B01001_Age["65_69_YEARS"] = B01001_Age.loc[:, ["B01001_020E","B01001_021E",
                                                   "B01001_044E", "B01001_045E"]].sum(axis = 1)
    B01001_Age["70_74_YEARS"] = B01001_Age.loc[:, ["B01001_022E","B01001_046E"]].sum(axis = 1)
    B01001_Age["75_79_YEARS"] = B01001_Age.loc[:, ["B01001_023E","B01001_047E"]].sum(axis = 1)
    B01001_Age["80_84_YEARS"] = B01001_Age.loc[:, ["B01001_024E","B01001_048E"]].sum(axis = 1)
    B01001_Age["85+_YEARS"]   = B01001_Age.loc[:, ["B01001_025E","B01001_049E"]].sum(axis = 1)


    # Calculate each column's values for table B09001
    B09001_Age["UNDER_3_YEARS"] =  B09001_Age.loc[:, ["B09001_003E"]]#.sum(axis = 1)
    B09001_Age["3_4_YEARS"] =  B09001_Age.loc[:, ["B09001_004E"]].sum(axis = 1)
    B09001_Age["5_YEARS"] =  B09001_Age.loc[:, ["B09001_005E"]].sum(axis = 1)
    B09001_Age["6_8_YEARS"] =  B09001_Age.loc[:, ["B09001_006E"]].sum(axis = 1)
    B09001_Age["9_11_YEARS"] =  B09001_Age.loc[:, ["B09001_007E"]].sum(axis = 1)
    B09001_Age["12_14_YEARS"] =  B09001_Age.loc[:, ["B09001_008E"]].sum(axis = 1)
    B09001_Age["15_17_YEARS"] =  B09001_Age.loc[:, ["B09001_009E"]].sum(axis = 1)

    # Show the tables in full
    print("\nTable: B01001")
    print(B01001_Age.to_string())

    print("\nTable: B09001")
    print(B09001_Age.to_string())

    # Construct the base table that is formated for the database
    community_series = pd.Series(ordered_communities)
    Database_df = pd.DataFrame(np.nan, index = range(len(B01001_Age["NAME"])), columns = Database_df_Headers)
    Database_df.insert(0, "YEAR", year)
    Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
    Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
    Database_df.insert(0, "COMMUNITY", B01001_Age["NAME"])
    Database_df.insert(0, "STATE", "MA")



    #Insert data from B01001 Table into database dataframe
    Database_df["18_19_YEARS"] = B01001_Age["18_19_YEARS"]
    Database_df["20_24_YEARS"] = B01001_Age["20_24_YEARS"]
    Database_df["25_29_YEARS"] = B01001_Age["25_29_YEARS"]
    Database_df["30_34_YEARS"] = B01001_Age["30_34_YEARS"]
    Database_df["35_39_YEARS"] = B01001_Age["35_39_YEARS"]
    Database_df["40_44_YEARS"] = B01001_Age["40_44_YEARS"]
    Database_df["45_49_YEARS"] = B01001_Age["45_49_YEARS"]
    Database_df["50_54_YEARS"] = B01001_Age["50_54_YEARS"]
    Database_df["55_59_YEARS"] = B01001_Age["55_59_YEARS"]
    Database_df["60_64_YEARS"] = B01001_Age["60_64_YEARS"]
    Database_df["65_69_YEARS"] = B01001_Age["65_69_YEARS"]
    Database_df["70_74_YEARS"] = B01001_Age["70_74_YEARS"]
    Database_df["75_79_YEARS"] = B01001_Age["75_79_YEARS"]
    Database_df["80_84_YEARS"] = B01001_Age["80_84_YEARS"]
    Database_df["85+_YEARS"]   = B01001_Age["85+_YEARS"]

    #Insert data from B09001 Table into database dataframe
    Database_df["UNDER_3_YEARS"] = B09001_Age ["UNDER_3_YEARS"]
    Database_df["3_4_YEARS"] = B09001_Age["3_4_YEARS"]
    Database_df["5_YEARS"] = B09001_Age["5_YEARS"]
    Database_df["6_8_YEARS"] = B09001_Age["6_8_YEARS"]
    Database_df["9_11_YEARS"] = B09001_Age["9_11_YEARS"]
    Database_df["12_14_YEARS"] = B09001_Age["12_14_YEARS"]
    Database_df["15_17_YEARS"] = B09001_Age["15_17_YEARS"]

    #Calculate data from other columns in database dataframe
    Database_df["CEN_POP_O4"] = Database_df.loc[:, ["5_YEARS","6_8_YEARS", "9_11_YEARS", "12_14_YEARS", "15_17_YEARS",
                                                    "18_19_YEARS", "20_24_YEARS", "25_29_YEARS", "30_34_YEARS",
                                                    "35_39_YEARS", "40_44_YEARS", "45_49_YEARS", "50_54_YEARS",
                                                    "55_59_YEARS", "60_64_YEARS", "65_69_YEARS", "70_74_YEARS",
                                                    "75_79_YEARS", "80_84_YEARS", "85+_YEARS"]].sum(axis = 1)

    Database_df["CEN_POP_U18"] = Database_df.loc[:, ["UNDER_3_YEARS","3_4_YEARS","5_YEARS","6_8_YEARS", "9_11_YEARS",
                                                     "12_14_YEARS", "15_17_YEARS"]].sum(axis = 1)

    Database_df["CEN_POP_O64"] = Database_df.loc[:, ["65_69_YEARS", "70_74_YEARS", "75_79_YEARS", "80_84_YEARS",
                                                     "85+_YEARS"]].sum(axis = 1)

    Database_df["CEN_POP_O24"] = Database_df.loc[:, ["25_29_YEARS", "30_34_YEARS","35_39_YEARS", "40_44_YEARS",
                                                     "45_49_YEARS", "50_54_YEARS","55_59_YEARS", "60_64_YEARS",
                                                     "65_69_YEARS", "70_74_YEARS","75_79_YEARS", "80_84_YEARS",
                                                     "85+_YEARS"]].sum(axis = 1)
    Database_df = Database_df.sort_values(by="COMMUNITY", ignore_index=True)
    print("\nDatabase_df")
    print(Database_df.to_string())

    print(f"Data is consistent and accurate: {match_checker}")

    Database_df.to_csv("C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census Age/Census Age "+ str(year) + ".csv")
    print(f"Dataframe printed to CSV in \nC:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census Age/{str(year)}.csv")

if __name__ == "__main__":
    Census_Age()