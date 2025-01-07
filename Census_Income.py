import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc

# This pipeline is in goodshape but not done yet, County level data is collected but not put into the database dataframe
# Regional level data is not yet collected

# Headers for appending to the left of the imported table B19001
B19001_Headers = ['Less than $10,000', 'Percent Less than $10,000', '$10,000 to $14,999', 'Percent $10,000 to $14,999',
                  '$15,000 to $24,999', 'Percent $15,000 to $24,999', '$25,000 to $34,999', 'Percent $25,000 to $34,999',
                  '$35,000 to $49,999', 'Percent $35,000 to $49,999', '$50,000 to $74,999', 'Percent $50,000 to $74,999',
                  '$75,000 to $99,999', 'Percent $75,000 to $99,999', '$100,000 to $149,999',
                  'Percent $100,000 to $149,999', '$150,000 to $199,999', 'Percent $150,000 to $199,999',
                  '$200,000 or more', 'Percent $200,000 or more',]

# Headers for the database dataframe
Database_df_Headers = ['Less than $10,000', 'Percent Less than $10,000', '$10,000 to $14,999',
                       'Percent $10,000 to $14,999', '$15,000 to $24,999', 'Percent $15,000 to $24,999',
                       '$25,000 to $34,999', 'Percent $25,000 to $34,999', '$35,000 to $49,999',
                       'Percent $35,000 to $49,999', '$50,000 to $74,999', 'Percent $50,000 to $74,999',
                       '$75,000 to $99,999', 'Percent $75,000 to $99,999', '$100,000 to $149,999',
                       'Percent $100,000 to $149,999', '$150,000 to $199,999', 'Percent $150,000 to $199,999',
                       '$200,000 or more', 'Percent $200,000 or more']

# Change the year to get a different vintage
year = 2023
year2  = year - 4
B19001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B19001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"
B19001_URL_Counties = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B19001)&for=county:011,013,015&in=state:25"

# Make API call
B19001_request = requests.get(B19001_URL)
B19001_URL_Counties_request = requests.get(B19001_URL_Counties)

# Convert the called data into json format
B19001_data = B19001_request.json()
B19001_Counties_data = B19001_URL_Counties_request.json()

#Convert the JSON data into a dataframe
B19001_income = pd.DataFrame(B19001_data[1:], columns = B19001_data[0], )
B19001_income_Counties = pd.DataFrame(B19001_Counties_data[1:], columns = B19001_Counties_data[0], )


# Convert all but  the listed columns into a numeric data type so calculations can be performed
B19001_income = pd.concat([pd.DataFrame([pd.to_numeric(B19001_income[e], errors = 'coerce')
                        for e in B19001_income.columns if e not in
                        ['GEO_ID','NAME','state','county','county subdivision']]).T,
                        B19001_income[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

B19001_income_Counties = pd.concat([pd.DataFrame([pd.to_numeric(B19001_income_Counties[e], errors = 'coerce')
                        for e in B19001_income_Counties.columns if e not in
                        ['GEO_ID','NAME','state','county']]).T,
                        B19001_income_Counties[['GEO_ID','NAME','state','county']]], axis = 1)



# Get the names of the communities from the dataframe from the API
ordered_communities = []
for i in B19001_income["NAME"]:
    ordered_communities.append(i)
print(f"ordered_communities:\n{ordered_communities}")

# Fill data frame which is currently empty with NANs so we know the difference between zeroes and missing data
for i in B19001_Headers:
    B19001_income.insert(0, column= i, value= "NAN")

# Again but for counties
ordered_counties = []
for i in B19001_income_Counties["NAME"]:
    ordered_counties.append(i)
print(f"ordered_counties:\n{ordered_counties}")

for i in B19001_Headers:
    B19001_income_Counties.insert(0, column= i, value= "NAN")



# Calculate each columns values for table B19001 Sum
B19001_income['Less than $10,000']    = B19001_income.loc[:,  ["B19001_002E"]].sum(axis = 1)
B19001_income['$10,000 to $14,999']   = B19001_income.loc[:,  ["B19001_003E"]].sum(axis = 1)
B19001_income['$15,000 to $24,999']   = B19001_income.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1)
B19001_income['$25,000 to $34,999']   = B19001_income.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1)
B19001_income['$35,000 to $49,999']   = B19001_income.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1)
B19001_income['$50,000 to $74,999']   = B19001_income.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1)
B19001_income['$75,000 to $99,999']   = B19001_income.loc[:,  ["B19001_013E"]].sum(axis = 1)
B19001_income['$100,000 to $149,999'] = B19001_income.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1)
B19001_income['$150,000 to $199,999'] = B19001_income.loc[:,  ["B19001_016E"]].sum(axis = 1)
B19001_income['$200,000 or more']     = B19001_income.loc[:,  ["B19001_017E"]].sum(axis = 1)

# Perform each table calculation for percentages Sum / Total
B19001_income['Percent Less than $10,000'] = round(B19001_income.loc[:,  ["B19001_002E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $10,000 to $14,999'] = round(B19001_income.loc[:,  ["B19001_003E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $15,000 to $24,999'] = round(B19001_income.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $25,000 to $34,999'] = round(B19001_income.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $35,000 to $49,999'] = round(B19001_income.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $50,000 to $74,999'] = round(B19001_income.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $75,000 to $99,999'] = round(B19001_income.loc[:,  ["B19001_013E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $100,000 to $149,999'] = round(B19001_income.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $150,000 to $199,999'] = round(B19001_income.loc[:,  ["B19001_016E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)
B19001_income['Percent $200,000 or more'] = round(B19001_income.loc[:,  ["B19001_017E"]].sum(axis = 1) / B19001_income["B19001_001E"],4)

# Calculate each columns values for table B19001 Sum for the Counties Table
B19001_income_Counties['Less than $10,000'] = B19001_income_Counties.loc[:,  ["B19001_002E"]].sum(axis = 1)
B19001_income_Counties['$10,000 to $14,999'] = B19001_income_Counties.loc[:,  ["B19001_003E"]].sum(axis = 1)
B19001_income_Counties['$15,000 to $24,999'] = B19001_income_Counties.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1)
B19001_income_Counties['$25,000 to $34,999'] = B19001_income_Counties.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1)
B19001_income_Counties['$35,000 to $49,999'] = B19001_income_Counties.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1)
B19001_income_Counties['$50,000 to $74,999'] = B19001_income_Counties.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1)
B19001_income_Counties['$75,000 to $99,999'] = B19001_income_Counties.loc[:,  ["B19001_013E"]].sum(axis = 1)
B19001_income_Counties['$100,000 to $149,999'] = B19001_income_Counties.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1)
B19001_income_Counties['$150,000 to $199,999'] = B19001_income_Counties.loc[:,  ["B19001_016E"]].sum(axis = 1)
B19001_income_Counties['$200,000 or more'] = B19001_income_Counties.loc[:,  ["B19001_017E"]].sum(axis = 1)

# Perform each table calculation for percentages Sum / Total for the Counties Table
B19001_income_Counties['Percent Less than $10,000'] = round(B19001_income_Counties.loc[:,  ["B19001_002E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $10,000 to $14,999'] = round(B19001_income_Counties.loc[:,  ["B19001_003E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $15,000 to $24,999'] = round(B19001_income_Counties.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $25,000 to $34,999'] = round(B19001_income_Counties.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $35,000 to $49,999'] = round(B19001_income_Counties.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $50,000 to $74,999'] = round(B19001_income_Counties.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $75,000 to $99,999'] = round(B19001_income_Counties.loc[:,  ["B19001_013E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $100,000 to $149,999'] = round(B19001_income_Counties.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $150,000 to $199,999'] = round(B19001_income_Counties.loc[:,  ["B19001_016E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)
B19001_income_Counties['Percent $200,000 or more'] = round(B19001_income_Counties.loc[:,  ["B19001_017E"]].sum(axis = 1) / B19001_income_Counties["B19001_001E"],4)

# Show the table we pulled with  all the added on columns for  the database dataframe
print("\nB19001 Table")
print(B19001_income.to_string())

# Initialize the Database Dataframe, use NANs for missing values then fill the columns in from B19001
community_series = pd.Series(ordered_communities)
Database_df = pd.DataFrame(np.nan, index = range(len(ordered_communities)), columns = Database_df_Headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
Database_df.insert(0, "COMMUNITY", B19001_income["NAME"])
Database_df.insert(0, "STATE", "MA")

# Fill in data from B19001
Database_df['Less than $10,000']  = B19001_income['Less than $10,000']
Database_df['$10,000 to $14,999'] = B19001_income['$10,000 to $14,999']
Database_df['$15,000 to $24,999'] = B19001_income['$15,000 to $24,999']
Database_df['$25,000 to $34,999'] = B19001_income['$25,000 to $34,999']
Database_df['$35,000 to $49,999'] = B19001_income['$35,000 to $49,999']
Database_df['$50,000 to $74,999'] = B19001_income['$50,000 to $74,999']
Database_df['$75,000 to $99,999'] = B19001_income['$75,000 to $99,999']
Database_df['$100,000 to $149,999'] = B19001_income['$100,000 to $149,999']
Database_df['$150,000 to $199,999'] = B19001_income['$150,000 to $199,999']
Database_df['$200,000 or more'] = B19001_income['$200,000 or more']

# Calculate the percent values using the above data
Database_df['Percent Less than $10,000'] = B19001_income['Percent Less than $10,000']
Database_df['Percent $10,000 to $14,999'] = B19001_income['Percent $10,000 to $14,999']
Database_df['Percent $15,000 to $24,999'] = B19001_income['Percent $15,000 to $24,999']
Database_df['Percent $25,000 to $34,999'] = B19001_income['Percent $25,000 to $34,999']
Database_df['Percent $35,000 to $49,999'] = B19001_income['Percent $35,000 to $49,999']
Database_df['Percent $50,000 to $74,999'] = B19001_income['Percent $50,000 to $74,999']
Database_df['Percent $75,000 to $99,999'] = B19001_income['Percent $75,000 to $99,999']
Database_df['Percent $100,000 to $149,999'] = B19001_income['Percent $100,000 to $149,999']
Database_df['Percent $150,000 to $199,999'] = B19001_income['Percent $150,000 to $199,999']
Database_df['Percent $200,000 or more'] = B19001_income['Percent $200,000 or more']

# Clean up the community names
patterns = [", Franklin County, Massachusetts",
            ", Hampden County, Massachusetts",
            ", Hampshire County, Massachusetts",
            " town",
            " city",
            " Town"]
# Create a single regex pattern that matches any of the unwanted substrings
combined_pattern = "|".join(map(re.escape, patterns))

# Apply the regex substitution to the entire column
Database_df["COMMUNITY"] = Database_df["COMMUNITY"].apply(lambda x: re.sub(combined_pattern, "", x))

# If you want to strip any leading/trailing whitespace
Database_df["COMMUNITY"] = Database_df["COMMUNITY"].str.strip()

Database_df = Database_df.sort_values(by="COMMUNITY")

print("\nDatabase Dataframe")
print(Database_df.to_string())

# Database_df = Database_df.append(B19001_income_Counties, ignore_index  = True)
for i in B19001_income_Counties:
    print(i)

print("\nCounties Data Table: B19001")
print(B19001_income_Counties.to_string())

# print(Database_df.loc[])


Database_df.to_csv("C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census Income Data/Census Income " + str(year) + ".csv")





