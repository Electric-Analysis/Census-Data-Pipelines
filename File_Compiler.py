import pandas as pd
import os

# File path containing the yearly data files
# Change the file path as needed to compile the data files you want
# Change the file name as needed as well to change the output name of the file
file_path = r"C:\Users\jtilsch\OneDrive - Pioneer Valley Planning Commission\Desktop\Projects\Database Design\Data\Census_Poverty_Demographics"
file_name = "Compiled_Census_Poverty_Demographics"
# Get a list of all files in the directory (assuming they're CSV files)
files = [os.path.join(file_path, f) for f in os.listdir(file_path) if f.endswith('.csv')]

# Initialize a list to store DataFrames
dataframes = []

# Loop through each file
for idx, file in enumerate(files):
    if idx == 0:
        # For the first file, read it with the header
        df = pd.read_csv(file)
    else:
        # For subsequent files, skip the header row
        df = pd.read_csv(file, header=0)
    dataframes.append(df)

# Concatenate all DataFrames into one
final_dataframe = pd.concat(dataframes, ignore_index=True)

# Save the concatenated DataFrame to a new file
output_path = os.path.join(file_path, file_name+".csv")
final_dataframe.to_csv(output_path, index=False)

print(f"All files compiled successfully into \n{output_path}")
