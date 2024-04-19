import os
import pandas as pd
import math
from multiprocessing import Process

# Function to combine CSV files into a single DataFrame and save as final.csv
def combine_csv_files():
    # Get a list of files in the current directory
    files = os.listdir(os.getcwd())
    # Filter the list to include only CSV files with names like '123.csv'
    csv_files = [f for f in files if f.endswith('.csv') and f[:-4].isdigit()]
    # Read each CSV file into a DataFrame and store them in a list
    combined_dataframes = [pd.read_csv(f) for f in csv_files]
    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(combined_dataframes, ignore_index=True)
    # Write the combined DataFrame to 'final.csv' without including the index
    combined_df.to_csv('final.csv', index=False)

# Function to process a portion of the DataFrame, sort it, and save it to a CSV file
def process_portion_of_df(df, chunk_id):
    # Convert 'MailingChanged' column to datetime, handling errors with 'coerce' argument
    df['MailingChanged'] = pd.to_datetime(df['MailingChanged'], errors='coerce')
    # Convert 'ConsID' column to string to ensure sorting works correctly
    df['ConsID'] = df['ConsID'].astype(str)
    # Sort the DataFrame by 'ConsID' and 'MailingChanged' in descending order
    sorted_df = df.sort_values(by=['ConsID', 'MailingChanged'], ascending=[True, False])
    # Keep only the latest entry for each 'ConsID'
    sorted_df.drop_duplicates(subset='ConsID', keep='first', inplace=True)
    # Save the sorted DataFrame to a CSV file with chunk_id as the filename
    sorted_df.to_csv(f'{chunk_id}.csv', index=False)

# Function to split the DataFrame into chunks, process each chunk, and combine the results
def split_and_process_dataframe(df):
    # Get the number of CPU cores
    num_cpus = os.cpu_count()
    # Calculate the chunk size based on the number of CPU cores
    chunk_size = math.ceil(len(df) / num_cpus)
    # Split the DataFrame into chunks
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    # Create a list to store Process objects
    processes = []
    # Iterate over chunks and start a separate Process for each chunk
    for i, chunk in enumerate(chunks):
        p = Process(target=process_portion_of_df, args=(chunk, i))
        processes.append(p)
        p.start()
    # Wait for all Processes to finish
    for p in processes:
        p.join()
    # Combine all CSV files into 'final.csv'
    combine_csv_files()

if __name__ == '__main__':
    # Read the Excel file into a DataFrame
    df = pd.read_excel('All Families 03.29.24.xlsx')
    # Split and process the DataFrame
    split_and_process_dataframe(df)
