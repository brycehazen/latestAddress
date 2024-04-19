import os
import pandas as pd
import math
from multiprocessing import Process

def combine_csv_files():
    files = os.listdir(os.getcwd())

    # Filter the files to include only CSV files with a number followed by ".csv"
    csv_files = [
        file for file in files if file.endswith(".csv") and file[:-4].isdigit()
    ]

    combined_dataframes = []

    # Iterate over the CSV files and read them into DataFrames
    for file in csv_files:
        df = pd.read_csv(file)
        combined_dataframes.append(df)

    # Combine all the DataFrames into a single DataFrame
    combined_df = pd.concat(combined_dataframes, ignore_index=True)

    # Write the combined DataFrame to a new CSV file called "final.csv"
    combined_df.to_csv("final.csv", index=False)

    print("Combined CSV file 'final.csv' has been created.")


def process_portion_of_df(df, chunk_id):
    # Sort by ConsID and MailingChanged to ensure the most recent entries
    df.sort_values(by=['ConsID', 'MailingChanged'], ascending=[True, False], inplace=True)

    # Keep only the latest entry for each ConsID
    df.drop_duplicates(subset='ConsID', keep='first', inplace=True)

    # Save the chunk to a CSV file
    df.to_csv(f"{chunk_id}.csv", index=False)


def split_and_process_dataframe(df):
    num_cpus = os.cpu_count()
    chunk_size = int(math.ceil(len(df) / num_cpus))

    chunks = [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]

    processes = []
    for i, chunk in enumerate(chunks):
        process = Process(
            target=process_portion_of_df, args=(chunk, i)
        )
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    # Combine all the CSV files into a final file
    combine_csv_files()


# Example usage
if __name__ == "__main__":
    # Read the Excel file into a DataFrame
    df = pd.read_excel('All Families 03.29.24.xlsx')

    # Split and process the DataFrame
    split_and_process_dataframe(df)
