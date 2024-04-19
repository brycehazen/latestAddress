import os
import pandas as pd
import math
from multiprocessing import Process

def combine_csv_files():
    files = os.listdir(os.getcwd())
    csv_files = [f for f in files if 'processed_chunk_' in f]
    combined_dataframes = [pd.read_csv(f, low_memory=False) for f in csv_files]
    combined_df = pd.concat(combined_dataframes, ignore_index=True)
    combined_df.to_csv('final_combined.csv', index=False)

def process_portion_mailing(oca_df, final_df, chunk_id):
    # Merge the main OCA file with additional data from final.csv on 'ConsID'
    merged_df = pd.merge(oca_df, final_df[['ConsID', 'StreetChanged', 'PrefAddrLines', 'PrefAddrCity', 'PrefAddrState', 'PrefAddrZIP']], 
                         on='ConsID', how='left')
    
    # Convert date columns to datetime
    merged_df['StreetChanged'] = pd.to_datetime(merged_df['StreetChanged'], errors='coerce')
    merged_df['CnAdrPrf_DateLastChanged'] = pd.to_datetime(merged_df['CnAdrPrf_DateLastChanged'], errors='coerce')

    # Check which address is more recent and update the address fields if final.csv has a more recent address
    condition = pd.notna(merged_df['StreetChanged']) & (merged_df['StreetChanged'] > merged_df['CnAdrPrf_DateLastChanged'])
    for col in ['PrefAddrLines', 'PrefAddrCity', 'PrefAddrState', 'PrefAddrZIP']:
        merged_df[col] = merged_df.apply(
            lambda row: row[f"{col}"] if not condition[row.name] else row[f"{col}"],
            axis=1)

    # Save the processed chunk to a CSV file
    merged_df.to_csv(f'processed_chunk_{chunk_id}.csv', index=False)


def split_and_process_dataframe(oca_df, final_df):
    num_cpus = os.cpu_count()
    chunk_size = math.ceil(len(oca_df) / num_cpus)
    chunks = [oca_df.iloc[i:i + chunk_size] for i in range(0, len(oca_df), chunk_size)]
    processes = []
    for i, chunk in enumerate(chunks):
        p = Process(target=process_portion_mailing, args=(chunk, final_df, i))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    combine_csv_files()

if __name__ == "__main__":
    try:
        final_df = pd.read_csv('final.csv', encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        final_df = pd.read_csv('final.csv', encoding='ISO-8859-1', low_memory=False)

    try:
        oca_df = pd.read_csv('OCA_Last5Years_No2024.CSV', encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        oca_df = pd.read_csv('OCA_Last5Years_No2024.CSV', encoding='ISO-8859-1', low_memory=False)

    split_and_process_dataframe(oca_df, final_df)
