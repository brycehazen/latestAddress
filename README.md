# CSV File Processor

This Python script is designed to process multiple CSV files containing data related to families and their addresses. It combines the CSV files into a single DataFrame, sorts the data, and saves the processed data into new CSV files. Additionally, it utilizes multiprocessing to improve processing speed.

## Usage

1. Place Excel file 'file.xlsx' in the same directory as the script.
2. Run the script `recentAddress.py`.
3. The script will read  'file.xlsx' into a DataFrame, split the DataFrame into chunks, process each chunk, and combine the results of most recent addresses with no duplicate ConsID into 'final.csv'.
4. Run the script `final_recentAddress.py` with 'final.csv' and 'oca_mailing.csv' in the same file
5. 'final_combined.csv' will output more updated addresses from 'final.csv' appended to 'oca_mailing.csv'

## Functions

### `combine_csv_files()`

- Reads all CSV files in the current directory.
- Combines the CSV files into a single DataFrame.
- Saves the combined DataFrame to 'final.csv'.

### `process_portion_parish(df, chunk_id)`

- Processes a portion of the DataFrame within a chunk.
- Converts 'MailingChanged' column to datetime and 'ConsID' column to string.
- Sorts the DataFrame by 'ConsID' and 'MailingChanged' in descending order.
- Keeps only the latest entry for each 'ConsID'.
- Saves the sorted DataFrame to a CSV file.

### `process_portion_mailing(df, chunk_id)`
- Reads 'OCA_Last5Years_No2024.CSV' into a DataFrame, attempting UTF-8 encoding first and falling back to ISO-8859-1 encoding if needed.
- Merges 'final.csv' (loaded as `df`) with 'OCA_Last5Years_No2024.CSV' (loaded as `oca_df`) on 'ConsID'.
- Converts 'CnAdrPrf_DateLastChanged' and 'MailingChanged' columns to datetime.
- Compares 'CnAdrPrf_DateLastChanged' and 'MailingChanged', and appends the address if 'final.csv' has a more recent address.
- Saves the processed chunk DataFrame to a CSV file with `processed_chunk_{chunk_id}.csv` as the filename.


### `split_and_process_dataframe(df)`

- Splits the DataFrame into chunks.
- Starts a separate Process for each chunk to process the data.
- Combines the processed data into 'final.csv'.

## Dependencies

- pandas
- os
- math
- multiprocessing
