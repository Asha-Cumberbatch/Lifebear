import pandas as pd
import re
import os

# Paths
csv_file_path = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/Lifebear.csv'
before_cleaning_folder = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/Chunks/'
cleaned_chunks_folder = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/Cleaned_Chunks/'
garbage_folder = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/Garbage/'
merged_cleaned_file = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/merged_cleaned.csv'
merged_garbage_file = 'C:/Users/aaack/OneDrive/Desktop/ProtexxaAI/DataCleaning/merged_garbage.csv'

# Create output directories
for folder in [before_cleaning_folder, cleaned_chunks_folder, garbage_folder]:
    os.makedirs(folder, exist_ok=True)
    print(f"Directory created or exists: {folder}")

# Specify dtypes
dtypes = {
    'login_id': 'string',
    'mail_address': 'string',
    'password': 'string',
    'salt': 'string',
    'birthday_on': 'string',
    'gender': 'float64'
}

# Simplified email validation - just checks the general format
email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$'

# Read the CSV to get the total number of rows
total_rows = len(pd.read_csv(csv_file_path, sep=';', dtype=dtypes, low_memory=False))
print(f"Total rows: {total_rows}")

# Determine chunk size
num_chunks = 6
chunk_size_rows = total_rows // num_chunks
print(f"Rows per chunk: {chunk_size_rows}")

def is_valid_email(email):
    # More lenient email validation (accept emails with uncommon domains and capitalization)
    email = email.strip().lower()  # Normalize casing
    return bool(re.match(email_regex, email)) if pd.notnull(email) else False

def clean_data(df):
    # Drop rows with three consecutive commas (,,,,)
    invalid_pattern = df.apply(lambda row: row.astype(str).str.contains(',,,,').any(), axis=1)
    df = df[~invalid_pattern]

    # Select relevant columns and rename them
    columns_to_keep = ['login_id', 'mail_address', 'password', 'salt', 'birthday_on', 'gender']
    df = df[columns_to_keep].copy()
    df.rename(columns={
        'login_id': 'login',
        'mail_address': 'email',
        'password': 'password',
        'salt': 'salt',
        'birthday_on': 'date_of_birth',
        'gender': 'gender'
    }, inplace=True)

    # Capture rows with missing essential data (where all essential fields are missing)
    essential_columns = ['login', 'email', 'password']
    missing_data = df[df[essential_columns].isna().all(axis=1)]

    # Filter invalid emails based on the general format
    df['valid_email'] = df['email'].apply(is_valid_email)
    invalid_emails = df[~df['valid_email']]
    print(f"Invalid emails: {len(invalid_emails)}")
    
    # Drop rows where all essential data is missing (instead of any missing)
    df = df[(df[essential_columns].notnull().any(axis=1)) & df['valid_email']]

    # Drop the 'valid_email' helper column
    df.drop(columns=['valid_email'], inplace=True)

    # Drop duplicates, keeping the first occurrence
    df.drop_duplicates(subset=['email', 'login'], keep='first', inplace=True)

    # Combine all removed rows into a garbage DataFrame, drop duplicates
    garbage_rows = pd.concat([missing_data, invalid_emails]).drop_duplicates()

    return df, garbage_rows

# Initialize DataFrames to hold merged cleaned and garbage data
merged_cleaned_df = pd.DataFrame()
merged_garbage_df = pd.DataFrame()

# Loop through chunks and process
for chunk_index in range(num_chunks):
    start_row = chunk_index * chunk_size_rows
    df_chunk = pd.read_csv(csv_file_path, sep=';', skiprows=range(1, start_row + 1),
                           nrows=chunk_size_rows, dtype=dtypes, low_memory=False)

    # Handle the last chunk (remaining rows)
    if chunk_index == num_chunks - 1:
        df_chunk = pd.read_csv(csv_file_path, sep=';', skiprows=range(1, start_row + 1), dtype=dtypes, low_memory=False)

    # Save original chunk
    original_chunk_path = f'{before_cleaning_folder}original_chunk_{chunk_index}.csv'
    df_chunk.to_csv(original_chunk_path, index=False)
    print(f"Saved original chunk {chunk_index} to {original_chunk_path}")

    # Clean the chunk
    cleaned_chunk, chunk_garbage = clean_data(df_chunk)

    # Save cleaned chunk
    cleaned_count = len(cleaned_chunk)
    garbage_count = len(chunk_garbage)

    if cleaned_count > 0:
        cleaned_chunk_path = f'{cleaned_chunks_folder}cleaned_chunk_{chunk_index}.csv'
        cleaned_chunk.to_csv(cleaned_chunk_path, index=False)
        print(f"Saved cleaned chunk {chunk_index} to {cleaned_chunk_path} ({cleaned_count} rows)")

        # Add the cleaned chunk to the merged cleaned DataFrame
        merged_cleaned_df = pd.concat([merged_cleaned_df, cleaned_chunk])
    else:
        print(f"Chunk {chunk_index} is empty after cleaning.")

    # Save garbage data
    if garbage_count > 0:
        garbage_chunk_path = f'{garbage_folder}garbage_chunk_{chunk_index}.csv'
        chunk_garbage.to_csv(garbage_chunk_path, index=False)
        print(f"Saved garbage data for chunk {chunk_index} to {garbage_chunk_path} ({garbage_count} rows)")

        # Add the garbage chunk to the merged garbage DataFrame
        merged_garbage_df = pd.concat([merged_garbage_df, chunk_garbage])
    else:
        print(f"No garbage data for chunk {chunk_index}.")

    # Calculate the rate of cleaned to garbage rows
    if garbage_count > 0:
        clean_to_garbage_rate = cleaned_count / garbage_count
        print(f"Clean to Garbage Rate for chunk {chunk_index}: {clean_to_garbage_rate:.2f}")
    else:
        print(f"No garbage data for chunk {chunk_index}, rate not applicable.")

# Drop duplicates in the merged DataFrames
merged_cleaned_df.drop_duplicates(inplace=True)
merged_garbage_df.drop_duplicates(inplace=True)

# Save merged cleaned and garbage DataFrames to CSV
merged_cleaned_df.to_csv(merged_cleaned_file, index=False)
print(f"Saved merged cleaned data to {merged_cleaned_file} ({len(merged_cleaned_df)} rows)")

merged_garbage_df.to_csv(merged_garbage_file, index=False)
print(f"Saved merged garbage data to {merged_garbage_file} ({len(merged_garbage_df)} rows)")
